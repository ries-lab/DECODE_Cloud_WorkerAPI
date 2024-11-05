import abc
import os
import random
import threading
import time

import boto3
import pytest
from fastapi import HTTPException

from workerfacing_api.core.queue import (
    JobStates,
    LocalJobQueue,
    RDSJobQueue,
    SQSJobQueue,
)
from workerfacing_api.schemas.queue_jobs import EnvironmentTypes, JobFilter


@pytest.fixture
def skip_aws(env):
    if env != "local":
        pytest.skip("Only tested on local DB")
    yield env


@pytest.fixture
def skip_local(env):
    if env == "local":
        pytest.skip("Only tested on AWS")
    yield env


class _TestJobQueue(abc.ABC):
    @pytest.fixture(scope="function")
    def job_queue(self):
        raise NotImplementedError

    @pytest.fixture
    def queue(self, job_queue):
        yield job_queue
        job_queue.delete()

    @pytest.fixture
    def job_filter(self):
        return JobFilter(environment=EnvironmentTypes.local)

    def test_create_queue(self, queue, job_filter):
        # test queue is empty
        assert queue.peek(hostname="i", filter=job_filter) is None

    def test_enqueue(self, populated_queue, job_filter):
        assert populated_queue.peek(hostname="i", filter=job_filter) is not None

    def test_peek(self, populated_queue, job_filter):
        assert populated_queue.peek(hostname="i", filter=job_filter)[1].meta.job_id == 0
        # peeking does not remove elements
        assert populated_queue.peek(hostname="i", filter=job_filter)[1].meta.job_id == 0

    def test_dequeue(self, populated_queue, job_filter):
        assert (
            populated_queue.dequeue(hostname="i", filter=job_filter)[1].meta.job_id == 0
        )
        # dequeue removes elements
        assert (
            populated_queue.dequeue(hostname="i", filter=job_filter)[1].meta.job_id == 1
        )
        # None env jobs can be pulled by everyone
        assert (
            populated_queue.dequeue(hostname="i", filter=job_filter)[1].meta.job_id == 2
        )
        # environment is filtered correctly
        assert populated_queue.dequeue(hostname="i", filter=job_filter) is None

    def test_dequeue_old(self, populated_queue, job_filter):
        filter = job_filter.model_copy()
        filter.older_than = 15
        # jobs in the same environment can immediately be pulled
        assert populated_queue.dequeue(hostname="i", filter=filter) is not None
        assert populated_queue.dequeue(hostname="i", filter=filter) is not None
        # jobs in the "any" environment must be old enough
        assert populated_queue.dequeue(hostname="i", filter=filter) is None
        time.sleep(15)
        assert populated_queue.dequeue(hostname="i", filter=filter) is not None

    def test_concurrent_pulls(self, populated_full_queue, job_filter):
        found_jobs = [None] * 100

        def _pull(i):
            time.sleep(random.random() * 5)  # ensure enough time to pull all jobs
            found_jobs[i] = populated_full_queue.dequeue(
                hostname="i", filter=job_filter
            )

        threads = []
        for i in range(100):
            thread = threading.Thread(target=_pull, args=(i,))
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        found_jobs = [job for job in found_jobs if job is not None]
        # no duplicates
        assert len(found_jobs) == len(set([job[1].meta.job_id for job in found_jobs]))


class TestLocalQueue(_TestJobQueue):
    @pytest.fixture(scope="function")
    def job_queue(self, skip_aws, tmpdir):
        queue_path = str(tmpdir / "queue.pkl")
        if os.path.exists(queue_path):
            os.remove(queue_path)
        job_queue = LocalJobQueue(queue_path)
        job_queue.create()
        return job_queue


class TestSQSQueue(_TestJobQueue):
    @pytest.fixture
    def job_queue(self, skip_local):
        job_queue = SQSJobQueue(boto3.client("sqs", "eu-central-1"))
        # wait for queue to be deleted before recreating
        while True:
            try:
                job_queue.create(err_on_exists=True)
                break
            except HTTPException:
                time.sleep(1)
        return job_queue


class TestRDSLocalQueue(_TestJobQueue):
    @pytest.fixture(scope="function")
    def job_queue(self, skip_aws, tmpdir):
        job_queue = RDSJobQueue(f"sqlite:///{tmpdir}/local.db")
        job_queue.create()
        return job_queue

    # additional tests for additional functionality
    def test_filtering(self, populated_full_queue):
        assert (
            populated_full_queue.peek(
                hostname="i",
                filter=JobFilter(
                    environment=EnvironmentTypes.local, cpu_cores=2, memory=1
                ),
            )[1].meta.job_id
            == 1
        )
        assert (
            populated_full_queue.peek(
                hostname="i",
                filter=JobFilter(environment=EnvironmentTypes.local, memory=1),
            )[1].meta.job_id
            == 2
        )
        assert (
            populated_full_queue.peek(
                hostname="i",
                filter=JobFilter(
                    environment=EnvironmentTypes.local,
                    gpu_model="gpu_model",
                    gpu_archi="gpu_archi",
                ),
            )[1].meta.job_id
            == 0
        )

    def test_priorities(self, populated_full_queue):
        common_filter = {
            "cpu_cores": 10,
            "memory": 10,
        }
        # group priority
        assert (
            populated_full_queue.dequeue(
                hostname="i",
                filter=JobFilter(
                    environment=EnvironmentTypes.local,
                    groups=["group", "another group"],
                    **common_filter,
                ),
            )[1].meta.job_id
            == 2
        )
        # job priority
        assert (
            populated_full_queue.dequeue(
                hostname="i",
                filter=JobFilter(environment=EnvironmentTypes.local, **common_filter),
            )[1].meta.job_id
            == 1
        )
        assert (
            populated_full_queue.dequeue(
                hostname="i",
                filter=JobFilter(environment=EnvironmentTypes.local, **common_filter),
            )[1].meta.job_id
            == 3
        )

    def test_dequeue_old(self, populated_queue):
        # older_than does not apply when the right environment is selected
        # not old enough
        assert (
            populated_queue.dequeue(
                hostname="i",
                filter=JobFilter(environment=EnvironmentTypes.local, older_than=5),
            )
            is not None
        )
        assert (
            populated_queue.dequeue(
                hostname="i",
                filter=JobFilter(environment=EnvironmentTypes.local, older_than=5),
            )
            is not None
        )
        assert (
            populated_queue.dequeue(
                hostname="i",
                filter=JobFilter(environment=EnvironmentTypes.local, older_than=5),
            )
            is None
        )
        # old enough
        time.sleep(5)
        assert (
            populated_queue.dequeue(
                hostname="i",
                filter=JobFilter(environment=EnvironmentTypes.local, older_than=5),
            )
            is not None
        )

    def test_failures(self, populated_full_queue):
        job_id = populated_full_queue.dequeue(
            hostname="first", filter=JobFilter(environment=EnvironmentTypes.local)
        )[0]
        res = populated_full_queue.dequeue(
            hostname="second", filter=JobFilter(environment=EnvironmentTypes.local)
        )
        assert not res or res[0] != job_id
        # fail -> requeue
        time.sleep(6)
        populated_full_queue.handle_timeouts(max_retries=1, timeout_failure=5)
        assert populated_full_queue.get_job(job_id).status == "queued"
        # same worker can not repull
        res = populated_full_queue.dequeue(
            hostname="first", filter=JobFilter(environment=EnvironmentTypes.local)
        )
        assert not res or res[0] != job_id
        # different worker can repull
        res = populated_full_queue.dequeue(
            hostname="second", filter=JobFilter(environment=EnvironmentTypes.local)
        )
        assert res[0] == job_id
        # first worker cannot update
        with pytest.raises(Exception):
            populated_full_queue.update_job_status(
                job_id, status=JobStates.running, hostname="first"
            )
        # second worker can update
        populated_full_queue.update_job_status(
            job_id, status=JobStates.running, hostname="second"
        )
        # fail
        time.sleep(6)
        populated_full_queue.handle_timeouts(max_retries=1, timeout_failure=5)
        assert populated_full_queue.get_job(job_id).status == "error"
