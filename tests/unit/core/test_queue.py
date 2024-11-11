import abc
import os
import random
import threading
import time
from pathlib import Path
from typing import Any, Generator, TypedDict

import boto3
import pytest
from fastapi import HTTPException

from workerfacing_api.core.queue import (
    JobQueue,
    LocalJobQueue,
    RDSJobQueue,
    SQSJobQueue,
)
from workerfacing_api.schemas.queue_jobs import (
    EnvironmentTypes,
    JobFilter,
    JobSpecs,
)
from workerfacing_api.schemas.rds_models import JobStates


@pytest.fixture
def skip_aws(env: str) -> Generator[str, Any, None]:
    if env != "local":
        pytest.skip("Only tested on local DB")
    yield env


@pytest.fixture
def skip_local(env: str) -> Generator[str, Any, None]:
    if env == "local":
        pytest.skip("Only tested on AWS")
    yield env


class _TestJobQueue(abc.ABC):
    @pytest.fixture(scope="function")
    def job_queue(self, *args: Any, **kwargs: Any) -> JobQueue:
        raise NotImplementedError

    @pytest.fixture
    def queue(self, job_queue: JobQueue) -> Generator[JobQueue, None, None]:
        yield job_queue
        job_queue.delete()

    @pytest.fixture
    def job_filter(self) -> JobFilter:
        return JobFilter(environment=EnvironmentTypes.local)

    def test_create_queue(self, queue: JobQueue, job_filter: JobFilter) -> None:
        # test queue is empty
        assert queue.peek(hostname="i", filter=job_filter) is None

    def test_enqueue(self, populated_queue: JobQueue, job_filter: JobFilter) -> None:
        assert populated_queue.peek(hostname="i", filter=job_filter) is not None

    def test_peek(self, populated_queue: JobQueue, job_filter: JobFilter) -> None:
        job = populated_queue.peek(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 0
        # peeking does not remove elements
        job = populated_queue.peek(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 0

    def test_dequeue(self, populated_queue: JobQueue, job_filter: JobFilter) -> None:
        job = populated_queue.dequeue(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 0
        # dequeue removes elements
        job = populated_queue.dequeue(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 1
        # None env jobs can be pulled by everyone
        job = populated_queue.dequeue(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 2
        # environment is filtered correctly
        assert populated_queue.dequeue(hostname="i", filter=job_filter) is None

    def test_dequeue_old(
        self, populated_queue: JobQueue, job_filter: JobFilter
    ) -> None:
        filter = job_filter.model_copy()
        filter.older_than = 15
        # jobs in the same environment can immediately be pulled
        assert populated_queue.dequeue(hostname="i", filter=filter) is not None
        assert populated_queue.dequeue(hostname="i", filter=filter) is not None
        # jobs in the "any" environment must be old enough
        assert populated_queue.dequeue(hostname="i", filter=filter) is None
        time.sleep(15)
        assert populated_queue.dequeue(hostname="i", filter=filter) is not None

    def test_concurrent_pulls(
        self, populated_full_queue: JobQueue, job_filter: JobFilter
    ) -> None:
        found_jobs: list[None | tuple[int, JobSpecs]] = [None] * 100

        def _pull(i: int) -> None:
            time.sleep(random.random() * 5)  # ensure enough time to pull all jobs
            found_jobs[i] = populated_full_queue.dequeue(
                hostname="i", filter=job_filter
            )

        threads = [threading.Thread(target=_pull, args=(i,)) for i in range(100)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        found_jobs_filt: list[tuple[int, JobSpecs]] = [
            job for job in found_jobs if job is not None
        ]
        # no duplicates
        assert len(found_jobs_filt) == len(
            set([job[1].meta.job_id for job in found_jobs_filt])
        )


class TestLocalQueue(_TestJobQueue):
    @pytest.fixture(scope="function")
    def job_queue(self, skip_aws: str, tmpdir: Path) -> LocalJobQueue:
        queue_path = str(tmpdir / "queue.pkl")
        if os.path.exists(queue_path):
            os.remove(queue_path)
        job_queue = LocalJobQueue(queue_path)
        job_queue.create()
        return job_queue


class TestSQSQueue(_TestJobQueue):
    @pytest.fixture
    def job_queue(self, skip_local: str) -> SQSJobQueue:
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
    def job_queue(self, skip_aws: str, tmpdir: Path) -> RDSJobQueue:
        job_queue = RDSJobQueue(f"sqlite:///{tmpdir}/local.db")
        job_queue.create()
        return job_queue

    # additional tests for additional functionality
    def test_filtering(self, populated_full_queue: RDSJobQueue) -> None:
        job = populated_full_queue.peek(
            hostname="i",
            filter=JobFilter(environment=EnvironmentTypes.local, cpu_cores=2, memory=1),
        )
        assert job is not None
        assert job[1].meta.job_id == 1
        job = populated_full_queue.peek(
            hostname="i",
            filter=JobFilter(environment=EnvironmentTypes.local, memory=1),
        )
        assert job is not None
        assert job[1].meta.job_id == 2
        job = populated_full_queue.peek(
            hostname="i",
            filter=JobFilter(
                environment=EnvironmentTypes.local,
                gpu_model="gpu_model",
                gpu_archi="gpu_archi",
            ),
        )
        assert job is not None
        assert job[1].meta.job_id == 0

    def test_priorities(self, populated_full_queue: RDSJobQueue) -> None:
        CommonFilter = TypedDict("CommonFilter", {"cpu_cores": int, "memory": int})
        common_filter: CommonFilter = {
            "cpu_cores": 10,
            "memory": 10,
        }
        # group priority
        job = populated_full_queue.dequeue(
            hostname="i",
            filter=JobFilter(
                environment=EnvironmentTypes.local,
                groups=["group", "another group"],
                **common_filter,
            ),
        )
        assert job is not None
        assert job[1].meta.job_id == 2
        # job priority
        job = populated_full_queue.dequeue(
            hostname="i",
            filter=JobFilter(environment=EnvironmentTypes.local, **common_filter),
        )
        assert job is not None
        assert job[1].meta.job_id == 1
        job = populated_full_queue.dequeue(
            hostname="i",
            filter=JobFilter(environment=EnvironmentTypes.local, **common_filter),
        )
        assert job is not None
        assert job[1].meta.job_id == 3

    def test_dequeue_old_expanded(self, populated_queue: RDSJobQueue) -> None:
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

    def test_failures(self, populated_full_queue: RDSJobQueue) -> None:
        job = populated_full_queue.dequeue(
            hostname="first", filter=JobFilter(environment=EnvironmentTypes.local)
        )
        assert job is not None
        job_id = job[0]
        job = populated_full_queue.dequeue(
            hostname="second", filter=JobFilter(environment=EnvironmentTypes.local)
        )
        assert not job or job[0] != job_id
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
        assert res is not None
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
