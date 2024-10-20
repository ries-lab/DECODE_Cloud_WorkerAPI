import abc
import os
import random
import threading
import time

import pytest
from fastapi import HTTPException

from workerfacing_api.core.queue import (
    JobStates,
    LocalJobQueue,
    RDSJobQueue,
    SQSJobQueue,
)


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
    def env_name(self):
        return "test-queue"

    def test_create_queue(self, queue, env_name):
        # test queue is empty
        assert queue.peek(hostname="i", environment=env_name)[0] is None

    def test_enqueue(self, populated_queue, env_name):
        assert populated_queue.peek(hostname="i", environment=env_name)[0] is not None

    def test_peek(self, populated_queue, env_name):
        assert (
            populated_queue.peek(hostname="i", environment=env_name)[0]["meta"][
                "job_id"
            ]
            == 0
        )
        # peeking does not remove elements
        assert (
            populated_queue.peek(hostname="i", environment=env_name)[0]["meta"][
                "job_id"
            ]
            == 0
        )

    def test_dequeue(self, populated_queue, env_name):
        assert (
            populated_queue.dequeue(hostname="i", environment=env_name)["meta"][
                "job_id"
            ]
            == 0
        )
        # dequeue removes elements
        assert (
            populated_queue.dequeue(hostname="i", environment=env_name)["meta"][
                "job_id"
            ]
            == 1
        )
        # None env jobs can be pulled by everyone
        assert (
            populated_queue.dequeue(hostname="i", environment=None)["meta"]["job_id"]
            == 2
        )
        # environment is filtered correctly
        assert populated_queue.dequeue(hostname="i", environment=env_name) is None

    def test_dequeue_old(self, populated_queue, env_name):
        # not old enough
        assert (
            populated_queue.dequeue(hostname="i", environment=env_name, older_than=2)
            is None
        )
        # old enough
        time.sleep(2)
        assert (
            populated_queue.dequeue(hostname="i", environment=env_name, older_than=2)
            is not None
        )

    def test_concurrent_pulls(self, populated_full_queue, env_name):
        found_jobs = [None] * 100

        def _pull(i):
            time.sleep(random.random() * 5)  # ensure enough time to pull all jobs
            found_jobs[i] = populated_full_queue.dequeue(
                hostname="i", environment=env_name
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
        assert len(found_jobs) == len(
            set([job["meta"]["job_id"] for job in found_jobs])
        )


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
    def job_queue(self, skip_local, env_name):
        # need new env name for each test (SQS queues can't be recreated after less than 60 seconds)
        job_queue = SQSJobQueue([env_name, f"not-{env_name}"])
        # wait for queue to be deleted before recreating
        while True:
            try:
                job_queue.create(err_on_exists=True)
                break
            except HTTPException:
                time.sleep(1)
        return job_queue


def _patched_func(queue, func_name):
    to_patch = getattr(queue, func_name)

    def patch(*args, **kwargs):
        for required_arg in ("cpu_cores", "memory", "gpu_mem"):
            if required_arg not in kwargs:
                kwargs[required_arg] = 9999
        return to_patch(*args, **kwargs)

    return patch


@pytest.fixture(autouse=True, scope="module")
def patch_queue_funcs(monkeypatch_module):
    monkeypatch_module.setattr(RDSJobQueue, "peek", _patched_func(RDSJobQueue, "peek"))
    monkeypatch_module.setattr(
        RDSJobQueue, "dequeue", _patched_func(RDSJobQueue, "dequeue")
    )


class TestRDSLocalQueue(_TestJobQueue):
    @pytest.fixture(scope="function")
    def job_queue(self, skip_aws, tmpdir, env_name):
        job_queue = RDSJobQueue(f"sqlite:///{tmpdir}/{env_name}.db")
        job_queue.create()
        return job_queue

    # additional tests for additional functionality
    def test_filtering(self, populated_full_queue, env_name):
        assert (
            populated_full_queue.peek(
                hostname="i", environment=env_name, cpu_cores=2, memory=1
            )[0]["meta"]["job_id"]
            == 1
        )
        assert (
            populated_full_queue.peek(hostname="i", environment=env_name, memory=1)[0][
                "meta"
            ]["job_id"]
            == 1
        )
        assert (
            populated_full_queue.peek(
                hostname="i", environment=env_name, gpu_model="gpu_model"
            )[0]["meta"]["job_id"]
            == 1
        )
        assert (
            populated_full_queue.peek(
                hostname="i", environment=env_name, gpu_archi="gpu_archi"
            )[0]["meta"]["job_id"]
            == 1
        )

    def test_priorities(self, populated_full_queue, env_name):
        # group priority
        assert (
            populated_full_queue.dequeue(
                hostname="i", environment=env_name, groups=["group", "another group"]
            )["meta"]["job_id"]
            == 2
        )
        # job priority
        assert (
            populated_full_queue.dequeue(hostname="i", environment=env_name)["meta"][
                "job_id"
            ]
            == 1
        )
        assert (
            populated_full_queue.dequeue(hostname="i", environment=env_name)["meta"][
                "job_id"
            ]
            == 3
        )

    def test_dequeue_old(self, populated_queue, env_name):
        # older_than does not apply when the right environment is selected
        # not old enough
        assert (
            populated_queue.dequeue(hostname="i", environment=env_name, older_than=5)
            is not None
        )
        assert (
            populated_queue.dequeue(hostname="i", environment=env_name, older_than=5)
            is not None
        )
        assert (
            populated_queue.dequeue(hostname="i", environment=env_name, older_than=5)
            is None
        )
        # old enough
        time.sleep(5)
        assert (
            populated_queue.dequeue(hostname="i", environment=env_name, older_than=5)
            is not None
        )

    def test_failures(self, populated_full_queue, env_name):
        job_id = populated_full_queue.dequeue(hostname="first", environment=env_name)[
            "job_id"
        ]
        res = populated_full_queue.dequeue(hostname="second", environment=env_name)
        assert not res or res["job_id"] != job_id
        # fail -> requeue
        time.sleep(6)
        populated_full_queue.handle_timeouts(max_retries=1, timeout_failure=5)
        assert populated_full_queue.get_job(job_id).status == "queued"
        # same worker can not repull
        res = populated_full_queue.dequeue(hostname="first", environment=env_name)
        assert not res or res["job_id"] != job_id
        # different worker can repull
        res = populated_full_queue.dequeue(hostname="second", environment=env_name)
        assert res["job_id"] == job_id
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
