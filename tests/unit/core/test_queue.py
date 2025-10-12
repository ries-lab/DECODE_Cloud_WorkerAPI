import abc
import datetime
import random
import threading
import time
from contextlib import nullcontext
from typing import Any, Generator, cast

import boto3
import pytest
from moto import mock_aws

from tests.conftest import RDSTestingInstance
from workerfacing_api.core.queue import (
    JobQueue,
    LocalJobQueue,
    RDSJobQueue,
    SQSJobQueue,
)
from workerfacing_api.schemas.queue_jobs import (
    AppSpecs,
    EnvironmentTypes,
    HandlerSpecs,
    HardwareSpecs,
    JobFilter,
    JobSpecs,
    MetaSpecs,
    PathsUploadSpecs,
    SubmittedJob,
)
from workerfacing_api.schemas.rds_models import JobStates


def get_job(
    job_id: int,
    env: EnvironmentTypes | None = None,
    hw_specs: HardwareSpecs | None = None,
    group: str | None = None,
    priority: int = 5,
) -> SubmittedJob:
    time_now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return SubmittedJob(
        job=JobSpecs(
            app=AppSpecs(cmd=["cmd"], env={"env": "var"}),
            handler=HandlerSpecs(image_url="u", files_up={"output": "out"}),
            hardware=hw_specs or HardwareSpecs(),
            meta=MetaSpecs(job_id=job_id, date_created=time_now),
        ),
        environment=env or EnvironmentTypes.local,
        group=group,
        priority=priority,
        paths_upload=PathsUploadSpecs(output="out", log="log", artifact="art"),
    )


class _TestJobQueue(abc.ABC):
    @pytest.fixture(scope="class")
    def base_queue(self, *args: Any, **kwargs: Any) -> Generator[JobQueue, Any, None]:
        # class-scoped, creates the base infrastructure
        raise NotImplementedError

    @pytest.fixture
    def queue(
        self, base_queue: JobQueue, *args: Any, **kwargs: Any
    ) -> Generator[JobQueue, Any, None]:
        # function-scoped, clears the queue (delete)
        # and re-initializes it (create) before every test
        base_queue.delete()
        success = False
        for _ in range(10):  # i.p. SQS, RDS, etc. might need some time to delete
            try:
                base_queue.create(err_on_exists=True)
                success = True
                break
            except Exception:
                time.sleep(10)
        if not success:
            raise RuntimeError("Could not create queue")
        yield base_queue
        base_queue.delete()

    @pytest.fixture
    def job_filter(self) -> JobFilter:
        return JobFilter(environment=EnvironmentTypes.local)

    def test_initial_queue_empty(self, queue: JobQueue, job_filter: JobFilter) -> None:
        assert queue.peek(hostname="i", filter=job_filter) is None

    def test_enqueue_peek(self, queue: JobQueue, job_filter: JobFilter) -> None:
        queue.enqueue(get_job(0))
        assert queue.peek(hostname="i", filter=job_filter) is not None

    def test_peek_idempotent(self, queue: JobQueue, job_filter: JobFilter) -> None:
        queue.enqueue(get_job(0))
        job = queue.peek(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 0
        # peeking does not remove elements
        job = queue.peek(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 0

    def test_dequeue_removes(self, queue: JobQueue, job_filter: JobFilter) -> None:
        queue.enqueue(get_job(0))
        job = queue.dequeue(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 0
        # dequeue removes elements
        assert queue.dequeue(hostname="i", filter=job_filter) is None

    def test_dequeue_wrong_env(self, queue: JobQueue, job_filter: JobFilter) -> None:
        queue.enqueue(get_job(0, env=EnvironmentTypes.cloud))
        job = queue.dequeue(hostname="i", filter=job_filter)
        assert job is None

    def test_dequeue_none_env(self, queue: JobQueue, job_filter: JobFilter) -> None:
        queue.enqueue(get_job(0, env=EnvironmentTypes.any))
        job = queue.dequeue(hostname="i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 0

    def test_dequeue_older_than_check(
        self, queue: JobQueue, job_filter: JobFilter
    ) -> None:
        filter = job_filter.model_copy()
        filter.older_than = 15
        # jobs in the same environment can immediately be pulled
        queue.enqueue(get_job(0))
        assert queue.dequeue(hostname="i", filter=filter) is not None
        # jobs in the "any" environment must be old enough
        queue.enqueue(get_job(0, env=EnvironmentTypes.any))
        assert queue.dequeue(hostname="i", filter=filter) is None
        time.sleep(15)
        assert queue.dequeue(hostname="i", filter=filter) is not None

    def test_concurrent_pulls(self, queue: JobQueue, job_filter: JobFilter) -> None:
        queue.enqueue(get_job(0))
        queue.enqueue(get_job(1))
        queue.enqueue(get_job(2))

        found_jobs: list[None | tuple[int, JobSpecs]] = [None] * 100

        def _pull(i: int) -> None:
            time.sleep(random.random() * 5)  # ensure enough time to pull all jobs
            found_jobs[i] = queue.dequeue(hostname=str(i), filter=job_filter)

        threads = [threading.Thread(target=_pull, args=(i,)) for i in range(100)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        found_jobs_filt: list[tuple[int, JobSpecs]] = [
            job for job in found_jobs if job is not None
        ]
        # no duplicates
        assert (
            3
            == len(found_jobs_filt)
            == len(set([job[1].meta.job_id for job in found_jobs_filt]))
        )


class TestLocalQueue(_TestJobQueue):
    @pytest.fixture(scope="class")
    def base_queue(
        self, tmpdir_factory: pytest.TempdirFactory
    ) -> Generator[LocalJobQueue, Any, None]:
        queue_path = str(tmpdir_factory.mktemp("queue") / "queue.pkl")
        base_queue = LocalJobQueue(queue_path)
        yield base_queue


class TestSQSQueue(_TestJobQueue):
    @pytest.fixture(
        params=[True, pytest.param(False, marks=pytest.mark.aws)], scope="class"
    )
    def mock_aws_(self, request: pytest.FixtureRequest) -> bool:
        return cast(bool, request.param)

    @pytest.fixture(scope="class")
    def base_queue(self, mock_aws_: bool) -> Generator[SQSJobQueue, Any, None]:
        context_manager = mock_aws if mock_aws_ else nullcontext
        with context_manager():
            yield SQSJobQueue(boto3.client("sqs", "eu-central-1"))


class _TestRDSQueue(_TestJobQueue, abc.ABC):
    @pytest.fixture(scope="class")
    def base_queue(
        self, *args: Any, **kwargs: Any
    ) -> Generator[RDSJobQueue, Any, None]:
        raise NotImplementedError

    # additional tests for additional functionality
    def test_filtering(self, queue: RDSJobQueue, job_filter: JobFilter) -> None:
        queue.enqueue(get_job(0, hw_specs=HardwareSpecs(cpu_cores=4, memory=10)))
        queue.enqueue(get_job(1, hw_specs=HardwareSpecs(gpu_model="m", gpu_archi="a")))
        queue.enqueue(get_job(2))

        job = queue.peek("i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 2

        job = queue.peek(
            "i", filter=job_filter.model_copy(update={"cpu_cores": 8, "memory": 20})
        )
        assert job is not None
        assert job[1].meta.job_id == 0

        job = queue.peek(
            "i",
            filter=job_filter.model_copy(update={"gpu_model": "m", "gpu_archi": "a"}),
        )
        assert job is not None
        assert job[1].meta.job_id == 1

    def test_priorities(self, queue: RDSJobQueue, job_filter: JobFilter) -> None:
        # priorities: first own group, then priority
        queue.enqueue(get_job(0, priority=3))
        queue.enqueue(get_job(1, priority=5))
        queue.enqueue(get_job(2, group="group", priority=1))

        job = queue.peek("i", filter=job_filter)
        assert job is not None
        assert job[1].meta.job_id == 1

        job = queue.peek(
            "i", filter=job_filter.model_copy(update={"groups": ["group", "another"]})
        )
        assert job is not None
        assert job[1].meta.job_id == 2

    def test_dequeue_old_expanded(
        self, queue: RDSJobQueue, job_filter: JobFilter
    ) -> None:
        # older_than does not apply when the right environment is
        queue.enqueue(get_job(0, env=EnvironmentTypes.any))
        queue.enqueue(get_job(1))

        job = queue.dequeue(
            "i", filter=job_filter.model_copy(update={"older_than": 15})
        )
        assert job is not None
        assert job[1].meta.job_id == 1

        assert (
            queue.dequeue("i", filter=job_filter.model_copy(update={"older_than": 15}))
            is None
        )
        time.sleep(15)
        job = queue.dequeue(
            "i", filter=job_filter.model_copy(update={"older_than": 15})
        )
        assert job is not None
        assert job[1].meta.job_id == 0

    def test_failures(self, queue: RDSJobQueue, job_filter: JobFilter) -> None:
        queue.enqueue(get_job(0))

        job = queue.dequeue("first", filter=job_filter)
        assert job is not None
        job_id, job_specs = job
        assert job_specs.meta.job_id == 0

        # fail -> requeue
        time.sleep(6)
        queue.handle_timeouts(max_retries=1, timeout_failure=5)
        assert queue.get_job(job_id).status == "queued"

        # same worker can not repull
        res = queue.dequeue("first", filter=job_filter)
        assert res is None

        # different worker can repull
        res = queue.dequeue("second", filter=job_filter)
        assert res is not None
        assert res[1].meta.job_id == 0

        # first worker cannot update
        with pytest.raises(Exception):
            queue.update_job_status(job_id, status=JobStates.running, hostname="first")

        # second worker can update
        queue.update_job_status(job_id, status=JobStates.running, hostname="second")

        # fail
        time.sleep(6)
        queue.handle_timeouts(max_retries=1, timeout_failure=5)
        assert queue.get_job(job_id).status == "error"


class TestRDSLocalQueue(_TestRDSQueue):
    @pytest.fixture(scope="class")
    def base_queue(
        self, tmpdir_factory: pytest.TempdirFactory
    ) -> Generator[RDSJobQueue, Any, None]:
        base_queue = RDSJobQueue(f"sqlite:///{tmpdir_factory.mktemp('queue')}/local.db")
        yield base_queue


@pytest.mark.aws
class TestRDSAWSQueue(_TestRDSQueue):
    @pytest.fixture(scope="class")
    def base_queue(
        self, rds_testing_instance: RDSTestingInstance
    ) -> Generator[RDSJobQueue, Any, None]:
        yield RDSJobQueue(rds_testing_instance.db_url)
        rds_testing_instance.cleanup()
