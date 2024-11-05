import datetime
import json
import os
import pickle
import threading
import time
from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Type

import botocore.exceptions
from deprecated import deprecated
from dict_hash import sha256  # type: ignore
from mypy_boto3_sqs import SQSClient
from sqlalchemy import create_engine, inspect, not_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Query, Session

from workerfacing_api import settings
from workerfacing_api.crud import job_tracking
from workerfacing_api.schemas.queue_jobs import (
    EnvironmentTypes,
    JobFilter,
    JobSpecs,
    SubmittedJob,
)
from workerfacing_api.schemas.rds_models import Base, JobStates, QueuedJob


class UpdateLock:
    """
    Context manager to lock a queue for update.
    Required for RDSQueue on SQLite since `with_for_update` does not lock there.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()

    def __enter__(self) -> None:
        self.lock.acquire()

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.lock.release()


class MockUpdateLock:
    """
    Mock context manager.
    Used for RDSQueue on databases that are not SQLite,
    since locking is already achieved via `with_for_update`.
    """

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass


class JobQueue(ABC):
    """Abstract multi-environment job queue."""

    @abstractmethod
    def create(self, err_on_exists: bool = True) -> None:
        """Create the initialized queue."""
        raise NotImplementedError

    @abstractmethod
    def delete(self) -> None:
        """Delete the queue."""
        raise NotImplementedError

    @abstractmethod
    def enqueue(self, job: SubmittedJob) -> None:
        """Push a new job to the queue."""
        raise NotImplementedError

    @abstractmethod
    def peek(
        self, hostname: str, filter: JobFilter
    ) -> tuple[int, JobSpecs, str] | None:
        """Look at first element in the queue.

        Returns:
         - job id
         - job
         - receipt handle (to provide to the pop method to delete the job from the queue)
        If a job is found, else None.
        """
        raise NotImplementedError

    @abstractmethod
    def pop(self, environment: EnvironmentTypes, receipt_handle: str) -> bool:
        """Delete job from the queue."""
        raise NotImplementedError

    def dequeue(self, hostname: str, filter: JobFilter) -> tuple[int, JobSpecs] | None:
        """Peek last element and remove it from the queue if it is older than `older_than'."""
        # get last element
        res = self.peek(hostname=hostname, filter=filter)
        # if element found
        if res:
            id_, item, receipt_handle = res
            successful = self.pop(
                environment=filter.environment, receipt_handle=receipt_handle or ""
            )
            if not successful:
                # might have been an unspecified environment
                successful = self.pop(
                    environment=EnvironmentTypes.any,
                    receipt_handle=receipt_handle or "",
                )
            if successful:
                return id_, item
            else:  # job pulled by other worker first, get another one
                return self.dequeue(hostname=hostname, filter=filter)
        return None


@deprecated(reason="Using a database as queue for enhanced job tracking and filtering")
class LocalJobQueue(JobQueue):
    """Local job queue, for testing purposes only."""

    def __init__(self, queue_path: str):
        self.queue_path = queue_path
        self.update_lock = UpdateLock()

    def create(self, err_on_exists: bool = True) -> None:
        if os.path.exists(self.queue_path) and err_on_exists:
            raise ValueError("A queue at this path already exists.")
        queue: dict[EnvironmentTypes, list[SubmittedJob]] = {
            env: [] for env in EnvironmentTypes
        }
        with open(self.queue_path, "wb") as f:
            pickle.dump(queue, f)

    def delete(self) -> None:
        try:
            os.remove(self.queue_path)
        except FileNotFoundError:
            pass

    def enqueue(self, job: SubmittedJob) -> None:
        with open(self.queue_path, "rb+") as f:
            # Read in
            queue = pickle.load(f)
            # Push new job
            queue_item = {
                **job.model_dump(),
                "creation_timestamp": datetime.datetime.now(datetime.timezone.utc),
            }
            queue[job.environment] = queue[job.environment] + [queue_item]
            # Overwrite
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)

    def peek(
        self, hostname: str, filter: JobFilter
    ) -> tuple[int, JobSpecs, str] | None:
        if extra_fields := (filter.model_fields_set - {"environment", "older_than"}):
            raise ValueError(f"{extra_fields} not accepted as filters for {type(self)}")
        with open(self.queue_path, "rb+") as f:
            queue = pickle.load(f)
        queue_item: dict[str, Any] = {}
        if len(queue.get(filter.environment, list())):
            queue_item = queue[filter.environment][0]
            env = filter.environment
        elif len(queue.get(EnvironmentTypes.any, list())):
            queue_item = queue[EnvironmentTypes.any][0]
            env = EnvironmentTypes.any
        if queue_item:
            age = (
                datetime.datetime.now(datetime.timezone.utc)
                - queue_item["creation_timestamp"]
            )
            if (
                age > datetime.timedelta(seconds=filter.older_than)
                or env == filter.environment
            ):
                job_id = int(queue_item["job"]["meta"]["job_id"])
                return (
                    job_id,
                    JobSpecs(**queue_item["job"]),
                    sha256(queue_item),
                )
        return None

    def pop(self, environment: EnvironmentTypes, receipt_handle: str) -> bool:
        with self.update_lock:
            with open(self.queue_path, "rb+") as f:
                queue = pickle.load(f)
                if (
                    len(queue[environment]) == 0
                    or sha256(queue[environment][0]) != receipt_handle
                ):
                    return False
                queue[environment] = queue[environment][1:]
                f.seek(0)
                f.truncate()
                pickle.dump(queue, f)
            return True


@deprecated(reason="Using a database as queue for enhanced job tracking and filtering")
class SQSJobQueue(JobQueue):
    """SQS job queue. Not used anymore since it lacks filtering and prioritization."""

    def __init__(self, sqs_client: SQSClient):
        self.sqs_client = sqs_client
        self.queue_names = {}
        for environment in EnvironmentTypes:
            self.queue_names[environment] = f"{str(environment.value)}_queue.fifo"
        self.queue_urls = dict()
        for environment, queue_name in self.queue_names.items():
            try:
                self.queue_urls[environment] = self.sqs_client.get_queue_url(
                    QueueName=queue_name
                )["QueueUrl"]
            except self.sqs_client.exceptions.QueueDoesNotExist:
                pass

    def create(self, err_on_exists: bool = True) -> None:
        for environment, queue_name in self.queue_names.items():
            try:
                res = self.sqs_client.create_queue(
                    QueueName=queue_name,
                    Attributes={
                        "FifoQueue": "true",
                        "ContentBasedDeduplication": "true",
                        "VisibilityTimeout": "5",
                    },
                )
                self.queue_urls[environment] = res["QueueUrl"]
            except self.sqs_client.exceptions.QueueNameExists:
                if err_on_exists:
                    raise ValueError(
                        f"A queue with the name {queue_name} already exists."
                    )

    def delete(self) -> None:
        for queue_url in self.queue_urls.values():
            self.sqs_client.delete_queue(QueueUrl=queue_url)

    def enqueue(self, job: SubmittedJob) -> None:
        try:
            queue_item = {
                **{
                    k: (v.value if k == "environment" else v)
                    for k, v in job.model_dump().items()
                },
                "creation_timestamp": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            self.sqs_client.send_message(
                QueueUrl=self.queue_urls[job.environment],
                MessageBody=json.dumps(queue_item),
                MessageGroupId="0",
            )
        except botocore.exceptions.ClientError as error:
            raise ValueError(f"Error sending message to SQS queue: {error}.")

    def peek(
        self, hostname: str, filter: JobFilter
    ) -> tuple[int, JobSpecs, str] | None:
        if extra_fields := (filter.model_fields_set - {"environment", "older_than"}):
            raise ValueError(f"{extra_fields} filters not accepted for {type(self)}")
        # older_than argument not supported
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_urls[filter.environment],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
            )
            env = filter.environment
            if not response.get("Messages"):
                # try pulling from any queue
                response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_urls[EnvironmentTypes.any],
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=10,
                )
                env = EnvironmentTypes.any
        except botocore.exceptions.ClientError as error:
            raise ValueError(f"Error receiving message from SQS queue: {error}.")
        if len(response.get("Messages", [])):
            message = response["Messages"][0]
            message_body = message["Body"]
            queue_item = json.loads(message_body)
            if (
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.datetime.fromisoformat(queue_item["creation_timestamp"])
                > datetime.timedelta(seconds=filter.older_than)
            ) or env == filter.environment:
                receipt_handle = message["ReceiptHandle"]
                return (
                    queue_item["job"]["meta"]["job_id"],
                    JobSpecs(**queue_item["job"]),
                    receipt_handle,
                )
        return None

    def pop(self, environment: EnvironmentTypes, receipt_handle: str) -> bool:
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_urls[environment],
                ReceiptHandle=receipt_handle,
            )
        except botocore.exceptions.ClientError:
            return False
        return True


class RDSJobQueue(JobQueue):
    """Relational Database System job queue.
    Allows enhanced filtering and prioritization by not being a pure queue.
    Allows job tracking.
    """

    def __init__(self, db_url: str, max_retries: int = 10, retry_wait: int = 60):
        self.db_url = db_url
        self.update_lock = (
            UpdateLock() if self.db_url.startswith("sqlite") else MockUpdateLock()
        )
        self.engine = self._get_engine(self.db_url, max_retries, retry_wait)
        self.table_name = QueuedJob.__tablename__

    def _get_engine(self, db_url: str, max_retries: int, retry_wait: int) -> Engine:
        retries = 0
        while retries < max_retries:
            try:
                engine = create_engine(
                    db_url,
                    connect_args=(
                        {"check_same_thread": False}
                        if db_url.startswith("sqlite")
                        else {}
                    ),
                )
                # Attempt to create a connection or perform any necessary operations
                engine.connect()
                return engine  # Connection successful
            except Exception:
                retries += 1
                time.sleep(retry_wait)
        raise RuntimeError("Could not create engine.")

    def create(self, err_on_exists: bool = True) -> None:
        inspector = inspect(self.engine)
        if inspector.has_table(self.table_name) and err_on_exists:
            raise ValueError(f"A table with the name {self.table_name} already exists.")
        Base.metadata.create_all(self.engine)

    def delete(self) -> None:
        Base.metadata.drop_all(self.engine)

    def enqueue(self, job: SubmittedJob) -> None:
        with Session(self.engine) as session:
            session.add(
                QueuedJob(
                    job=job.job.model_dump(),
                    paths_upload=job.paths_upload.model_dump(),
                    environment=job.environment.value,
                    # None values in the resource requirements will make any puller match
                    cpu_cores=job.job.hardware.cpu_cores,
                    memory=job.job.hardware.memory,
                    gpu_model=job.job.hardware.gpu_model,
                    gpu_archi=job.job.hardware.gpu_archi,
                    gpu_mem=job.job.hardware.gpu_mem,
                    group=job.group,  # TODO: still to add to job model
                    priority=job.priority,
                    status=JobStates.queued.value,
                )
            )
            session.commit()

    def peek(
        self,
        hostname: str,
        filter: JobFilter,
    ) -> tuple[int, JobSpecs, str] | None:
        groups = filter.groups or []
        if ";" in hostname:
            # TODO: make sure not possible in endpoint
            raise ValueError("Hostname cannot contain ; for technical reasons.")
        with Session(self.engine) as session:
            query = session.query(QueuedJob)

            def filter_sort_query(query: Query[QueuedJob]) -> QueuedJob | None:
                query = query.filter(
                    QueuedJob.status == JobStates.queued.value,
                    (
                        (
                            (
                                QueuedJob.creation_timestamp
                                < datetime.datetime.now(datetime.timezone.utc)
                                - datetime.timedelta(seconds=filter.older_than)
                            )
                            & (QueuedJob.environment.is_(None))
                        )
                        | (QueuedJob.environment == filter.environment.value)
                    ),  # right environment pulls its jobs immediately
                    (QueuedJob.cpu_cores <= filter.cpu_cores)
                    | (QueuedJob.cpu_cores.is_(None)),
                    (QueuedJob.memory <= filter.memory) | (QueuedJob.memory.is_(None)),
                    (QueuedJob.gpu_model == filter.gpu_model)
                    | (QueuedJob.gpu_model.is_(None)),
                    (QueuedJob.gpu_archi == filter.gpu_archi)
                    | (QueuedJob.gpu_archi.is_(None)),
                    (QueuedJob.gpu_mem <= filter.gpu_mem)
                    | (QueuedJob.gpu_mem.is_(None)),
                )
                if settings.retry_different:
                    # only if worker did not already try running this job
                    query = query.filter(not_(QueuedJob.workers.contains(hostname)))
                query = query.order_by(QueuedJob.priority.desc()).order_by(
                    QueuedJob.creation_timestamp.asc()
                )
                ret = query.first()
                if ret is not None:
                    assert isinstance(ret, QueuedJob)
                return ret

            # prioritize private jobs
            job = filter_sort_query(query.filter(QueuedJob.group.in_(groups)))
            if job is None:
                job = filter_sort_query(query)
            if job:
                return job.id, JobSpecs(**job.job), json.dumps((job.id, hostname))  # type: ignore
        return None

    def pop(self, environment: EnvironmentTypes, receipt_handle: str) -> bool:
        with self.update_lock:
            job_id, hostname = json.loads(receipt_handle)
            with Session(self.engine) as session:
                try:
                    job = self.get_job(job_id, session, lock=True)
                except Exception:
                    return False
                if job.status != JobStates.queued.value:
                    return False
                job.workers = ";".join(job.workers.split(";") + [hostname])  # type: ignore
                self._update_job_status(session, job, status=JobStates.pulled)
            return True

    def get_job(
        self,
        job_id: int,
        session: Session | None = None,
        lock: bool = False,
        hostname: str | None = None,
    ) -> QueuedJob:
        """Get job information, not necessarily in queue."""
        if not session:
            with Session(self.engine) as session:
                return self.get_job(job_id, session, lock, hostname)
        res = session.query(QueuedJob).filter(QueuedJob.id == job_id)
        if lock:
            res = res.with_for_update(of=QueuedJob, nowait=True)
        job = res.first()
        if not job:
            raise RuntimeError(
                f"Job with id {job_id} not found (might have been pulled by another worker)"
            )
        if hostname:
            workers = job.workers.split(";")
            if not workers or hostname != workers[-1]:
                raise ValueError(
                    f"Job with id {job_id} is not assigned to worker {hostname}"
                )
        return job

    def _update_job_status(
        self,
        session: Session,
        job: QueuedJob,
        status: JobStates,
        runtime_details: str | None = None,
    ) -> None:
        """Internal job status update handler."""
        job.status = status.value  # type: ignore
        job.last_updated = datetime.datetime.now(datetime.timezone.utc)  # type: ignore
        session.add(job)
        session.commit()
        try:
            job_id = job.job["meta"]["job_id"]
            assert isinstance(job_id, int)
            job_tracking.update_job(job_id, status, runtime_details)
        except ValueError as e:
            # job probably deleted by user
            session.delete(job)
            session.commit()
            raise ValueError(f"Could not update job, probably deleted by user: {e}")

    def update_job_status(
        self,
        job_id: int,
        status: JobStates,
        runtime_details: str | None = None,
        hostname: str | None = None,
    ) -> None:
        """External entrypoint for job status updates by workers."""
        with Session(self.engine) as session:
            job = self.get_job(job_id, session, lock=True, hostname=hostname)
            self._update_job_status(session, job, status, runtime_details)

    def handle_timeouts(
        self, max_retries: int, timeout_failure: int
    ) -> tuple[int, int]:
        """Handle a timeout (keepalive signal not received for a long time)."""
        n_retry, n_failed = 0, 0
        time_now = datetime.datetime.now(datetime.timezone.utc)
        with Session(self.engine) as session:
            jobs_timeout = session.query(QueuedJob).filter(
                (QueuedJob.status == JobStates.pulled.value)
                | (QueuedJob.status == JobStates.preprocessing.value)
                | (QueuedJob.status == JobStates.running.value)
                | (QueuedJob.status == JobStates.postprocessing.value),
                (
                    QueuedJob.last_updated
                    < time_now - datetime.timedelta(seconds=timeout_failure)
                ),
            )
            jobs_retry = jobs_timeout.filter(QueuedJob.num_retries < max_retries)
            for job in jobs_retry:
                # TODO: increase priority?
                job.num_retries += 1  # type: ignore
                session.add(job)
                self.update_job_status(
                    job.id,  # type: ignore
                    JobStates.queued,
                    f"timeout {job.num_retries} (workers tried: {job.workers})",
                )
                n_retry += 1
            jobs_failed = jobs_timeout.filter(QueuedJob.num_retries >= max_retries)
            for job in jobs_failed:
                self.update_job_status(
                    job.id,  # type: ignore
                    JobStates.error,
                    "max retries reached",
                )
                n_failed += 1
            session.commit()
        return n_retry, n_failed
