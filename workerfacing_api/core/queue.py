import boto3
import botocore
import datetime
import json
import os
import pickle
import time
from abc import ABC, abstractmethod
from deprecated import deprecated
from dict_hash import sha256
from fastapi import HTTPException, status
from typing import Tuple
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from workerfacing_api import settings
from workerfacing_api.schemas.rds_models import QueuedJob, JobStates, Base
from workerfacing_api.crud.job_tracking import update_job


class JobQueue(ABC):
    """Abstract multi-environment job queue.
    """

    @abstractmethod
    def create(self, err_on_exists: bool = True):
        """Create the initialized queue.
        """
        pass

    @abstractmethod
    def delete(self):
        """Delete the queue.
        """
        pass

    @abstractmethod
    def enqueue(self, environment: str | None, item: dict):
        """Push a new job to the queue.
        """
        pass

    @abstractmethod
    def peek(self, hostname: str, environment: str | None, older_than: int = 0) -> Tuple[dict, str | None] | None:
        """Look at first element in the queue.
        
        Returns:
         - job (as a dict)
         - receipt handle (to provide to the pop method to delete the job from the queue)
        """
        pass

    @abstractmethod
    def pop(self, environment: str | None, receipt_handle: str):
        """Delete job from the queue.
        """
        pass

    def dequeue(self, environment: str | None, older_than: int = 0, **kwargs) -> dict | None:
        """Peek last element and remove it from the queue if it is older than `older_than'.
        """
        # get last element
        item, receipt_handle = self.peek(environment=environment, older_than=older_than, **kwargs)
        # if element found
        if item:
            # check "old enough"
            time_now = datetime.datetime.utcnow()
            item_age = time_now - datetime.datetime.fromisoformat(item['meta']['date_created'])
            if item_age > datetime.timedelta(seconds=older_than):
                # remove from queue and return
                # concurrency handled by the fact that if the object cannot be popped
                # (was already popped by another worker), the peek will return None
                # and not the same object
                self.pop(environment=environment, receipt_handle=receipt_handle)
                return item
        return None
    
    @abstractmethod
    def get_job(self, job_id: str):
        """Get job information, not necessarily in queue.
        """
        pass
    
    @abstractmethod
    def update_job_status(self, job_id: int, status: JobStates):
        """Update the job status.
        """
        pass
    
    @abstractmethod
    def handle_timeouts(self, max_retries: int, timeout_failure: int):
        """Handle a timeout (keepalive signal not received).
        """
        pass


@deprecated(reason="Using a database as queue")
class LocalJobQueue(JobQueue):
    """Local job queue, for testing purposes only.
    """

    def __init__(self, queue_path: str):
        self.queue_path = queue_path
    
    def create(self, err_on_exists: bool = True):
        if os.path.exists(self.queue_path) and err_on_exists:
            raise ValueError("A queue at this path already exists.")
        queue = dict()
        with open(self.queue_path, 'wb') as f:
            pickle.dump(queue, f)
    
    def delete(self):
        try:
            os.remove(self.queue_path)
        except:
            pass
    
    def enqueue(self, environment: str | None, item: dict):
        with open(self.queue_path, 'rb+') as f:
            # Read in
            queue = pickle.load(f)
            # Push new job
            queue[environment] = queue.get(environment, list()) + [item]
            # Overwrite
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)
        super().enqueue(environment, item)
    
    def peek(self, hostname: str, environment: str | None, older_than: int = 0) -> Tuple[dict | None, str | None]:
        # older than argument not supported
        with open(self.queue_path, 'rb+') as f:
            queue = pickle.load(f)
        if len(queue.get(environment, list())):
            item = queue[environment][0]
            return item, sha256(item)
        return None, None

    def pop(self, environment: str | None, receipt_handle):
        with open(self.queue_path, 'rb+') as f:
            queue = pickle.load(f)
            if queue.get(environment):
                if sha256(queue[environment][0]) != receipt_handle:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Element not first in queue.")
            queue[environment] = queue[environment][1:]
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)
    
    def get_job(self, job_id: str):
        raise NotImplementedError("This method is implemented only for RDS queues.")

    def update_job_status(self, job_id: int, status: JobStates):
        raise NotImplementedError("This method is implemented only for RDS queues.")
    
    def handle_timeouts(self, max_retries: int, timeout_failure: int):
        raise NotImplementedError("This method is implemented only for RDS queues.")


@deprecated(reason="Using a database as queue")
class SQSJobQueue(JobQueue):
    """SQS job queue.
    """

    def __init__(self, environments: list[str | None], sqs_client = None):
        self.sqs_client = sqs_client or boto3.client('sqs')
        self.queue_names = {}
        if None not in environments:
            environments = [None] + environments
        for environment in environments:
            self.queue_names[environment] = f'{str(environment)}_queue.fifo'
        self.queue_urls = dict()
        for environment, queue_name in self.queue_names.items():
            try:
                self.queue_urls[environment] = self.sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
            except self.sqs_client.exceptions.QueueDoesNotExist:
                pass

    def create(self, err_on_exists: bool = True):
        for environment, queue_name in self.queue_names.items():
            try:
                res = self.sqs_client.create_queue(QueueName=queue_name, Attributes={
                    'FifoQueue': 'true', 'ContentBasedDeduplication': 'true', 'VisibilityTimeout': '5'}
                )
                self.queue_urls[environment] = res['QueueUrl']
            except self.sqs_client.exceptions.QueueNameExists:
                if err_on_exists:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"A queue with the name {queue_name} already exists."
                    )

    def delete(self):
        for queue_url in self.queue_urls.values():
            self.sqs_client.delete_queue(QueueUrl=queue_url)
    
    def delete_env(self, environment: str | None):
        self.sqs_client.delete_queue(QueueUrl=self.queue_urls[environment])

    def enqueue(self, environment: str | None, item: dict):
        try:
            self.sqs_client.send_message(
                QueueUrl=self.queue_urls[environment],
                MessageBody=json.dumps(item),
                MessageGroupId="0",
            )
        except botocore.exceptions.ClientError as error:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error sending message to SQS queue: {error}.")
        super().enqueue(environment, item)

    def peek(self, hostname: str, environment: str, older_than: int = 0) -> Tuple[dict | None, str | None]:
        # older_than argument not supported
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_urls[environment],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
            )
        except botocore.exceptions.ClientError as error:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error receiving message from SQS queue: {error}.")
        if len(response.get('Messages', [])):
            message = response['Messages'][0]
            message_body = message['Body']
            item = json.loads(message_body)
            receipt_handle = message['ReceiptHandle']
            return item, receipt_handle
        return None, None

    def pop(self, environment: str, receipt_handle: str):
        try:
            response = self.sqs_client.delete_message(
                QueueUrl=self.queue_urls[environment],
                ReceiptHandle=receipt_handle,
            )
        except botocore.exceptions.ClientError as error:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error deleting message from SQS queue: {error}.")
        return response

    def get_job(self, job_id: str):
        raise NotImplementedError("This method is implemented only for RDS queues.")

    def update_job_status(self, job_id: int, status: JobStates):
        raise NotImplementedError("This method is implemented only for RDS queues.")
    
    def handle_timeouts(self, max_retries: int, timeout_failure: int):
        raise NotImplementedError("This method is implemented only for RDS queues.")


class RDSJobQueue(JobQueue):
    """Relational Database System job queue.
    Allows other filter conditions and prioritization by not being a pure queue.
    """

    def __init__(self, db_url: str, max_retries: int = 10, retry_wait: int = 60):
        self.db_url = db_url
        self.engine = self._get_engine(self.db_url, max_retries, retry_wait)
        self.table_name = QueuedJob.__tablename__
    
    def _get_engine(self, db_url, max_retries: int, retry_wait: int):
        retries = 0
        while retries < max_retries:
            try:
                engine = create_engine(
                    db_url,
                    connect_args={"check_same_thread": False}
                    if db_url.startswith("sqlite")
                    else {},
                )
                # Attempt to create a connection or perform any necessary operations
                engine.connect()
                return engine  # Connection successful
            except Exception as e:
                print(f"Connection attempt failed: {str(e)}")
                retries += 1
                time.sleep(retry_wait)

    def create(self, err_on_exists: bool = True):  #TODO
        if self.engine.has_table(self.table_name) and err_on_exists:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"A table with the name {self.table_name} already exists."
            )
        Base.metadata.create_all(self.engine)

    def delete(self):
        Base.metadata.drop_all(self.engine)

    def enqueue(self, environment: str | None, item: dict):
        with Session(self.engine) as session:
            hw_specs = item.get('hardware') or {}
            session.add(QueuedJob(
                job=item["job"],
                path_upload=item["path_upload"],
                environment=environment,
                # None values in the resource requirements will make any puller match
                cpu_cores=hw_specs.get('cpu_cores'),
                memory=hw_specs.get('memory'),
                gpu_model=hw_specs.get('gpu_model'),
                gpu_archi=hw_specs.get('gpu_archi'),
                gpu_mem=hw_specs.get('gpu_mem'),
                group=item.get('group', None),  #TODO: still to add to job model
                priority=item.get('priority', 5),
                status=JobStates.queued.value,
            ))
            session.commit()

    def peek(
        self,
        hostname: str,
        cpu_cores: int,
        memory: int,
        gpu_mem: int,
        environment: str | None = None,
        gpu_model: str | None = None,
        gpu_archi: str | None = None,
        groups: list[str] | None = None,
        older_than: int = 0,
    ) -> Tuple[dict | None, str | None]:
        if groups is None:
            groups = []
        if "<w>" in hostname or "<\w>" in hostname:
            raise HTTPException(
                status_code=status.HTTP_412_PRECONDITION_FAILED,
                detail="Hostname cannot contain <w> or <\w> for technical reasons.",
            )
        worker_str = f"<w>{hostname}<\w>"
        with Session(self.engine) as session:
            query = session.query(QueuedJob)

            def filter_sort_query(query):
                ret = query.filter(
                    QueuedJob.status == JobStates.queued.value,
                    (((QueuedJob.creation_timestamp < datetime.datetime.utcnow() - datetime.timedelta(seconds=older_than))
                    & (QueuedJob.environment == None))
                    | (QueuedJob.environment == environment)),  # right environment pulls its jobs immediately
                    (QueuedJob.cpu_cores <= cpu_cores) | (QueuedJob.cpu_cores == None),
                    (QueuedJob.memory <= memory) | (QueuedJob.memory == None),
                    (QueuedJob.gpu_model == gpu_model) | (QueuedJob.gpu_model == None),
                    (QueuedJob.gpu_archi == gpu_archi) | (QueuedJob.gpu_archi == None),
                    (QueuedJob.gpu_mem <= gpu_mem) | (QueuedJob.gpu_mem == None),
                )
                if settings.retry_different:
                   # only if worker did not already try running this job
                   ret = ret.filter(QueuedJob.workers.contains(worker_str) == False)
                ret = ret.order_by(QueuedJob.priority.desc()).order_by(QueuedJob.creation_timestamp.asc())
                ret = ret.with_for_update().first()  # with_for_update locks concurrent pulls
                return ret

            # prioritize private jobs
            job = filter_sort_query(query.filter(QueuedJob.group.in_(groups)))
            if job is None:
                job = filter_sort_query(query)
            if job:
                self._update_job_status(session, job, status=JobStates.pulled)
                job.workers += worker_str
                session.add(job)
                session.commit()
                return {"job_id": job.id, **job.job}, str(job.id)
        return None, None

    def pop(self, environment: str, receipt_handle: str):
        # not doing anything, since we keep the job in the database for tracking
        pass
    
    def get_job(self, job_id: int, session = None):
        if not session:
            with Session(self.engine) as session:
                return self.get_job(job_id, session)
        res = session.query(QueuedJob).filter(QueuedJob.id == job_id).first()
        if not res:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with id {job_id} not found",
            )
        return res
    
    def _update_job_status(self, session, job, status: JobStates):
        job.status = status.value
        job.last_updated = datetime.datetime.utcnow()
        session.add(job)
        session.commit()
        update_job(job.job["meta"]["job_id"], status)
    
    def update_job_status(self, job_id: int, status: JobStates):
        with Session(self.engine) as session:
            job = self.get_job(job_id, session)
            self._update_job_status(session, job, status)
    
    def handle_timeouts(self, max_retries: int, timeout_failure: int):
        n_retry, n_failed = 0, 0
        time_now = datetime.datetime.utcnow()
        with Session(self.engine) as session:
            jobs_timeout = session.query(QueuedJob).filter(
                (QueuedJob.status == JobStates.running.value) | (QueuedJob.status == JobStates.pulled.value),
                (QueuedJob.last_updated < time_now - datetime.timedelta(seconds=timeout_failure)),
            )
            jobs_retry = jobs_timeout.filter(QueuedJob.num_retries < max_retries)
            for job in jobs_retry:
                #TODO: increase priority?
                job.num_retries += 1
                session.add(job)
                self.update_job_status(job.id, JobStates.queued)
                n_retry += 1
            jobs_failed = jobs_timeout.filter(QueuedJob.num_retries >= max_retries)
            for job in jobs_failed:
                self.update_job_status(job.id, JobStates.error)
                n_failed += 1
            session.commit()
            return n_retry, n_failed
