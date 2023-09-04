import boto3
import botocore
import datetime
import json
import os
import pickle
from abc import ABC, abstractmethod
from dict_hash import sha256
from fastapi import HTTPException
from typing import Tuple
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from workerfacing_api.core.rds_models import QueuedJob, Base


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
    def enqueue(self, env: str | None, item: dict):
        """Push a new job to the queue.
        """
        pass

    @abstractmethod
    def peek(self, env: str | None, older_than: float = 0) -> Tuple[dict, str | None] | None:
        """Look at first element in the queue.
        
        Returns:
         - job (as a dict)
         - receipt handle (to provide to the pop method to delete the job from the queue)
        """
        pass

    @abstractmethod
    def pop(self, env: str | None, receipt_handle: str):
        """Delete job from the queue.
        """
        pass

    def dequeue(self, env: str | None, older_than: float = 0, **kwargs) -> dict | None:
        """Peek last element and remove it from the queue if it is older than `older_than'.
        """
        # get last element
        item, receipt_handle = self.peek(env=env, older_than=older_than, **kwargs)
        # if element found
        if item:
            # check "old enough"
            time_now = datetime.datetime.utcnow()
            item_age = time_now - datetime.datetime.fromisoformat(item['date_created'])
            if item_age > datetime.timedelta(seconds=older_than):
                # remove from queue and return
                # concurrency handled by the fact that if the object cannot be popped
                # (was already popped by another worker), the peek will return None
                # and not the same object
                self.pop(env=env, receipt_handle=receipt_handle)
                return item
        return None


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
    
    def enqueue(self, env: str | None, item: dict):
        with open(self.queue_path, 'rb+') as f:
            # Read in
            queue = pickle.load(f)
            # Push new job
            queue[env] = queue.get(env, list()) + [item]
            # Overwrite
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)
    
    def peek(self, env: str | None, older_than: float = 0) -> Tuple[dict | None, str | None]:
        # older than argument not supported
        with open(self.queue_path, 'rb+') as f:
            queue = pickle.load(f)
        if len(queue.get(env, list())):
            item = queue[env][0]
            return item, sha256(item)
        return None, None

    def pop(self, env: str | None, receipt_handle):
        with open(self.queue_path, 'rb+') as f:
            queue = pickle.load(f)
            if queue.get(env):
                if sha256(queue[env][0]) != receipt_handle:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Element not first in queue.")
            queue[env] = queue[env][1:]
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)


class SQSJobQueue(JobQueue):
    """SQS job queue.
    """

    def __init__(self, envs: list[str | None], sqs_client = None):
        self.sqs_client = sqs_client or boto3.client('sqs')
        self.queue_names = {}
        if None not in envs:
            envs = [None] + envs
        for env in envs:
            self.queue_names[env] = f'{str(env)}_queue.fifo'
        self.queue_urls = dict()
        for env, queue_name in self.queue_names.items():
            try:
                self.queue_urls[env] = self.sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
            except self.sqs_client.exceptions.QueueDoesNotExist:
                pass

    def create(self, err_on_exists: bool = True):
        for env, queue_name in self.queue_names.items():
            try:
                res = self.sqs_client.create_queue(QueueName=queue_name, Attributes={
                    'FifoQueue': 'true', 'ContentBasedDeduplication': 'true', 'VisibilityTimeout': '5'}
                )
                self.queue_urls[env] = res['QueueUrl']
            except self.sqs_client.exceptions.QueueNameExists:
                if err_on_exists:
                    raise HTTPException(
                        status_code=400,
                        detail=f"A queue with the name {queue_name} already exists."
                    )

    def delete(self):
        for queue_url in self.queue_urls.values():
            self.sqs_client.delete_queue(QueueUrl=queue_url)
    
    def delete_env(self, env: str | None):
        self.sqs_client.delete_queue(QueueUrl=self.queue_urls[env])

    def enqueue(self, env: str | None, item: dict):
        try:
            self.sqs_client.send_message(
                QueueUrl=self.queue_urls[env],
                MessageBody=json.dumps(item),
                MessageGroupId="0",
            )
        except botocore.exceptions.ClientError as error:
            raise HTTPException(
                status_code=500,
                detail=f"Error sending message to SQS queue: {error}.")

    def peek(self, env: str, older_than: float = 0) -> Tuple[dict | None, str | None]:
        # older_than argument not supported
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_urls[env],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
            )
        except botocore.exceptions.ClientError as error:
            raise HTTPException(
                status_code=500,
                detail=f"Error receiving message from SQS queue: {error}.")
        if len(response.get('Messages', [])):
            message = response['Messages'][0]
            message_body = message['Body']
            item = json.loads(message_body)
            receipt_handle = message['ReceiptHandle']
            return item, receipt_handle
        return None, None

    def pop(self, env: str, receipt_handle: str):
        try:
            response = self.sqs_client.delete_message(
                QueueUrl=self.queue_urls[env],
                ReceiptHandle=receipt_handle,
            )
        except botocore.exceptions.ClientError as error:
            raise HTTPException(
                status_code=500,
                detail=f"Error deleting message from SQS queue: {error}.")
        return response


class RDSJobQueue(JobQueue):
    """Relational Database System job queue.
    Allows other filter conditions and prioritization by not being a pure queue.
    """

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = create_engine(self.db_url, connect_args={"check_same_thread": False} if self.db_url.startswith("sqlite") else {})
        self.table_name = QueuedJob.__tablename__

    def create(self, err_on_exists: bool = True):  #TODO
        if self.engine.has_table(self.table_name) and err_on_exists:
            raise HTTPException(
                status_code=400,
                detail=f"A table with the name {self.table_name} already exists."
            )
        Base.metadata.create_all(self.engine)

    def delete(self):
        Base.metadata.drop_all(self.engine)

    def enqueue(self, env: str | None, item: dict):
        with Session(self.engine) as session:
            hw_specs = item.get('hardware') or {}
            session.add(QueuedJob(
                job=item,
                env=env,
                # None values in the resource requirements will make any puller match
                cpu_cores=hw_specs.get('cpu_cores'),
                memory=hw_specs.get('memory'),
                gpu_model=hw_specs.get('gpu_model'),
                gpu_archi=hw_specs.get('gpu_archi'),
                group=item.get('group', None),  # still to add to job model
                priority=item.get('priority', (1 if item['job_type'] == 'training' else 5)),
                pulled=False,  # avoid concurrent pulls
            ))
            session.commit()

    def peek(
        self, env: str | None, cpu_cores: int = 1, memory: int = 0, gpu_model: str | None = None, gpu_archi: str | None = None, groups: list[str] = None, older_than: float = 0
    ) -> Tuple[dict | None, str | None]:
        if groups is None:
            groups = []
        with Session(self.engine) as session:
            query = session.query(QueuedJob)
            filter_sort_query = lambda query: query.filter(
                QueuedJob.pulled == False,
                (((QueuedJob.creation_timestamp < datetime.datetime.utcnow() - datetime.timedelta(seconds=older_than)) & (QueuedJob.env == None))
                 | (QueuedJob.env == env)),  # right environment pulls its jobs immediately
                (QueuedJob.cpu_cores <= cpu_cores) | (QueuedJob.cpu_cores == None),
                (QueuedJob.memory <= memory) | (QueuedJob.memory == None),
                (QueuedJob.gpu_model == gpu_model) | (QueuedJob.gpu_model == None),
                (QueuedJob.gpu_archi == gpu_archi) | (QueuedJob.gpu_archi == None),
            ).order_by(QueuedJob.priority.desc()).order_by(QueuedJob.creation_timestamp.asc()).with_for_update().first()  # with_for_update locks concurrent pulls
            # prioritize private jobs
            job = filter_sort_query(query.filter(QueuedJob.group.in_(groups)))
            if job is None:
                job = filter_sort_query(query)
        if job:
            job.pulled = True
            return job.job, str(job.id)
        return None, None

    def pop(self, env: str, receipt_handle: str):
        with Session(self.engine) as session:
            n_del = session.query(QueuedJob).filter_by(id=receipt_handle).delete()
            if n_del != 1:
                raise HTTPException(
                    status_code=500,
                    detail=f"Error deleting job from RDS queue: {n_del} jobs found."
                )
            session.commit()
