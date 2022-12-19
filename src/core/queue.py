import boto3
import datetime
import json
import os
import pickle
from abc import ABC, abstractmethod
from fastapi.encoders import jsonable_encoder
from typing import Tuple

from ..models import Job


class JobQueue(ABC):
    def __init__(self, path: str):
        self.queue_path = path

    @abstractmethod
    def enqueue(self, job: Job):
        pass

    @abstractmethod
    def peek(self) -> Tuple[dict, str | None] | None:
        pass

    @abstractmethod
    def pop(self, receipt_handle: str | None) -> None:
        pass

    def dequeue(self, older_than: float=0) -> Job | None:
        # get last element
        res = self.peek()
        # if element found
        if res:
            job_json, receipt_handle = res
            # get job
            job = Job(**job_json)
            # check "old enough"
            time_now = datetime.datetime.utcnow()
            if time_now - datetime.datetime.strptime(job.date_created) > older_than:
                # remove from queue and return
                self.pop(receipt_handle)
                return job
        return None


class LocalJobQueue(JobQueue):
    """Local job queue, for testing purposes only.
    """
    def __init__(self, queue_path: str):
        self.queue_path = queue_path
        # Create queue if it does not exist
        if not os.path.exists(queue_path):
            queue = list()
            with open(queue_path, 'wb') as f:
                pickle.dump(queue, f)
    
    def enqueue(self, job: Job):
        with open(self.queue_path, 'rb+') as f:
            # Read in
            queue = pickle.load(f)
            # Push new job
            queue.append(jsonable_encoder(job))
            # Overwrite
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)
    
    def peek(self) -> Tuple[dict, str | None]:
        with open(self.queue_path, 'rb+') as f:
            queue = pickle.load(f)
        if len(queue):
            job_json = queue[0]
            return job_json, None
        return None

    def pop(self, receipt_handle=None):
        with open(self.queue_path, 'rb+') as f:
            queue = pickle.load(f)
            queue = queue[1:]
            f.seek(0)
            f.truncate()
            pickle.dump(queue, f)


#TODO: queue will need to have a high enough visibility timeout
#TODO: AWS lambda will need to pull infrequently enough that local
# has the time to pull
class SQSJobQueue(JobQueue):

    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.sqs_client = boto3.client("sqs")

    def enqueue(self, job: Job):
        self.sqs_client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(jsonable_encoder(job))
        )

    def peek(self) -> Tuple[dict, str | None]:
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )
        if len(response.get('Messages', [])):
            message = response['Messages'][0]
            message_body = message['Body']
            job_json = message_body
            receipt_handle = message['ReceiptHandle']
            return job_json, receipt_handle
        return None

    def pop(self, receipt_handle: str | None):
        response = self.sqs_client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )
        return response
