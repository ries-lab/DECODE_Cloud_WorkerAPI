import json
import os
from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api import settings


queue_db_url = settings.queue_db_url

# AWS password
if "QUEUE_DB_SECRET" in os.environ:
    queue_db_url = queue_db_url.format(json.loads(os.environ["QUEUE_DB_SECRET"])["password"])

queue = RDSJobQueue(queue_db_url)
queue.create(err_on_exists=False)


def get_queue() -> RDSJobQueue:
    return queue
