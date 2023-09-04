from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api import settings


queue_db_url = settings.queue_db_url

queue = RDSJobQueue(queue_db_url)
queue.create(err_on_exists=False)


def get_queue() -> RDSJobQueue:
    return queue
