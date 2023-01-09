from .core.queue import get_queue_from_path, JobQueue
from .config import get_settings


def get_queue() -> JobQueue:
    return get_queue_from_path(get_settings().QUEUE_PATH)
