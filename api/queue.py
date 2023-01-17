from api.core.queue import get_queue, JobQueue
from api.config import get_settings
from api.models import EnvironmentTypes


def get_queues() -> JobQueue:
    settings = get_settings()
    queues = {env.value: get_queue(getattr(settings, f"{env.name}_queue".upper()))
        for env in EnvironmentTypes}
    return queues
