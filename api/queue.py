from api.core.queue import get_queue
import api.settings as settings
from api.models import EnvironmentTypes


def get_queues() -> dict:
    return {env.value: get_queue(getattr(settings, f"{env.name}_queue")) for env in EnvironmentTypes}
