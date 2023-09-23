from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder

from workerfacing_api.core.queue import JobQueue
from workerfacing_api.dependencies import get_queue
from workerfacing_api.schemas.queue_jobs import EnvironmentTypes, QueueJob


router = APIRouter()


@router.post("/_jobs")
async def post_job(job: QueueJob, queue: JobQueue = Depends(get_queue)):
    if job.environment is None:
        job.environment = EnvironmentTypes.any
    queue.enqueue(environment=job.environment.value, item=jsonable_encoder(job))
    return job