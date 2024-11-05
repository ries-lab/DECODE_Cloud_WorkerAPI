from fastapi import APIRouter, Depends, status

from workerfacing_api.core.queue import JobQueue
from workerfacing_api.dependencies import get_queue
from workerfacing_api.schemas.queue_jobs import EnvironmentTypes, SubmittedJob

router = APIRouter()


@router.post("/_jobs", status_code=status.HTTP_201_CREATED)
async def post_job(job: SubmittedJob, queue: JobQueue = Depends(get_queue)):
    if job.environment is None:
        job.environment = EnvironmentTypes.any
    queue.enqueue(item=job)
    return job
