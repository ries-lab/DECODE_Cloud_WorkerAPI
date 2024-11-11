from fastapi import APIRouter, Depends, status

from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api.dependencies import get_queue
from workerfacing_api.schemas.queue_jobs import SubmittedJob

router = APIRouter()


@router.post(
    "/_jobs",
    status_code=status.HTTP_201_CREATED,
    response_model=SubmittedJob,
    description="Submit a job to the queue (private internal endpoint).",
)
async def post_job(
    job: SubmittedJob, queue: RDSJobQueue = Depends(get_queue)
) -> SubmittedJob:
    queue.enqueue(job)
    return job
