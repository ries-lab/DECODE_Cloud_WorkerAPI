from fastapi import APIRouter, Depends, Query, status
from fastapi.encoders import jsonable_encoder
from workerfacing_api.core.queue import JobQueue
from workerfacing_api.queue import get_queue
from workerfacing_api.schemas.rds_models import JobStates
from workerfacing_api.schemas.queue_jobs import QueueJob, JobSpecs


router = APIRouter()


@router.get("/jobs", response_model=dict[int, JobSpecs])
async def get_jobs(
    hostname: str,
    cpu_cores: int,
    memory: int,
    environment: str | None = None,
    gpu_model: str | None = None,
    gpu_archi: str | None = None,
    gpu_mem: int | None = None,
    groups: list[str] | None = Query(None),
    limit: int = 1,
    older_than: int | None = None,
    queue: JobQueue = Depends(get_queue),
):
    jobs = {}
    for _ in range(limit):
        job = queue.dequeue(
            hostname=hostname,
            cpu_cores=cpu_cores,
            memory=memory,
            environment=environment,
            gpu_model=gpu_model,
            gpu_archi=gpu_archi,
            gpu_mem=gpu_mem or 0,
            groups=groups,
            older_than=older_than or 0,
        )
        if job:
            job_id = job.pop("job_id")
            jobs.update({job_id: JobSpecs(**job)})
            queue.update_job_status(job_id, status=JobStates.running)
        else:
            break
    return jobs


@router.post("/jobs")
async def post_job(job: QueueJob, queue: JobQueue = Depends(get_queue)):
    queue.enqueue(environment=job.environment.value, item=jsonable_encoder(job))
    return job


@router.get("/jobs/{job_id}/status")
async def get_job_status(job_id: int, queue: JobQueue = Depends(get_queue)):
    return queue.get_job(job_id).status


@router.put("/jobs/{job_id}/status", status_code=status.HTTP_200_OK)
async def put_job_status(job_id: int, status: JobStates, queue: JobQueue = Depends(get_queue)):
    return queue.update_job_status(job_id, status)
