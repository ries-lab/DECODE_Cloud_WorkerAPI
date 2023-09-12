from fastapi import APIRouter, Body, Depends, Query, status
from workerfacing_api.core.rds_models import JobStates
from workerfacing_api.core.queue import JobQueue
from workerfacing_api.queue import get_queue
from workerfacing_api.core.job_tracking import update_job


router = APIRouter()


@router.get("/jobs", response_model=list[dict])
async def get_jobs(
    cpu_cores: int,
    memory: int,
    env: str | None = None,
    gpu_model: str | None = None,
    gpu_archi: str | None = None,
    gpu_mem: int | None = None,
    groups: list[str] | None = Query(None),
    limit: int = 1,
    older_than: int | None = None,
    queue: JobQueue = Depends(get_queue),
):
    jobs = []
    for _ in range(limit):
        job = queue.dequeue(
            cpu_cores=cpu_cores,
            memory=memory,
            env=env,
            gpu_model=gpu_model,
            gpu_archi=gpu_archi,
            gpu_mem=gpu_mem or 0,
            groups=groups,
            older_than=older_than,
        )
        if job:
            jobs.append(job)
            update_job(job_id=job["id"], job_status=JobStates.running)
        else:
            break
    return jobs


@router.post("/jobs")
async def post_job(job: dict = Body(), queue: JobQueue = Depends(get_queue)):
    queue.enqueue(env=job.get('environment'), item=job)
    return job


@router.get("/jobs/{job_id}/status")
async def get_job_status(job_id: int, queue: JobQueue = Depends(get_queue)):
    return queue.get_job(job_id).status


@router.put("/jobs/{job_id}/status", status_code=status.HTTP_200_OK)
async def put_job_status(job_id: int, status: JobStates, queue: JobQueue = Depends(get_queue)):
    return queue.update_job_status(job_id, status)
