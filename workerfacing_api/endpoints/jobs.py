from fastapi import APIRouter, Body, Depends, HTTPException, status
from workerfacing_api.core.queue import JobQueue
from workerfacing_api.queue import get_queue


router = APIRouter()


@router.get("/jobs", response_model=list[dict])
def get_jobs(
    limit: int = 1,
    env: str | None = None,
    cpu_cores: int = 1,
    memory: int = 0,
    gpu_model: str | None = None,
    gpu_archi: str | None = None,
    groups: list[str] | None = None,
    older_than: int = 0,
    queue: JobQueue = Depends(get_queue),
):
    jobs = []
    for _ in range(limit):
        job = queue.dequeue(
            env=env,
            cpu_cores=cpu_cores,
            memory=memory,
            gpu_model=gpu_model,
            gpu_archi=gpu_archi,
            groups=groups or [],
            older_than=older_than,
        )
        if job:
            jobs.append(job)
        else:
            break
    return jobs


@router.post("/jobs")
def post_job(job: dict = Body(), queue: JobQueue = Depends(get_queue)):
    queue.enqueue(env=job.get('environment'), item=job)
    return job


@router.post("/outputs/{job_id}")
def post_output(job_id: int):
    #TODO: handles job postprocessing, API update
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED)
