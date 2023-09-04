from fastapi import APIRouter, Body, Depends, Query, status
from workerfacing_api.core.queue import JobQueue
from workerfacing_api.queue import get_queue
from workerfacing_api.core.job_tracking import JobStates, update_job


router = APIRouter()


@router.get("/jobs", response_model=list[dict])
def get_jobs(
    limit: int = 1,
    env: str | None = None,
    cpu_cores: int = 1,
    memory: int = 0,
    gpu_model: str | None = None,
    gpu_archi: str | None = None,
    groups: list[str] | None = Query(None),
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
            update_job(job_id=job["id"], job_status=JobStates.running)
        else:
            break
    return jobs


@router.post("/jobs")
def post_job(job: dict = Body(), queue: JobQueue = Depends(get_queue)):
    queue.enqueue(env=job.get('environment'), item=job)
    return job


@router.post("/outputs/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def post_job_finish(job_id: int):
    update_job(job_id=job_id, job_status=JobStates.finished)
    return {}


@router.post("/errors/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def post_job_error(job_id: int):
    update_job(job_id=job_id, job_status=JobStates.error)
    return {}
