from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from typing import Any

import api.database as database
from api.crud import job as crud_job, model as crud_model
from api.dependencies import current_user_global_dep
from api.models import ModelStates, JobTypes
from api.queue import get_enqueueing_function
from api.schemas.job import Job, JobCreate


router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.get("/jobs", response_model=list[Job])
def get_jobs(request: Request, offset: int = 0, limit: int = 100, db: Session = Depends(database.get_db)):
    return crud_job.get_jobs(db, request.state.current_user.username, offset, limit)


@router.get("/jobs/{job_id}", response_model=Job)
def get_job(request: Request, job_id: int, db: Session = Depends(database.get_db)):
    db_job = crud_job.get_job(db, job_id)
    if db_job is None or db_job.model.user_id != request.state.current_user.username:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return db_job


@router.post("/jobs", response_description="Start job", response_model=Job)
def start_job(
    request: Request,
    job: JobCreate,
    db: Any = Depends(database.get_db),
    enqueueing_func: str = Depends(get_enqueueing_function),
):
    model = crud_model.get_model(db, job.model_id)
    if not model or model.user_id != request.state.current_user.username:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Model {job.model_id} not found",
        )

    if job.job_type == JobTypes.train:
        ret_job = crud_job.create_train_job(db, model, enqueueing_func, job)
        model.status = ModelStates.training.value
        db.add(model)
        db.commit()
    elif job.job_type == JobTypes.inference:
        ret_job = crud_job.create_inference_job(db, model, enqueueing_func, job)
    else:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Only jobs of type 'train' or 'inference' are supported.",
        )
    return ret_job
