from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, status, Request

import api.crud.job as crud
import api.database as database
from api.schemas.job import Job
from api.dependencies import current_user_global_dep


router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.get("/jobs", response_model=list[Job])
def get_jobs(request: Request, offset: int = 0, limit: int = 100, db: Session = Depends(database.get_db)):
    return crud.get_jobs(db, request.state.current_user.username, offset, limit)


@router.get("/jobs/{job_id}", response_model=Job)
def get_job(request: Request, job_id: int, db: Session = Depends(database.get_db)):
    db_job = crud.get_job(db, job_id)
    if db_job is None or db_job.model.user_id != request.state.current_user.username:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return db_job
