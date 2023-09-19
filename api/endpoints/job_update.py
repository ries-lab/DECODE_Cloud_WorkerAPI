from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, status

import api.crud.job as job_crud
import api.database as database
from api.schemas.job_update import JobUpdate
from api.dependencies import workerfacing_api_auth_dep
from api.models import JobStates


router = APIRouter(dependencies=[Depends(workerfacing_api_auth_dep)])


@router.put("/_job_status", status_code=204)
def update_job(update: JobUpdate, db: Session = Depends(database.get_db)):
    db_job = job_crud.get_job(db, update.job_id)
    if db_job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    db_job.status = update.status.value
    db.add(db_job)
    #TODO: user notifications
    db.commit()
