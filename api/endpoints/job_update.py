from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, status

import api.crud.job as job_crud
import api.crud.model as model_crud
import api.database as database
from api.schemas.job_update import JobUpdate
from api.dependencies import workerfacing_api_auth_dep
from api.models import JobStates, JobTypes, ModelStates


router = APIRouter(dependencies=[Depends(workerfacing_api_auth_dep)])


@router.post("/updateJob", status_code=status.HTTP_204_NO_CONTENT)
def update_job(update: JobUpdate, db: Session = Depends(database.get_db)):
    db_job = job_crud.get_job(db, update.job_id)
    if db_job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    db_job.status = update.status
    db.add(db_job)

    if db_job.type == JobTypes.train.value:
        db_model = model_crud.get_model(db, db_job.model_id)
        if db_job.status == JobStates.finished.value:
            db_model.status = ModelStates.trained.value
        elif db_job.status == JobStates.error.value:
            db_model.status = ModelStates.error.value
        db.add(db_model)

    db.commit()
    return {}
