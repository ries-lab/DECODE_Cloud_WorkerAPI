from datetime import datetime

from sqlalchemy.orm import Session, joinedload
from fastapi import HTTPException

import api.models as models
import api.schemas as schemas
from api.core.queue import JobQueue
from api.core.filesystem import get_user_filesystem, get_filesystem_with_root


def enqueue_job(job: models.Job, queues: dict[JobQueue]):
    env = job.environment if job.environment else models.EnvironmentTypes.any.value
    user_fs = get_user_filesystem(user_id=job.model.user_id)
    fs = get_filesystem_with_root('')
    queue_item = {
        "job_id": job.id,
        "model_id": job.model_id,
        "environment": env,
        "job_type": job.job_type,
        "date_created": job.date_created,
        "decode_version": job.model.decode_version,
        "model_path": fs.full_path_uri(job.model.model_path),
        "attributes": job.attributes if job.job_type == models.JobTypes.inference.value else {
            "config_file": user_fs.full_path_uri(job.model.config_file),
            "calibration_file": user_fs.full_path_uri(job.model.calibration_file)
        },
    }
    queues[env].enqueue(queue_item)


def get_jobs(db: Session, user_id: int, offset: int = 0, limit: int = 100):
    return db.query(models.Job).join(models.Model).filter(models.Model.user_id == user_id).offset(offset).limit(limit).all()


def get_job(db: Session, job_id: int):
    return db.query(models.Job).options(joinedload(models.Job.model)).get(job_id)


def create_train_job(db: Session, model: models.Model, queues: dict[JobQueue], train_job: schemas.TrainJobCreate):
    if model.status == models.ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already trained")
    if model.status == models.ModelStates.training.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already training")

    db_train_job = models.Job(job_type=models.JobTypes.train.value, **train_job.dict())
    db.add(db_train_job)

    model.last_used = datetime.now()
    model.status = models.ModelStates.training.value
    db.add(model)
    db.flush()

    enqueue_job(db_train_job, queues)

    db.commit()
    db.refresh(db_train_job)
    return db_train_job


def create_inference_job(db: Session, model: models.Model, queues: dict[JobQueue], inference_job: schemas.InferenceJobCreate):
    if model.status != models.ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {inference_job.model_id} has not been trained")

    db_inference_job = models.Job(job_type=models.JobTypes.inference.value, **inference_job.dict())
    # TODO: Verify attributes - e.g data exist, etc..
    db.add(db_inference_job)

    model.last_used = db_inference_job.created_at
    db.add(model)

    enqueue_job(db_inference_job, queues)

    db.commit()
    db.refresh(db_inference_job)
    return db_inference_job
