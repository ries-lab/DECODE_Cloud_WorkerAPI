import json
import requests

from datetime import datetime

from sqlalchemy.orm import Session, joinedload
from fastapi import HTTPException

import api.models as models
import api.schemas as schemas
from api.core.filesystem import get_user_filesystem, get_filesystem_with_root


def enqueue_job(job: models.Job, enqueueing_func: callable):
    env = job.environment if job.environment else models.EnvironmentTypes.any
    user_fs = get_user_filesystem(user_id=job.model.user_id)
    fs = get_filesystem_with_root('')
    queue_item = {
        "job_id": job.id,
        "model_id": job.model_id,
        "environment": env.value,
        "job_type": job.job_type,
        "date_created": job.date_created,
        "decode_version": job.model.decode_version,
        "model_path": fs.full_path_uri(job.model.model_path),
        "attributes": {user_fs.full_path_uri(file) for name, file in job.attributes.items() if name != "decode_version"}
    }
    enqueueing_func(queue_item)


def get_jobs(db: Session, user_id: int, offset: int = 0, limit: int = 100):
    return db.query(models.Job).join(models.Model).filter(models.Model.user_id == user_id).offset(offset).limit(limit).all()


def get_job(db: Session, job_id: int):
    return db.query(models.Job).options(joinedload(models.Job.model)).get(job_id)


def create_train_job(db: Session, model: models.Model, enqueueing_func: callable, train_job: schemas.TrainJobCreate):
    if model.status == models.ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already trained")
    if model.status == models.ModelStates.training.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already training")

    train_attributes = train_job.attributes.dict()
    model.decode_version = train_attributes["decode_version"]
    del train_attributes["decode_version"]

    filesystem = get_user_filesystem(model.user_id)
    for name, train_file in train_attributes.items():
        if not filesystem.exists(train_file):
            raise HTTPException(status_code=400, detail=f"File {train_file} does not exist")

    db_train_job = models.Job(job_type=models.JobTypes.train.value, **train_job.dict())
    db.add(db_train_job)

    model.last_used = datetime.now()
    model.status = models.ModelStates.training.value
    model.train_attributes = train_attributes
    db.add(model)
    db.flush()

    enqueue_job(db_train_job, enqueueing_func)

    db.commit()
    db.refresh(db_train_job)
    return db_train_job


def create_inference_job(db: Session, model: models.Model, enqueueing_func: callable, inference_job: schemas.InferenceJobCreate):
    if model.status != models.ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {inference_job.model_id} has not been trained")

    filesystem = get_user_filesystem(model.user_id)
    for name, fit_file in inference_job.attributes.dict().items():
        if not filesystem.exists(fit_file):
            raise HTTPException(status_code=400, detail=f"File {fit_file} does not exist")

    db_inference_job = models.Job(job_type=models.JobTypes.inference.value, **inference_job.dict())
    db.add(db_inference_job)

    model.last_used = db_inference_job.date_created
    db.add(model)
    db.flush()

    enqueue_job(db_inference_job, enqueueing_func)

    db.commit()
    db.refresh(db_inference_job)
    return db_inference_job
