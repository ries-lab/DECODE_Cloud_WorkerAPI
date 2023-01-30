from sqlalchemy.orm import Session
from fastapi import HTTPException

import api.models as models
import api.schemas as schemas
from api.core.queue import JobQueue


def enqueue_job(job: models.Job, queues: dict[JobQueue]):
    env = job.attributes["environment"] if "environment" in job.attributes else models.EnvironmentTypes.any.value
    queues[env].enqueue(job)


def create_train_job(db: Session, model: models.Model, queues: dict[JobQueue], train_job: schemas.TrainJobCreate):
    if model.status == models.ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already trained")
    if model.status == models.ModelStates.training.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already training")

    db_train_job = models.Job(job_type=models.JobTypes.train.value, **train_job.dict())
    if db_train_job.attributes["decode_version"] == "latest":
        db_train_job.attributes["decode_version"] = list(models.DecodeVersions)[-1].value
    db.add(db_train_job)

    model.last_used = db_train_job.created_at
    model.status = models.ModelStates.training.value
    db.add(model)

    enqueue_job(db_train_job, queues)

    db.commit()
    db.refresh(db_train_job)
    return db_train_job


def create_inference_job(db: Session, model: models.Model, queues: dict[JobQueue], inference_job: schemas.InferenceJobCreate):
    if model.status != models.ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {inference_job.model_id} has not been trained")

    db_inference_job = models.Job(job_type=models.JobTypes.inference.value, **inference_job.dict())
    db.add(db_inference_job)

    model.last_used = db_inference_job.created_at
    db.add(model)

    enqueue_job(db_inference_job, queues)

    db.commit()
    db.refresh(db_inference_job)
    return db_inference_job
