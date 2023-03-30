from datetime import datetime

from sqlalchemy.orm import Session, joinedload
from fastapi import HTTPException

import api.models as models
import api.schemas as schemas
from api.core.filesystem import get_user_filesystem, get_filesystem_with_root
import api.settings as settings


def enqueue_job(job: models.Job, enqueueing_func: callable):
    env = job.environment if job.environment else models.EnvironmentTypes.any
    user_fs = get_user_filesystem(user_id=job.model.user_id)
    fs = get_filesystem_with_root('')
    decode_version = job.model.decode_version
    queue_item = {
        "id": job.id,
        "model_id": job.model_id,
        "environment": env.value,
        "job_type": job.job_type,
        "date_created": job.date_created,
        "decode_version": decode_version,
        "model_path": fs.full_path_uri(job.model.model_path),
        "attributes": {user_fs.full_path_uri(file) for name, file in job.attributes.items() if name != "decode_version"},
        "aws_batch": settings.version_config[decode_version]["entrypoints"]["train"]["aws_batch"],
        "hardware": job.hardware
    }
    enqueueing_func(queue_item)


def get_jobs(db: Session, user_id: int, offset: int = 0, limit: int = 100):
    return db.query(models.Job).join(models.Model).filter(models.Model.user_id == user_id).offset(offset).limit(limit).all()


def get_job(db: Session, job_id: int):
    return db.query(models.Job).options(joinedload(models.Job.model)).get(job_id)


def _validate_files(filesystem, paths: list[str]):
    for _file in paths:
        if not filesystem.exists(_file):
            raise HTTPException(status_code=400, detail=f"File {_file} does not exist")


def create_train_job(db: Session, model: models.Model, enqueueing_func: callable, train_job: schemas.TrainJobCreate):
    if model.status == models.ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already trained")
    if model.status == models.ModelStates.training.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already training")
    if train_job.priority < 1 or train_job.priority > 5:
        raise HTTPException(status_code=400, detail="Priority must be between 1 and 5")

    train_attributes = train_job.attributes.dict()
    model.decode_version = train_attributes["decode_version"]
    del train_attributes["decode_version"]

    filesystem = get_user_filesystem(model.user_id)
    paths = train_attributes.values()
    _validate_files(filesystem, paths)

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
    paths = inference_job.attributes.dict().values()
    _validate_files(filesystem, paths)

    db_inference_job = models.Job(job_type=models.JobTypes.inference.value, **inference_job.dict())
    db.add(db_inference_job)

    model.last_used = db_inference_job.date_created
    db.add(model)
    db.flush()

    enqueue_job(db_inference_job, enqueueing_func)

    db.commit()
    db.refresh(db_inference_job)
    return db_inference_job
