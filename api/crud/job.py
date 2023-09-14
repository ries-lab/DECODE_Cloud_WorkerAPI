import os
from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session, joinedload

import api.models as models
import api.schemas as schemas
from api.core.filesystem import get_user_filesystem, get_filesystem_with_root
import api.settings as settings


def enqueue_job(job: models.Job, enqueueing_func: callable):
    user_fs = get_user_filesystem(user_id=job.model.user_id)
    fs = get_filesystem_with_root('')

    version_config = settings.version_config[job.model.decode_version]['entrypoints'][job.job_type]

    def prepare_files(root_in, root_out):
        root_in_dir = root_in + ("/" if not root_in[-1] == "/" else "")
        out_files = {}
        if not user_fs.isdir(root_in_dir):
            in_files = [root_in]
        else:
            in_files = [f.path for f in user_fs.list_directory(root_in_dir, dirs=False, recursive=True)]
        for in_f in in_files:
            out_files[f"{root_out}/{os.path.relpath(in_f, root_in)}"] = user_fs.full_path_uri(in_f)
        return out_files

    config_path = f"config/{job.attributes['config_id']}"
    data_paths = [f"data/{data_id}" for data_id in job.attributes['data_ids']]
    _validate_files(user_fs, data_paths + [config_path])
    files = prepare_files(config_path, "/data/config")
    for data_path in data_paths:
        files.update(prepare_files(data_path, "/data/data"))

    job_specs = schemas.JobSpecs(
        date_created=job.date_created,
        image_url=version_config["image_url"],
        command=version_config["command"],
        job_env={},
        files=files,
    )
    queue_item = schemas.QueueJob(
        job_id=job.id,
        job=job_specs,
        env=job.environment if job.environment else models.EnvironmentTypes.any,
        hardware=job.hardware,
        group=None,  #TODO
        priority=job.priority or (1 if job.job_type == models.JobTypes.train else 5),
        path_upload=fs.full_path_uri(job.model.model_path),
    )
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

    db_inference_job = models.Job(job_type=models.JobTypes.inference.value, **inference_job.dict())
    db.add(db_inference_job)

    model.last_used = db_inference_job.date_created
    db.add(model)
    db.flush()

    enqueue_job(db_inference_job, enqueueing_func)

    db.commit()
    db.refresh(db_inference_job)
    return db_inference_job
