from sqlalchemy.orm import Session

import models
import schemas
from core.queue import JobQueue


def create_train_job(db: Session, queues: dict[JobQueue], train_job: schemas.TrainJobCreate):
    # TODO: Update model last_used
    db_train_job = models.Job(job_type=models.JobTypes.train.value, **train_job.dict())
    if db_train_job.attributes["decode_version"] == "latest":
        db_train_job.attributes["decode_version"] = list(models.DecodeVersions)[-1].value
    env = train_job.environment.value if train_job.environment else models.EnvironmentTypes.any.value
    queues[env].enqueue(db_train_job)
    db.add(db_train_job)
    db.commit()
    db.refresh(db_train_job)
    return db_train_job


def create_inference_job(db: Session, queues: dict[JobQueue], inference_job: schemas.InferenceJobCreate):
    # TODO: Update model last_used
    db_inference_job = models.Job(job_type=models.JobTypes.inference.value, **inference_job.dict())
    env = inference_job.environment.value if inference_job.environment else models.EnvironmentTypes.any.value
    queues[env].enqueue(db_inference_job)
    db.add(db_inference_job)
    db.commit()
    db.refresh(db_inference_job)
    return db_inference_job
