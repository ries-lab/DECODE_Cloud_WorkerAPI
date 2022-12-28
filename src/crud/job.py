from sqlalchemy.orm import Session

from .. import models
from .. import schemas
from ..core import JobQueue


def create_train_job(db: Session, queue: JobQueue, train_job: schemas.TrainJobCreate):
    db_train_job = models.Job(job_type=models.JobTypes.train.value, **train_job.dict())
    if db_train_job.attributes["decode_version"] == "latest":
        db_train_job.attributes["decode_version"] = list(models.DecodeVersions)[-1].value
    queue.enqueue(db_train_job)
    db.add(db_train_job)
    db.commit()
    db.refresh(db_train_job)
    return db_train_job


def create_inference_job(db: Session, queue: JobQueue, inference_job: schemas.InferenceJobCreate):
    db_inference_job = models.Job(job_type=models.JobTypes.inference.value, **inference_job.dict())
    queue.enqueue(db_inference_job)
    db.add(db_inference_job)
    db.commit()
    db.refresh(db_inference_job)
    return db_inference_job
