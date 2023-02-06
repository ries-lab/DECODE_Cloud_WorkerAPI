from typing import Literal
import datetime
from pydantic import BaseModel

from api.models import EnvironmentTypes, JobStates


class JobBase(BaseModel):
    model_id: int
    environment: EnvironmentTypes | None = None


class JobReadBase(BaseModel):
    id: int
    job_type: Literal["train", "inference"]
    date_created: datetime.datetime
    date_started: datetime.datetime | None
    date_finished: datetime.datetime | None
    status: JobStates


class TrainJobAttributes(BaseModel):
    pass


class TrainJobBase(JobBase):
    attributes: TrainJobAttributes = {}


class TrainJobCreate(TrainJobBase):
    pass


class TrainJob(TrainJobBase, JobReadBase):
    job_type: Literal["train"]

    class Config:
        orm_mode = True


class InferenceJobAttributes(BaseModel):
    frames_file: str
    frame_meta_file: str


class InferenceJobBase(JobBase):
    attributes: InferenceJobAttributes


class InferenceJobCreate(InferenceJobBase):
    pass


class InferenceJob(InferenceJobBase, JobReadBase):
    job_type: Literal["inference"]

    class Config:
        orm_mode = True


class Job(JobBase, JobReadBase):
    attributes: TrainJobAttributes | InferenceJobAttributes

    class Config:
        orm_mode = True
