from typing import Literal
import datetime
from pydantic import BaseModel

from api.models import EnvironmentTypes, JobStates


class HardwareSpecs(BaseModel):
    cpu_cores: int | None = None
    memory: int | None = None
    gpu_model: str | None = None
    gpu_archi: str | None = None
    gpu_mem: int | None = None


class JobBase(BaseModel):
    model_id: int
    environment: EnvironmentTypes | None = None
    priority: int = 5
    hardware: HardwareSpecs | None = None


class JobReadBase(BaseModel):
    id: int
    job_type: Literal["train", "inference"]
    date_created: datetime.datetime
    date_started: datetime.datetime | None
    date_finished: datetime.datetime | None
    status: JobStates


class TrainJobAttributesBase(BaseModel):
    class Config:
        use_enum_values = True
        extra = "allow"


class TrainJobBase(JobBase):
    attributes: TrainJobAttributesBase


class TrainJobCreate(TrainJobBase):
    pass


class TrainJob(TrainJobBase, JobReadBase):
    job_type: Literal["train"]

    class Config:
        orm_mode = True


class InferenceJobAttributes(BaseModel):

    class Config:
        extra = "allow"


class InferenceJobBase(JobBase):
    attributes: InferenceJobAttributes


class InferenceJobCreate(InferenceJobBase):
    pass


class InferenceJob(InferenceJobBase, JobReadBase):
    job_type: Literal["inference"]

    class Config:
        orm_mode = True


class Job(JobBase, JobReadBase):
    attributes: TrainJobAttributesBase | InferenceJobAttributes

    class Config:
        orm_mode = True
