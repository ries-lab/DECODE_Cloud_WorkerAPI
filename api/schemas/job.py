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


class JobAttributesBase(BaseModel):
    config_id: str | None = None
    data_ids: list[str] | None = None
    env_vars: dict[str, str] | None = None


class JobBase(BaseModel):
    model_id: int
    environment: EnvironmentTypes | None = None
    priority: int | None = None
    hardware: HardwareSpecs | None = None
    attributes: JobAttributesBase


class JobReadBase(BaseModel):
    id: int
    job_type: Literal["train", "inference"]
    date_created: datetime.datetime
    date_started: datetime.datetime | None
    date_finished: datetime.datetime | None
    status: JobStates


class TrainJobCreate(JobBase):
    pass


class TrainJob(JobBase, JobReadBase):
    job_type: Literal["train"]

    class Config:
        orm_mode = True


class InferenceJobCreate(JobBase):
    pass


class InferenceJob(JobBase, JobReadBase):
    job_type: Literal["inference"]

    class Config:
        orm_mode = True


class Job(JobBase, JobReadBase):
    pass


class MetaSpecs(BaseModel):
    job_id: int
    date_created: datetime.datetime
    class Config:
        extra = "allow"


class AppSpecs(BaseModel):
    cmd: list[str] | None = None
    env: dict[str, str] | None = None


class HandlerSpecs(BaseModel):
    image_url: str
    aws_job_def: str | None = None
    files_down: dict[str, str] | None = None
    files_up: list[str] | None = None


class JobSpecs(BaseModel):
    app: AppSpecs
    handler: HandlerSpecs
    meta: MetaSpecs


class QueueJob(BaseModel):
    job: JobSpecs
    environment: EnvironmentTypes | None = None
    hardware: HardwareSpecs
    group: str | None = None
    priority: int | None = None
    path_upload: str
