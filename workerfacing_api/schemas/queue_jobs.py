import datetime
import enum
from pydantic import BaseModel


class EnvironmentTypes(enum.Enum):
    cloud = "cloud"
    local = "local"
    any = None


class HardwareSpecs(BaseModel):
    cpu_cores: int | None = None
    memory: int | None = None
    gpu_model: str | None = None
    gpu_archi: str | None = None
    gpu_mem: int | None = None


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
