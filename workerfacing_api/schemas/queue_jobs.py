import datetime
import enum
from pydantic import BaseModel


class EnvironmentTypes(enum.Enum):
    cloud = "cloud"
    local = "local"
    any = None


class OutputEndpoints(enum.Enum):
    output = "output"
    log = "log"
    artifact = "artifact"


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
    image_name: str | None = None
    image_version: str | None = None
    files_down: dict[str, str] | None = None
    files_up: dict[OutputEndpoints, str] | None = None


class JobSpecs(BaseModel):
    app: AppSpecs
    handler: HandlerSpecs
    meta: MetaSpecs


class PathsUploadSpecs(BaseModel):
    output: str
    log: str


class QueueJob(BaseModel):
    job: JobSpecs
    environment: EnvironmentTypes | None = None
    hardware: HardwareSpecs
    group: str | None = None
    priority: int | None = None
    paths_upload: PathsUploadSpecs
