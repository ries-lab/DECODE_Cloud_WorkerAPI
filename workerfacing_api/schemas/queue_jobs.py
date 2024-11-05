import enum
from typing import Literal

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
    date_created: str  # iso format

    class Config:
        extra = "allow"


class AppSpecs(BaseModel):
    cmd: list[str] | None = None
    env: dict[str, str] | None = None


class HandlerSpecs(BaseModel):
    image_url: str
    image_name: str | None = None
    image_version: str | None = None
    entrypoint: str | None = None
    files_down: dict[str, str] | None = None
    files_up: dict[Literal["output", "log", "artifact"], str] | None = None


class JobSpecs(BaseModel):
    app: AppSpecs
    handler: HandlerSpecs
    meta: MetaSpecs
    hardware: HardwareSpecs


class PathsUploadSpecs(BaseModel):
    output: str
    log: str
    artifact: str


class SubmittedJob(BaseModel):
    job: JobSpecs
    environment: EnvironmentTypes = EnvironmentTypes.any
    group: str | None = None
    priority: int = 5
    paths_upload: PathsUploadSpecs


class JobFilter(BaseModel):
    # common
    environment: EnvironmentTypes
    older_than: int = 0
    # RDS queue
    cpu_cores: int = 1
    memory: int = 0
    gpu_mem: int = 0
    gpu_model: str | None = None
    gpu_archi: str | None = None
    groups: list[str] | None = None
