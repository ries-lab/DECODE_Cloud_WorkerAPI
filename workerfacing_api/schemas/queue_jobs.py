import enum
from typing import Literal

from pydantic import BaseModel, Field


class EnvironmentTypes(enum.Enum):
    cloud = "cloud"
    local = "local"
    any = None


class HardwareSpecs(BaseModel):
    cpu_cores: int | None = Field(default=None, example=2)
    memory: int | None = Field(default=None, example=4096)
    gpu_model: str | None = Field(default=None, example="RTX3080")
    gpu_archi: str | None = Field(default=None, example="ampere")
    gpu_mem: int | None = Field(default=None, example=8192)


class MetaSpecs(BaseModel):
    job_id: int = Field(..., example=12345)
    date_created: str = Field(..., example="2024-01-15T10:30:00Z")  # iso format

    class Config:
        extra = "allow"


class AppSpecs(BaseModel):
    cmd: list[str] | None = Field(default=None, example=["python", "main.py", "--config", "config.json"])
    env: dict[str, str] | None = Field(default=None, example={"CUDA_VISIBLE_DEVICES": "0", "PYTHONPATH": "/app"})


class HandlerSpecs(BaseModel):
    image_url: str = Field(..., example="ghcr.io/decode/decode-ml:latest")
    image_name: str | None = Field(default=None, example="decode-ml")
    image_version: str | None = Field(default=None, example="v1.2.0")
    entrypoint: str | None = Field(default=None, example="/app/entrypoint.sh")
    files_down: dict[str, str] | None = Field(default=None, example={"data": "/input/data"})
    files_up: dict[Literal["output", "log", "artifact"], str] | None = Field(
        default=None, 
        example={"output": "/results", "log": "/logs", "artifact": "/artifacts"}
    )


class JobSpecs(BaseModel):
    app: AppSpecs
    handler: HandlerSpecs
    meta: MetaSpecs
    hardware: HardwareSpecs


class PathsUploadSpecs(BaseModel):
    output: str = Field(..., example="s3://decode-bucket/jobs/12345/output")
    log: str = Field(..., example="s3://decode-bucket/jobs/12345/log")
    artifact: str = Field(..., example="s3://decode-bucket/jobs/12345/artifact")


class SubmittedJob(BaseModel):
    job: JobSpecs
    environment: EnvironmentTypes = Field(default=EnvironmentTypes.any, example=EnvironmentTypes.cloud)
    group: str | None = Field(default=None, example="gpu-group")
    priority: int = Field(default=5, example=7)
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
