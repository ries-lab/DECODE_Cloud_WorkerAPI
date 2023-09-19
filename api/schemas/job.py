import datetime
from pydantic import BaseModel, validator

from api import settings
from api.models import EnvironmentTypes, JobStates, OutputEndpoints


class HardwareSpecs(BaseModel):
    cpu_cores: int | None = None
    memory: int | None = None
    gpu_model: str | None = None
    gpu_archi: str | None = None
    gpu_mem: int | None = None


class Application(BaseModel):
    application: str
    version: str
    entrypoint: str

    @validator('application')
    def application_check(cls, v, values):
        allowed = list(settings.application_config.keys())
        if v not in allowed:
            raise ValueError(f"Application must be one of {allowed}, not {v}.")
        return v

    @validator('version')
    def version_check(cls, v, values):
        # no need to check application, since validation done in order of definition
        allowed = list(settings.application_config[values["application"]].keys())
        if v not in allowed:
            raise ValueError(f"Version must be one of {allowed}, not {v}.")
        return v

    @validator('entrypoint')
    def entrypoint_check(cls, v, values):
        allowed = list(settings.application_config[values["application"]][values["version"]].keys())
        if v not in allowed:
            raise ValueError(f"Entrypoint must be one of {allowed}, not {v}.")
        return v


class InputJobAttributes(BaseModel):
    config_id: str | None = None
    data_ids: list[str] | None = None
    artifact_ids: list[str] | None = None


class JobAttributes(BaseModel):
    files_down: InputJobAttributes
    env_vars: dict[str, str] | None = None


class JobBase(BaseModel):
    job_name: str
    environment: EnvironmentTypes | None = None
    priority: int | None = None
    application: Application
    attributes: JobAttributes
    hardware: HardwareSpecs | None = None

    @validator('attributes')
    def env_check(cls, v, values):
        app = values["application"]
        application = app.application if hasattr(app, "application") else app["application"]
        version = app.version if hasattr(app, "version") else app["version"]
        entrypoint = app.entrypoint if hasattr(app, "entrypoint") else app["entrypoint"]
        allowed = settings.application_config[application][version][entrypoint]["app"]["env"]
        if not all(v_ in allowed for v_ in v.env_vars):
            raise ValueError(f"Environment variables must be in {allowed}.")
        return v


class JobReadBase(BaseModel):
    id: int
    date_created: datetime.datetime
    date_started: datetime.datetime | None
    date_finished: datetime.datetime | None
    status: JobStates


class JobCreate(JobBase):
    pass


class Job(JobBase, JobReadBase):
    user_id: int


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
    files_down: dict[str, str] | None = None  # local_path: fs_path
    files_up: dict[OutputEndpoints, str]  # endpoint: local_path


class JobSpecs(BaseModel):
    app: AppSpecs
    handler: HandlerSpecs
    meta: MetaSpecs


class PathsUploadSpecs(BaseModel):
    output: str
    log: str
    artifact: str


class QueueJob(BaseModel):
    job: JobSpecs
    environment: EnvironmentTypes | None = None
    hardware: HardwareSpecs
    group: str | None = None
    priority: int | None = None
    paths_upload: PathsUploadSpecs
