import datetime
import enum
from pydantic import BaseModel


#TODO: this is basically a copy-paste from user-facing API
# not imported to leave the two independently usable
# but beware of the tight coupling

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


class JobSpecs(BaseModel):
    date_created: datetime.datetime
    image_url: str
    command: str | list[str] | None
    job_env: dict[str, str] | None
    files: dict[str, str]


class QueueJob(BaseModel):
    job_id: str
    job: JobSpecs
    environment: EnvironmentTypes | None = None
    hardware: HardwareSpecs
    group: str | None = None
    priority: int | None = None
    path_upload: str

    class Config:
        orm_mode = True


class JobSpecsQueue(JobSpecs):
    queue_id: str  # added to user-facing API model, to trace back to DB
