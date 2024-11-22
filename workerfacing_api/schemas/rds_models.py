import datetime
import enum

from sqlalchemy import JSON, DateTime, Enum, Integer, String
from sqlalchemy.orm import DeclarativeBase, mapped_column


class JobStates(enum.Enum):
    queued = "queued"
    preprocessing = "preprocessing"
    pulled = "pulled"
    running = "running"
    postprocessing = "postprocessing"
    finished = "finished"
    error = "error"


class Base(DeclarativeBase):
    pass


class QueuedJob(Base):
    __tablename__ = "queued_jobs"

    # base queue attributes
    id = mapped_column(Integer, primary_key=True, index=True)
    creation_timestamp = mapped_column(
        DateTime, default=datetime.datetime.utcnow
    )  # to check job age
    last_updated = mapped_column(
        DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow
    )

    status = mapped_column(
        String, Enum(JobStates), nullable=False, default=JobStates.queued.value
    )
    num_retries = mapped_column(Integer, default=0)

    job = mapped_column(JSON, nullable=False)
    paths_upload = mapped_column(JSON, nullable=False)

    # filters (see HardwareSpecs)
    environment = mapped_column(String)
    # resource requirements (could be json column for flexibility, but separate columns optimize performance)
    cpu_cores = mapped_column(Integer, default=None)
    memory = mapped_column(Integer, default=None)
    gpu_model = mapped_column(String, default=None)
    gpu_archi = mapped_column(String, default=None)
    gpu_mem = mapped_column(Integer, default=None)

    # prioritization attributes
    group = mapped_column(String, default=None)  # worker pulls its own groups first
    priority = mapped_column(Integer, default=0)  # set by user/userfacing API

    # logging which workers tried running/run the job
    workers = mapped_column(String, default="")
