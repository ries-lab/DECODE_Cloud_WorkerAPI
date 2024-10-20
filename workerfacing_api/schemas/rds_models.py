import datetime
import enum

from sqlalchemy import JSON, Column, DateTime, Enum, Integer, String
from sqlalchemy.orm import DeclarativeBase


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
    id = Column(Integer, primary_key=True, index=True)
    creation_timestamp = Column(
        DateTime, default=datetime.datetime.utcnow
    )  # to check job age
    last_updated = Column(
        DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow
    )

    status = Column(
        String, Enum(JobStates), nullable=False, default=JobStates.queued.value
    )
    num_retries = Column(Integer, default=0)

    job = Column(JSON, nullable=False)
    paths_upload = Column(JSON, nullable=False)

    # filters (see HardwareSpecs)
    environment = Column(String)
    # resource requirements (could be json column for flexibility, but separate columns optimize performance)
    cpu_cores = Column(Integer, default=None)
    memory = Column(Integer, default=None)
    gpu_model = Column(String, default=None)
    gpu_archi = Column(String, default=None)
    gpu_mem = Column(Integer, default=None)

    # prioritization attributes
    group = Column(String, default=None)  # worker pulls its own groups first
    priority = Column(Integer, default=0)  # set by user/userfacing API

    # logging which workers tried running/run the job
    workers = Column(String, default="")
