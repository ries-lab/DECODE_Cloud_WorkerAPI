import enum
import datetime
from sqlalchemy import Column, Integer, String, Enum, DateTime, JSON, UniqueConstraint

from api.database import Base


class JobStates(enum.Enum):
    queued = "queued"
    pulled = "pulled"
    running = "running"
    finished = "finished"
    error = "error"


class EnvironmentTypes(enum.Enum):
    cloud = "cloud"
    local = "local"
    any = None


class OutputEndpoints(enum.Enum):
    output = "output"
    log = "log"
    artifact = "artifact"


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    job_name = Column(String)
    date_created = Column(DateTime, default=datetime.datetime.utcnow)
    date_started = Column(DateTime)
    date_finished = Column(DateTime)
    status = Column(String, Enum(JobStates), nullable=False, default=JobStates.queued.value)
    environment = Column(Enum(EnvironmentTypes))
    priority = Column(Integer, nullable=False, default=0)
    application = Column(JSON, nullable=False)
    attributes = Column(JSON, nullable=False)
    hardware = Column(JSON, nullable=True)

    __table_args__ = (UniqueConstraint("user_id", "job_name", name="_user_job_name_unique"),)
