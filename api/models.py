import enum
import datetime

from sqlalchemy import Column, ForeignKey, Integer, String, Enum, DateTime, JSON
from sqlalchemy.orm import relationship

from api.database import Base


class ModelStates(enum.Enum):
    untrained = "untrained"
    training = "training"
    trained = "trained"
    error = "error"


class DecodeVersions(enum.Enum):
    v1 = "v1"
    v2 = "v2"


class JobTypes(enum.Enum):
    train = "train"
    inference = "inference"


class JobStates(enum.Enum):
    pending = "pending"
    running = "running"
    finished = "finished"
    error = "error"


class EnvironmentTypes(enum.Enum):
    cloud = "cloud"
    local = "local"
    any = None


class Model(Base):
    __tablename__ = "models"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    status = Column(String, Enum(ModelStates), nullable=False, default=ModelStates.untrained.value)
    config_file = Column(String)
    calibration_file = Column(String)
    date_created = Column(DateTime, default=datetime.datetime.utcnow)
    date_trained = Column(DateTime)
    last_used = Column(DateTime)
    model_path = Column(String)
    decode_version = Column(String, Enum(DecodeVersions))
    user_id = Column(String, nullable=False)
    jobs = relationship("Job", back_populates="model", cascade="delete")


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_type = Column(String, Enum(JobTypes), nullable=False)
    date_created = Column(DateTime, default=datetime.datetime.utcnow)
    date_started = Column(DateTime)
    date_finished = Column(DateTime)
    status = Column(String, Enum(JobStates), nullable=False, default=JobStates.pending.value)
    model_id = Column(Integer, ForeignKey("models.id"), nullable=False)
    model = relationship("Model", back_populates="jobs")
    environment = Column(Enum(EnvironmentTypes))
    attributes = Column(JSON, nullable=False)
