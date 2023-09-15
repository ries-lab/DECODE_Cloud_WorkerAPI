import enum
import datetime

from sqlalchemy import Column, ForeignKey, Integer, String, Enum, DateTime, JSON, UniqueConstraint
from sqlalchemy.orm import relationship

from api.database import Base


class ModelStates(enum.Enum):
    untrained = "untrained"
    training = "training"
    trained = "trained"
    error = "error"


class JobTypes(enum.Enum):
    train = "train"
    inference = "inference"


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


class Model(Base):
    __tablename__ = "models"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    status = Column(String, Enum(ModelStates), nullable=False, default=ModelStates.untrained.value)
    date_created = Column(DateTime, default=datetime.datetime.utcnow)
    date_trained = Column(DateTime)
    last_used = Column(DateTime)
    model_path = Column(String)
    decode_version = Column(String, nullable=True)
    train_attributes = Column(JSON, nullable=True)
    user_id = Column(String, nullable=False)
    jobs = relationship("Job", back_populates="model", cascade="delete")
    __table_args__ = (UniqueConstraint("name", "user_id", name="_user_model_name_unique"),)


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_type = Column(String, Enum(JobTypes), nullable=False)
    date_created = Column(DateTime, default=datetime.datetime.utcnow)
    date_started = Column(DateTime)
    date_finished = Column(DateTime)
    status = Column(String, Enum(JobStates), nullable=False, default=JobStates.queued.value)
    model_id = Column(Integer, ForeignKey("models.id"), nullable=False)
    model = relationship("Model", back_populates="jobs")
    priority = Column(Integer, nullable=False, default=5)
    environment = Column(Enum(EnvironmentTypes))
    attributes = Column(JSON, nullable=False)
    hardware = Column(JSON, nullable=True)
