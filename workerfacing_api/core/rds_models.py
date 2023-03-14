import datetime
from sqlalchemy import Column, Integer, String, DateTime, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class QueuedJob(Base):
    __tablename__ = "queued_jobs"

    # base queue attributes
    id = Column(Integer, primary_key=True, index=True)
    creation_timestamp = Column(DateTime, default=datetime.datetime.utcnow)  # to check job age
    pulled = Column(Boolean, default=False)  # pulled by worker, in process of deletion (avoid concurrent access)
    job = Column(JSON, nullable=False)

    # filters
    env = Column(String)
    # resource requirements (could be json column for flexibility, but separate columns optimize performance)
    cpu_cores = Column(Integer, default=None)
    memory = Column(Integer, default=None)
    gpu_model = Column(String, default=None)
    gpu_archi = Column(String, default=None)

    # prioritization attributes
    group = Column(String, default=None)  # worker pulls its own groups first
    priority = Column(Integer, default=0)  # set by user/userfacing API
