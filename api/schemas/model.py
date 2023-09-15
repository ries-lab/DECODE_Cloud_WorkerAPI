import datetime
from pydantic import BaseModel, validator

from api.models import ModelStates
from api import settings


class ModelBase(BaseModel):
    name: str
    decode_version: str

    @validator('decode_version')
    def version_check(cls, v, values):
        allowed = list(settings.version_config.keys())
        if v not in allowed:
            raise ValueError(f"Version must be one of {allowed}, not {v}.")
        return v

    class Config:
        orm_mode = True


class ModelCreate(ModelBase):
    pass


class Model(ModelBase):
    id: int
    status: ModelStates
    date_created: datetime.datetime
    last_used: datetime.datetime | None
    date_trained: datetime.datetime | None
    train_attributes: dict | None

    class Config:
        orm_mode = True
