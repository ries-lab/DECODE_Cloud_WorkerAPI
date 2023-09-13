import datetime
from pydantic import BaseModel

from api.models import ModelStates, Versions


class ModelBase(BaseModel):
    name: str
    decode_version: Versions = [e.value for e in Versions][-1]

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
