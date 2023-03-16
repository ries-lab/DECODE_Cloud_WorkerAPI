import datetime

from pydantic import BaseModel

from api.models import ModelStates


class ModelBase(BaseModel):
    name: str


class ModelCreate(ModelBase):
    pass


class Model(ModelBase):
    id: int
    status: ModelStates
    date_created: datetime.datetime
    last_used: datetime.datetime | None
    date_trained: datetime.datetime | None
    decode_version: str | None
    train_attributes: dict | None

    class Config:
        orm_mode = True
