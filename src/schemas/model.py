import datetime

from pydantic import BaseModel


class ModelBase(BaseModel):
    name: str


class ModelCreate(ModelBase):
    pass


class ModelUpdate(ModelBase):
    pass


class Model(ModelBase):
    id: int
    status: str
    config_file: str | None = None
    inference_config_file: str | None
    model_file: str | None = None
    decode_version: str | None
    date_created: datetime.datetime
    last_used: datetime.datetime | None
    date_trained: datetime.datetime | None

    class Config:
        orm_mode = True
