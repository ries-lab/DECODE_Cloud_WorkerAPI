import datetime
from typing import Literal

from pydantic import BaseModel

from api.models import DecodeVersions, ModelStates


class ModelBase(BaseModel):
    name: str
    config_file: str
    calibration_file: str
    decode_version: DecodeVersions | Literal["latest"] = "latest"


class ModelCreate(ModelBase):
    pass


class Model(ModelBase):
    id: int
    status: ModelStates
    date_created: datetime.datetime
    last_used: datetime.datetime | None
    date_trained: datetime.datetime | None

    class Config:
        orm_mode = True
