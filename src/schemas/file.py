import datetime
from core.filesystem import FileTypes

from pydantic import BaseModel


class FileBase(BaseModel):
    name: str


class FileUpdate(FileBase):
    pass


class File(FileBase):
    type: FileTypes
    parent: str
    size: str
    date_created: datetime.datetime

    class Config:
        orm_mode = True
