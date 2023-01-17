import datetime
from core.filesystem import FileTypes

from pydantic import BaseModel


class FileBase(BaseModel):
    path: str


class FileUpdate(FileBase):
    pass


class File(FileBase):
    type: FileTypes
    size: str

    class Config:
        orm_mode = True
