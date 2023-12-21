from pydantic import BaseModel


class FileHTTPRequest(BaseModel):
    method: str
    url: str
    headers: dict = {}  # thank you pydantic, for handling mutable defaults
    data: dict = {}
