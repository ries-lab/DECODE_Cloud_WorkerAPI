from pydantic import BaseModel


class FileHTTPRequest(BaseModel):
    method: str
    url: str
    headers: dict[str, str | dict[str, str]] = {}
    data: dict[str, str] = {}
