from pydantic import BaseModel, Field


class FileHTTPRequest(BaseModel):
    method: str = Field(..., example="POST")
    url: str = Field(..., example="https://example.com/upload")
    headers: dict[str, str | dict[str, str]] = Field(
        default={}, 
        example={"Content-Type": "multipart/form-data"}
    )
    data: dict[str, str] = Field(
        default={}, 
        example={"key": "test-upload-key"}
    )
