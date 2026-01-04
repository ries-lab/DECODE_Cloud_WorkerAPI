from pydantic import BaseModel, Field


class WelcomeMessage(BaseModel):
    message: str = Field(..., example="Welcome to the DECODE OpenCloud Worker-facing API")


class HTTPErrorDetail(BaseModel):
    detail: str = Field(..., example="Resource not found")


class UploadSuccess(BaseModel):
    message: str = Field(default="File uploaded successfully", example="File uploaded successfully")