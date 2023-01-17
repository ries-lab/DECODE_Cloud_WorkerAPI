from pydantic import BaseModel


class TokenLogin(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    id_token: str
    expires_in: int
