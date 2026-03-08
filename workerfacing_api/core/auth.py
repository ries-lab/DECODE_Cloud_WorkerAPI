from typing import Any

from fastapi import Header, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from fastapi_cloudauth.cognito import CognitoClaims, CognitoCurrentUser  # type: ignore
from pydantic import Field


# https://github.com/iwpnd/fastapi-key-auth/blob/main/fastapi_key_auth/dependency/authorizer.py
class APIKeyDependency:
    def __init__(self, key: str | None):
        self.key = key

    def __call__(self, x_api_key: str | None = Header(...)) -> str | None:
        if x_api_key != self.key:
            raise HTTPException(status_code=401, detail="unauthorized")
        return x_api_key


class GroupClaims(CognitoClaims):  # type: ignore
    cognito_groups: list[str] | None = Field(alias="cognito:groups")


class WorkerGroupCognitoCurrentUser(CognitoCurrentUser):  # type: ignore
    user_info = GroupClaims

    async def call(self, http_auth: HTTPAuthorizationCredentials) -> Any:
        user_info = await super().call(http_auth)
        if "workers" not in (getattr(user_info, "cognito_groups") or []):
            raise HTTPException(
                status_code=403, detail="Not a member of the 'workers' group"
            )
        return user_info
