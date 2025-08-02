from typing import Any

import boto3
from botocore.config import Config
from botocore.utils import fix_s3_host
from fastapi import Depends, Header, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials
from fastapi_cloudauth.cognito import CognitoClaims, CognitoCurrentUser  # type: ignore
from pydantic import Field

from workerfacing_api import settings
from workerfacing_api.core import filesystem, queue

# Queue
queue_db_url = settings.queue_db_url
queue_ = queue.RDSJobQueue(queue_db_url)
queue_.create(err_on_exists=False)


def queue_dep() -> queue.RDSJobQueue:
    return queue_


# App-internal authentication (i.e. user-facing API <-> worker-facing API)
# https://github.com/iwpnd/fastapi-key-auth/blob/main/fastapi_key_auth/dependency/authorizer.py
class APIKeyDependency:
    def __init__(self, key: str | None):
        self.key = key

    def __call__(self, x_api_key: str | None = Header(...)) -> str | None:
        if x_api_key != self.key:
            raise HTTPException(status_code=401, detail="unauthorized")
        return x_api_key


authorizer = APIKeyDependency(key=settings.internal_api_key_secret)


# Worker authentication
class GroupClaims(CognitoClaims):  # type: ignore
    cognito_groups: list[str] | None = Field(alias="cognito:groups")


class WorkerGroupCognitoCurrentUser(CognitoCurrentUser):  # type: ignore
    user_info = GroupClaims

    async def call(self, http_auth: HTTPAuthorizationCredentials) -> Any:
        print("WorkerGroupCognitoCurrentUser call")
        user_info = await super().call(http_auth)
        print(f"{user_info=}")
        print(f"cognito_groups={getattr(user_info, 'cognito_groups', None)}")
        if "workers" not in (getattr(user_info, "cognito_groups") or []):
            raise HTTPException(
                status_code=403, detail="Not a member of the 'workers' group"
            )
        return user_info


current_user_dep = WorkerGroupCognitoCurrentUser(
    region=settings.cognito_region,
    userPoolId=settings.cognito_user_pool_id,
    client_id=settings.cognito_client_id,
)


async def current_user_global_dep(
    request: Request, current_user: CognitoClaims = Depends(current_user_dep)
) -> CognitoClaims:
    request.state.current_user = current_user
    return current_user


# Files
async def filesystem_dep() -> filesystem.FileSystem:
    if settings.filesystem == "s3":
        s3_client = boto3.client(
            "s3",
            region_name=settings.s3_region,
            config=Config(signature_version="v4", s3={"addressing_style": "path"}),
        )
        # this and config=... required to avoid DNS problems with new buckets
        s3_client.meta.events.unregister("before-sign.s3", fix_s3_host)
        if settings.s3_bucket is None:
            raise ValueError("S3 bucket not configured")
        return filesystem.S3Filesystem(s3_client, settings.s3_bucket)
    elif settings.filesystem == "local":
        if settings.user_data_root_path is None:
            raise ValueError("Local filesystem requires user_data_root_path")
        return filesystem.LocalFilesystem(
            settings.user_data_root_path, settings.user_data_root_path
        )
    else:
        raise ValueError("Invalid filesystem setting")
