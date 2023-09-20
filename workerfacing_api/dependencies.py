import boto3
import typing
from fastapi import Depends, Header, HTTPException, Request
from fastapi_cloudauth.cognito import CognitoCurrentUser, CognitoClaims

from workerfacing_api import settings
from workerfacing_api.core import filesystem, queue


# Queue
queue_db_url = settings.queue_db_url
queue_ = queue.RDSJobQueue(queue_db_url)
queue_.create(err_on_exists=False)

def get_queue() -> queue.RDSJobQueue:
    return queue_


# App-internal authentication (i.e. user-facing API <-> worker-facing API)
# https://github.com/iwpnd/fastapi-key-auth/blob/main/fastapi_key_auth/dependency/authorizer.py
class APIKeyDependency:
    def __init__(self, key: str):
        self.key = key

    def __call__(self, x_api_key: typing.Optional[str] = Header(...)):
        if x_api_key != self.key:
            raise HTTPException(status_code=401, detail="unauthorized")
        return x_api_key

authorizer = APIKeyDependency(key=settings.internal_api_key_secret)


# Worker authentication
current_user_dep = CognitoCurrentUser(
    region=settings.cognito_region,
    userPoolId=settings.cognito_user_pool_id,
    client_id=settings.cognito_client_id
)

async def current_user_global_dep(request: Request, current_user: CognitoClaims = Depends(current_user_dep)):
    request.state.current_user = current_user
    return current_user


# Files
async def filesystem_dep():
    if settings.filesystem == 's3':
        s3_client = boto3.client('s3')
        s3_bucket = settings.s3_bucket
        return filesystem.S3Filesystem(s3_client, s3_bucket)
    elif settings.filesystem == 'local':
        return filesystem.LocalFilesystem(settings.user_data_root_path, settings.user_data_root_path)
    else:
        raise ValueError('Invalid filesystem setting')
