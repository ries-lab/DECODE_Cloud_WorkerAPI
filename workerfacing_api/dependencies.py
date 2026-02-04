import boto3
from botocore.config import Config
from botocore.utils import fix_s3_host
from fastapi import Depends, Request
from fastapi_cloudauth.cognito import CognitoClaims  # type: ignore

from workerfacing_api import settings
from workerfacing_api.core import auth, filesystem, queue

# S3 client setup
s3_client = None
if settings.s3_bucket:
    s3_client = boto3.client(
        "s3",
        region_name=settings.s3_region,
        config=Config(signature_version="v4", s3={"addressing_style": "path"}),
    )
    # this and config=... required to avoid DNS problems with new buckets
    s3_client.meta.events.unregister("before-sign.s3", fix_s3_host)

# Queue
queue_db_url = settings.queue_db_url
retry_different = settings.retry_different
if queue_db_url.startswith("sqlite"):
    queue_: queue.RDSJobQueue = queue.SQLiteRDSJobQueue(
        db_url=queue_db_url,
        retry_different=retry_different,
        s3_client=s3_client,
        s3_bucket=settings.s3_bucket,
    )
else:
    queue_ = queue.RDSJobQueue(db_url=queue_db_url, retry_different=retry_different)


def queue_dep() -> queue.RDSJobQueue:
    return queue_


# App-internal authentication (i.e. user-facing API <-> worker-facing API)
authorizer = auth.APIKeyDependency(key=settings.internal_api_key_secret)


# Worker authentication
current_user_dep = auth.WorkerGroupCognitoCurrentUser(
    region=settings.cognito_region,
    userPoolId=settings.cognito_user_pool_id,
    client_id=settings.cognito_client_id,
)


async def current_user_global_dep(
    request: Request, current_user: CognitoClaims = Depends(current_user_dep)
) -> CognitoClaims:
    request.state.current_user = current_user
    return current_user


# Filesystem
async def filesystem_dep() -> filesystem.FileSystem:
    if settings.filesystem == "s3":
        if s3_client is None or settings.s3_bucket is None:
            raise ValueError("S3 bucket or client not configured")
        return filesystem.S3Filesystem(s3_client, settings.s3_bucket)
    elif settings.filesystem == "local":
        if settings.user_data_root_path is None:
            raise ValueError("Local filesystem requires user_data_root_path")
        return filesystem.LocalFilesystem(
            settings.user_data_root_path, settings.user_data_root_path
        )
    else:
        raise ValueError("Invalid filesystem setting")
