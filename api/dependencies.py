from fastapi import Request, Depends
from fastapi_cloudauth.cognito import CognitoCurrentUser, CognitoClaims

from api.core.filesystem import get_user_filesystem
from api.settings import cognito_client_id, cognito_region, cognito_user_pool_id

current_user_dep = CognitoCurrentUser(
    region=cognito_region,
    userPoolId=cognito_user_pool_id,
    client_id=cognito_client_id
)


async def current_user_global_dep(request: Request, current_user: CognitoClaims = Depends(current_user_dep)):
    request.state.current_user = current_user
    return current_user


async def filesystem_dep(current_user: CognitoClaims = Depends(current_user_dep)):
    return get_user_filesystem(current_user.username)
