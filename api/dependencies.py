import typing

from fastapi import Request, Depends, Header, HTTPException
from fastapi_cloudauth.cognito import CognitoCurrentUser, CognitoClaims

from api.core.filesystem import get_user_filesystem
from api.settings import cognito_client_id, cognito_region, cognito_user_pool_id, internal_api_key_secret

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


class APIKeyDependency:
    def __init__(self, key: str):
        self.key = key

    def __call__(self, x_api_key: typing.Optional[str] = Header(...)):
        if x_api_key != self.key:
            raise HTTPException(status_code=401, detail="unauthorized")
        return x_api_key


workerfacing_api_auth_dep = APIKeyDependency(internal_api_key_secret)
