from fastapi_cloudauth.cognito import CognitoCurrentUser

from settings import cognito_client_id, cognito_region, cognito_user_pool_id

current_user_dep = CognitoCurrentUser(
    region=cognito_region,
    userPoolId=cognito_user_pool_id,
    client_id=cognito_client_id
)
