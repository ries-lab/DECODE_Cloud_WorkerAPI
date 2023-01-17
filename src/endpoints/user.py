import boto3

from fastapi import APIRouter, HTTPException, Depends
from fastapi_cloudauth.cognito import CognitoClaims

from schemas.user import User, UserCreate
from settings import cognito_user_pool_id
from dependencies import current_user_dep

router = APIRouter()


@router.get("/user", response_model=User)
def get_user(current_user: CognitoClaims = Depends(current_user_dep)):
    return {"email": current_user.email}


@router.post("/user", response_model=User)
def register_user(user: UserCreate):
    client = boto3.client('cognito-idp')

    try:
        # Perform the signup using the email and password
        client.admin_create_user(
            UserPoolId=cognito_user_pool_id,
            Username=user.email,
            TemporaryPassword=user.password,
            MessageAction='SUPPRESS',
        )

        # Reset password to change state
        client.admin_set_user_password(
            UserPoolId=cognito_user_pool_id,
            Username=user.email,
            Password=user.password,
            Permanent=True
        )
        return {"email": user.email}
    except client.exceptions.UsernameExistsException:
        raise HTTPException(status_code=400, detail="User already exists")