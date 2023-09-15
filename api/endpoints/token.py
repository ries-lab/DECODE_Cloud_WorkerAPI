from fastapi import APIRouter, HTTPException, status
import boto3

from api.schemas.token import TokenLogin, TokenResponse
from api.core.aws import calculate_secret_hash
from api.settings import cognito_client_id, cognito_secret

router = APIRouter()


@router.post("/token", response_model=TokenResponse)
async def token(login: TokenLogin):
    client = boto3.client('cognito-idp')
    try:
        # Perform the login using the email and password
        response = client.initiate_auth(
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters={
                'USERNAME': login.email,
                'PASSWORD': login.password,
                'SECRET_HASH': calculate_secret_hash(login.email, cognito_client_id, cognito_secret)
            },
            ClientId=cognito_client_id,
        )
        # Get the ID token from the response
        id_token = response['AuthenticationResult']['IdToken']
        expires_in = response['AuthenticationResult']['ExpiresIn']
        return {"id_token": id_token, "expires_in": expires_in}
    except client.exceptions.NotAuthorizedException:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Incorrect email or password")
    except client.exceptions.UserNotFoundException:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=e.text)
