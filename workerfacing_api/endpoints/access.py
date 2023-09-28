import enum
from fastapi import APIRouter
from workerfacing_api.settings import cognito_user_pool_id, cognito_public_client_id


router = APIRouter()


class AccessType(enum.Enum):
    COGNITO="cognito"


@router.get("/access_info", response_model=dict[AccessType, dict])
def get_access_info():
    return {
        AccessType.COGNITO: {
            "cognito_user_pool_id": cognito_user_pool_id,
            "cognito_public_client_id": cognito_public_client_id,
        },
    }
