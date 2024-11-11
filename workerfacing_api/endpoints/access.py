import enum

from fastapi import APIRouter

from workerfacing_api.settings import (
    cognito_client_id,
    cognito_region,
    cognito_user_pool_id,
)

router = APIRouter()


class AccessType(enum.Enum):
    COGNITO = "cognito"


@router.get(
    "/access_info",
    response_model=dict[AccessType, dict[str, str | None]],
    description="Get information about where API users should authenticate.",
)
def get_access_info() -> dict[AccessType, dict[str, str | None]]:
    return {
        AccessType.COGNITO: {
            "user_pool_id": cognito_user_pool_id,
            "client_id": cognito_client_id,
            "region": cognito_region,
        },
    }
