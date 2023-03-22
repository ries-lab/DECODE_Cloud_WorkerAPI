import os
import typing
from fastapi import Header, HTTPException

# https://github.com/iwpnd/fastapi-key-auth/blob/main/fastapi_key_auth/dependency/authorizer.py
class APIKeyDependency:
    def __init__(self, key: str):
        self.key = key

    def __call__(self, x_api_key: typing.Optional[str] = Header(...)):
        if x_api_key != os.environ[self.key]:
            raise HTTPException(status_code=401, detail="unauthorized")
        return x_api_key
