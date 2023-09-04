import typing
from fastapi import Header, HTTPException

# https://github.com/iwpnd/fastapi-key-auth/blob/main/fastapi_key_auth/dependency/authorizer.py
#TODO: we probably need a more secure authentification method
#TODO: use one key per worker, so that we can revoke keys
class APIKeyDependency:
    def __init__(self, key: str):
        self.key = key

    def __call__(self, x_api_key: typing.Optional[str] = Header(...)):
        if x_api_key != self.key:
            raise HTTPException(status_code=401, detail="unauthorized")
        return x_api_key
