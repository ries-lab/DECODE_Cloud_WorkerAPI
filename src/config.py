import os
from functools import lru_cache
from pydantic import BaseSettings


@lru_cache()
def get_settings(env_file_path="../.env"):
    class Settings(BaseSettings):
        DATABASE_URL: str
        LOCAL_QUEUE: str
        CLOUD_QUEUE: str
        ANY_QUEUE: str
        class Config:
            env_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                env_file_path)
    return Settings()
