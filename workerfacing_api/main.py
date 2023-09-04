import dotenv
dotenv.load_dotenv()
from fastapi import FastAPI, Depends
from workerfacing_api.auth import APIKeyDependency
from workerfacing_api.endpoints import jobs
from workerfacing_api.settings import internal_api_key_secret


authorizer = APIKeyDependency(key=internal_api_key_secret)

workerfacing_app = FastAPI()

workerfacing_app.include_router(jobs.router, dependencies=[Depends(authorizer)])


@workerfacing_app.get("/")
async def root():
    return {"message": "Hello World"}
