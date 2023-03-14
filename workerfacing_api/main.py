import dotenv
dotenv.load_dotenv()
from fastapi import FastAPI
from workerfacing_api.endpoints import jobs


workerfacing_app = FastAPI()

workerfacing_app.include_router(jobs.router)


@workerfacing_app.get("/")
async def root():
    return {"message": "Hello World"}
