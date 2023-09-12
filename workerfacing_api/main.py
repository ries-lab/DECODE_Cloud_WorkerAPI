import dotenv
dotenv.load_dotenv()
from fastapi import FastAPI, Depends
from fastapi_utils.tasks import repeat_every
from workerfacing_api.auth import APIKeyDependency
from workerfacing_api.endpoints import files, jobs
from workerfacing_api.queue import get_queue
from workerfacing_api import settings


authorizer = APIKeyDependency(key=settings.internal_api_key_secret)

workerfacing_app = FastAPI()

workerfacing_app.include_router(jobs.router, dependencies=[Depends(authorizer)])
workerfacing_app.include_router(files.router, dependencies=[Depends(authorizer)])


queue = get_queue()

@workerfacing_app.on_event("startup")
@repeat_every(seconds=60, raise_exceptions=True)
async def find_failed_jobs():
    max_retries = int(settings.max_retries)
    timeout_failure = int(settings.timeout_failure)
    n_retry, n_fail = queue.handle_timeouts(max_retries, timeout_failure)
    print(f"Silent fails check: {n_retry} re-queued, {n_fail} failed.")


@workerfacing_app.get("/")
async def root():
    return {"message": "Hello World"}
