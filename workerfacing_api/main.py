import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import dotenv
from fastapi import Depends, FastAPI

dotenv.load_dotenv()

from workerfacing_api import dependencies, settings, tags
from workerfacing_api.endpoints import access, files, jobs, jobs_post

queue = dependencies.queue_dep()


async def cron_handle_timeouts() -> dict[str, int]:
    while True:
        await asyncio.sleep(300)  # every 5 minutes
        print("Silent fails check: starting...")
        try:
            max_retries = settings.max_retries
            timeout_failure = settings.timeout_failure
            n_retry, n_fail = queue.handle_timeouts(max_retries, timeout_failure)
            print(f"Silent fails check: {n_retry} re-queued, {n_fail} failed.")
            return {"n_retry": n_retry, "n_fail": n_fail}
        except Exception as e:
            print(f"Silent fails check: failed with {e}")
            return {"n_retry": 0, "n_fail": 0}


async def cron_backup_database() -> bool:
    while True:
        await asyncio.sleep(3600)  # every hour
        # Run backup in thread pool to avoid blocking event loop;
        # Fine instead of making backup async since it runs infrequently.
        if await asyncio.to_thread(queue.backup):
            print("Backed up database.")
            return True
        return False


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    task_failed_jobs = asyncio.create_task(cron_handle_timeouts())
    task_backup = asyncio.create_task(cron_backup_database())
    yield
    task_failed_jobs.cancel()
    task_backup.cancel()
    if queue.backup():  # final backup on shutdown
        print("Created final backup on shutdown.")


workerfacing_app = FastAPI(openapi_tags=tags.tags_metadata, lifespan=lifespan)

workerfacing_app.include_router(
    jobs.router,
    dependencies=[Depends(dependencies.current_user_global_dep)],
)
workerfacing_app.include_router(
    files.router,
    dependencies=[Depends(dependencies.current_user_global_dep)],
    tags=["Files"],
)
workerfacing_app.include_router(access.router, tags=["Authentication"])
# private endpoint for user-facing API to call
workerfacing_app.include_router(
    jobs_post.router,
    dependencies=[Depends(dependencies.authorizer)],
    tags=["_Internal"],
)


@workerfacing_app.get("/")
async def root() -> dict[str, str]:
    return {"message": "Welcome to the DECODE OpenCloud Worker-facing API"}
