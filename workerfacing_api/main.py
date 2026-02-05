import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import dotenv
from fastapi import Depends, FastAPI

dotenv.load_dotenv()

logger = logging.getLogger(__name__)

from workerfacing_api import dependencies, settings, tags
from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api.endpoints import access, files, jobs, jobs_post


async def cron_handle_timeouts(queue: RDSJobQueue) -> None:
    while True:
        await asyncio.sleep(settings.cron_timeout_check_interval)
        logger.info("Silent fails check: starting...")
        try:
            max_retries = settings.max_retries
            timeout_failure = settings.timeout_failure
            n_retry, n_fail = queue.handle_timeouts(max_retries, timeout_failure)
            logger.info(f"Silent fails check: {n_retry} re-queued, {n_fail} failed.")
        except Exception as e:
            logger.error(f"Silent fails check: failed with {e}")


async def cron_backup_database(queue: RDSJobQueue) -> None:
    while True:
        await asyncio.sleep(settings.cron_backup_interval)
        logger.info("Database backup: starting...")
        # Run backup in thread pool to avoid blocking event loop;
        # Fine instead of making backup async since it runs infrequently.
        try:
            if await asyncio.to_thread(queue.backup):
                logger.info("Backed up database.")
        except Exception as e:
            logger.error(f"Database backup failed with {e}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    queue = app.dependency_overrides.get(
        dependencies.queue_dep, dependencies.queue_dep
    )()
    assert isinstance(queue, RDSJobQueue)
    queue.create()
    task_failed_jobs = asyncio.create_task(cron_handle_timeouts(queue))
    task_backup = asyncio.create_task(cron_backup_database(queue))
    yield
    task_failed_jobs.cancel()
    task_backup.cancel()
    await asyncio.gather(task_failed_jobs, task_backup, return_exceptions=True)
    if queue.backup():
        logger.info("Created final backup on shutdown.")


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
