import dotenv

dotenv.load_dotenv()

from fastapi import FastAPI, Depends
from fastapi_utils.tasks import repeat_every

from workerfacing_api.endpoints import files, jobs, jobs_post, access
from workerfacing_api import dependencies, settings, tags


workerfacing_app = FastAPI(openapi_tags=tags.tags_metadata)

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


queue = dependencies.get_queue()


@workerfacing_app.on_event("startup")
@repeat_every(seconds=60, raise_exceptions=True)
async def find_failed_jobs():
    print("Silent fails check: starting...")
    try:
        max_retries = settings.max_retries
        timeout_failure = settings.timeout_failure
        n_retry, n_fail = queue.handle_timeouts(max_retries, timeout_failure)
        print(f"Silent fails check: {n_retry} re-queued, {n_fail} failed.")
        return {"n_retry": n_retry, "n_fail": n_fail}
    except Exception as e:
        print(f"Silent fails check: failed with {e}")


@workerfacing_app.get("/")
async def root():
    return {"message": "Welcome to the DECODE OpenCloud Worker-facing API"}
