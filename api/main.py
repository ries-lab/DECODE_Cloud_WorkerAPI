import dotenv
dotenv.load_dotenv()

from fastapi import FastAPI
from .database import engine, Base

from api.endpoints import models, train, predict, files, token, user, jobs, job_update
from api.exceptions import register_exception_handlers


Base.metadata.create_all(bind=engine)

app = FastAPI()

app.include_router(models.router)
app.include_router(files.router)
app.include_router(token.router)
app.include_router(user.router)
app.include_router(jobs.router)
app.include_router(job_update.router)

register_exception_handlers(app)


@app.get("/")
async def root():
    return {"message": "Hello World"}
