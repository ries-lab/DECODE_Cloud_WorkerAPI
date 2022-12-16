from fastapi import FastAPI

from endpoints import models

from database import engine, Base

Base.metadata.create_all(bind=engine)  # TODO: Move to Alembic

app = FastAPI()

app.include_router(models.router)


@app.get("/")
async def root():
    return {"message": "Hello World"}
