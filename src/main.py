from fastapi import FastAPI

from endpoints import models, train, predict, files
from exceptions import register_exception_handlers

from database import engine, Base

Base.metadata.create_all(bind=engine)  # TODO: Move to Alembic

app = FastAPI()

app.include_router(models.router)
app.include_router(train.router)
app.include_router(predict.router)
app.include_router(files.router)

register_exception_handlers(app)

@app.get("/")
async def root():
    return {"message": "Hello World"}
