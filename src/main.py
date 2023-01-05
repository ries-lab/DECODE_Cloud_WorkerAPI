from fastapi import FastAPI

from .endpoints import models, train, predict

from .database import engine, Base



Base.metadata.create_all(bind=engine)  # TODO: Move to Alembic

app = FastAPI()

app.include_router(models.router)
app.include_router(train.router)
app.include_router(predict.router)


@app.get("/")
async def root():
    return {"message": "Hello World"}
