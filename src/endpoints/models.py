from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, status

import crud
import database
import schemas
from settings import user_id


router = APIRouter()


@router.get("/models", response_model=list[schemas.Model])
def get_models(offset: int = 0, limit: int = 100, db: Session = Depends(database.get_db)):
    return crud.get_models(db, user_id, offset, limit)


@router.get("/models/{model_id}", response_model=schemas.Model)
def get_model(model_id: int, db: Session = Depends(database.get_db)):
    db_model = crud.get_model(db, model_id)
    if db_model is None:
        raise HTTPException(status_code=404, detail="Model not found")
    return db_model


@router.post("/models", response_model=schemas.Model)
def create_model(model: schemas.ModelCreate, db: Session = Depends(database.get_db)):
    return crud.create_model(db, user_id, model)


@router.put("/models/{model_id}", response_model=schemas.Model)
def update_model(model_id: int, model: schemas.ModelUpdate, db: Session = Depends(database.get_db)):
    db_model = crud.update_model(db, model_id, model, partial=False)
    if db_model is None:
        raise HTTPException(status_code=404, detail="Model not found")
    return db_model


@router.patch("/models/{model_id}", response_model=schemas.Model)
def patch_model(model_id: int, model: schemas.ModelUpdate, db: Session = Depends(database.get_db)):
    db_model = crud.update_model(db, model_id, model, partial=True)
    if db_model is None:
        raise HTTPException(status_code=404, detail="Model not found")
    return db_model


@router.delete("/models/{model_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_model(model_id: int, db: Session = Depends(database.get_db)):
    if not crud.delete_model(db, model_id):
        raise HTTPException(status_code=404, detail="Model not found")
    return {}
