from sqlalchemy.orm import Session

from fastapi import APIRouter, Depends, HTTPException, status, Request

import api.crud as crud
import api.database as database
import api.schemas as schemas
from api.dependencies import current_user_global_dep


router = APIRouter(dependencies=[Depends(current_user_global_dep)])

model_not_found_error = HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Model not found")


@router.get("/models", response_model=list[schemas.Model])
def get_models(request: Request, offset: int = 0, limit: int = 100, db: Session = Depends(database.get_db)):
    return crud.get_models(db, request.state.current_user.username, offset, limit)


@router.get("/models/{model_id}", response_model=schemas.Model)
def get_model(request: Request, model_id: int, db: Session = Depends(database.get_db)):
    db_model = crud.get_model(db, model_id)
    if db_model is None or db_model.user_id != request.state.current_user.username:
        raise model_not_found_error
    return db_model


@router.post("/models", response_model=schemas.Model)
def create_model(request: Request, model: schemas.ModelCreate, db: Session = Depends(database.get_db)):
    return crud.create_model(db, request.state.current_user.username, model)


@router.delete("/models/{model_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_model(request: Request, model_id: int, db: Session = Depends(database.get_db)):
    if not crud.delete_model(db, request.state.current_user.username, model_id):
        raise model_not_found_error
    return {}
