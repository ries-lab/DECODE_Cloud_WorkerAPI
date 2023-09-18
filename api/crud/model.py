from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from fastapi import HTTPException, status

import api.models as models
import api.schemas as schemas
from api.core.filesystem import get_user_outputs_filesystem


def get_models(db: Session, user_id: str, skip: int = 0, limit: int = 100):
    return db.query(models.Model).where(models.Model.user_id == user_id).offset(skip).limit(limit).all()


def get_model(db: Session, model_id: int):
    return db.query(models.Model).where(models.Model.id == model_id).first()


def create_model(db: Session, user_id: str, model: schemas.ModelCreate):
    try:
        db_model = models.Model(name=model.name, software=model.software, version=model.version, user_id=user_id)
        db.add(db_model)
        db.flush()
    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Model name must be unique",
        )
    db_model.model_path = model.name
    db.add(db_model)
    db.commit()
    db.refresh(db_model)
    return db_model


def delete_model(db: Session, user_id: str, model_id: int):
    db_model = db.query(models.Model).where(models.Model.id == model_id).first()
    if not db_model or db_model.user_id != user_id:
        return None
    filesystem = get_user_outputs_filesystem(user_id)
    filesystem.delete(db_model.model_path)
    db.delete(db_model)
    db.commit()
    return True


def update_model_state(db: Session, model: models.Model, state: schemas.ModelStates):
    model.status = state.value
    db.add(model)
    db.commit()
    db.refresh(model)
    return model
