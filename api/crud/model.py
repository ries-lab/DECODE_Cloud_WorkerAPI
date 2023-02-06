from pathlib import PurePosixPath

from sqlalchemy.orm import Session
from fastapi import HTTPException

import api.models as models
import api.schemas as schemas
import api.settings as settings
from api.core.filesystem import get_filesystem


def validate_model(model: models.Model):
    filesystem = get_filesystem(model.user_id)
    if not filesystem.exists(model.config_file):
        raise HTTPException(status_code=400, detail=f"Config file {model.config_file} does not exist")
    if not filesystem.exists(model.calibration_file):
        raise HTTPException(status_code=400, detail=f"Calibration file {model.calibration_file} does not exist")


def get_models(db: Session, user_id: str, skip: int = 0, limit: int = 100):
    return db.query(models.Model).where(models.Model.user_id == user_id).offset(skip).limit(limit).all()


def get_model(db: Session, model_id: int):
    return db.query(models.Model).where(models.Model.id == model_id).first()


def create_model(db: Session, user_id: str, model: schemas.ModelCreate):
    if model.decode_version == "latest":
        model.decode_version = list(models.DecodeVersions)[-1].value
    db_model = models.Model(**model.dict(), user_id=user_id)
    validate_model(db_model)
    db.add(db_model)
    db.flush()
    model_path = PurePosixPath(settings.models_root_path, user_id, f"{model.name}_{db_model.id}")
    db_model.model_path = model_path.as_posix()
    db.commit()
    db.refresh(db_model)
    return db_model


def delete_model(db: Session, user_id: str, model_id: int):
    db_model = db.query(models.Model).where(models.Model.id == model_id).first()
    if not db_model or db_model.user_id != user_id:
        return None
    filesystem = get_filesystem(user_id)
    filesystem.delete(db_model.model_path)
    db.delete(db_model)
    db.commit()
    return True


def update_model_state(db: Session, model: models.Model, state: schemas.ModelStates):
    model.status = state.value
    validate_model(model)
    db.add(model)
    db.commit()
    db.refresh(model)
    return model
