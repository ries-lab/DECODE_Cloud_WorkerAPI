from sqlalchemy.orm import Session

import models
import schemas


def get_models(db: Session, user_id: str, skip: int = 0, limit: int = 100):
    return db.query(models.Model).where(models.Model.user_id == user_id).offset(skip).limit(limit).all()


def get_model(db: Session, model_id: int):
    return db.query(models.Model).where(models.Model.id == model_id).first()


def create_model(db: Session, user_id: str, model: schemas.ModelCreate):
    db_model = models.Model(**model.dict(), user_id=user_id)
    db.add(db_model)
    db.commit()
    db.refresh(db_model)
    return db_model


def update_model(db: Session, user_id: str, model_id: int, model: schemas.ModelUpdate, partial: bool = False):
    db_model = get_model(db, model_id)
    if not db_model or db_model.user_id != user_id:
        return None
    for field in model.dict(exclude_unset=partial):
        setattr(db_model, field, getattr(model, field))
    db.commit()
    db.refresh(db_model)
    return db_model


def delete_model(db: Session, user_id: str, model_id: int):
    # TODO: delete model files
    # TODO: delete related jobs
    db_model = db.query(models.Model).where(models.Model.id == model_id).first()
    if not db_model or db_model.user_id != user_id:
        return None
    db.delete(db_model)
    db.commit()
    return True


def update_model_state(db: Session, model: models.Model, state: schemas.ModelStates):
    model.status = state.value
    db.commit()
    db.refresh(model)
    return model
