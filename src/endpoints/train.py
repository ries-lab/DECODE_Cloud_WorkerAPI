from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from database import get_db
from schemas import TrainJobCreate, TrainJob
from crud.model import get_model, update_model_state
from crud.job import create_train_job
from models import ModelStates
from dependencies import current_user_global_dep
from queue import get_queues

router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.post("/train", response_description="Train model", response_model=TrainJob)
def train_model(
    request: Request,
    train_job: TrainJobCreate,
    db: Any = Depends(get_db),
    queues: Any = Depends(get_queues),
):
    model = get_model(db, train_job.model_id)
    if not model or model.user_id != request.state.current_user.username:
        raise HTTPException(status_code=404, detail=f"Model {train_job.model_id} not found")
    if model.status == ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already trained")
    if model.status == ModelStates.training.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already training")

    db_train_job = create_train_job(db, queues, train_job)
    update_model_state(db, model, ModelStates.training)
    return db_train_job
