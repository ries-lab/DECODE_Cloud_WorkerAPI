from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from ..database import get_db
from ..schemas import TrainJobCreate, TrainJob
from ..crud.model import get_model, update_model_state
from ..crud.job import create_train_job
from ..models import ModelStates
from ..settings import user_id
from ..core.queue import get_queue
from ..config import get_settings

router = APIRouter()


@router.post("/train", response_description="Train model", response_model=TrainJob)
def train_model(
    train_job: TrainJobCreate,
    db: Any = Depends(get_db),
    settings: Any = Depends(get_settings),
):
    queue = get_queue(settings.QUEUE_PATH)
    model = get_model(db, train_job.model_id)
    if not model or model.user_id != user_id:
        raise HTTPException(status_code=404, detail=f"Model {train_job.model_id} not found")
    if model.status == ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already trained")
    if model.status == ModelStates.training.value:
        raise HTTPException(status_code=400, detail=f"Model {train_job.model_id} is already training")

    db_train_job = create_train_job(db, queue, train_job)
    update_model_state(db, model, ModelStates.training)
    return db_train_job
