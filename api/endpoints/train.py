from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from api.database import get_db
from api.schemas import TrainJobCreate, TrainJob
from api.crud.model import get_model
from api.crud.job import create_train_job
from api.dependencies import current_user_global_dep
from api.queue import get_enqueueing_function

router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.post("/train", response_description="Train model", response_model=TrainJob)
def train_model(
    request: Request,
    train_job: TrainJobCreate,
    db: Any = Depends(get_db),
    enqueueing_func: str = Depends(get_enqueueing_function),
):
    model = get_model(db, train_job.model_id)
    if not model or model.user_id != request.state.current_user.username:
        raise HTTPException(status_code=404, detail=f"Model {train_job.model_id} not found")

    return create_train_job(db, model, enqueueing_func, train_job)
