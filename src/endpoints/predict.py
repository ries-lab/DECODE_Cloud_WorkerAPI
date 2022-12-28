from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from ..database import get_db
from ..schemas import InferenceJob, InferenceJobCreate
from ..crud.model import get_model
from ..crud.job import create_inference_job
from ..models import ModelStates
from ..settings import user_id
from ..core.queue import JobQueue, get_queue


router = APIRouter()


@router.post("/predict", response_description="Predict using model model", response_model=InferenceJob)
def predict(
    infer_job: InferenceJobCreate,
    db: Any = Depends(get_db),
    queue: JobQueue = Depends(get_queue),
):
    model = get_model(db, infer_job.model_id)
    if not model or model.user_id != user_id:
        raise HTTPException(status_code=404, detail=f"Model {infer_job.model_id} not found")
    if model.status == ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {infer_job.model_id} has not been trained")

    db_train_job = create_inference_job(db, queue, infer_job)
    return db_train_job
