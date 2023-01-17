from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from database import get_db
from schemas import InferenceJob, InferenceJobCreate
from crud.model import get_model
from crud.job import create_inference_job
from models import ModelStates
from dependencies import current_user_global_dep

router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.post("/predict", response_description="Predict using model model", response_model=InferenceJob)
def predict(
    request: Request,
    infer_job: InferenceJobCreate,
    db: Any = Depends(get_db),
):
    model = get_model(db, infer_job.model_id)
    if not model or model.user_id != request.state.current_user.username:
        raise HTTPException(status_code=404, detail=f"Model {infer_job.model_id} not found")
    if model.status == ModelStates.trained.value:
        raise HTTPException(status_code=400, detail=f"Model {infer_job.model_id} has not been trained trained")

    db_train_job = create_inference_job(db, infer_job)
    return db_train_job
