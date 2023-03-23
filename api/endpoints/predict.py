from typing import Any

import pydantic
from fastapi import APIRouter, Depends, HTTPException, Request

from api.database import get_db
from api.schemas import InferenceJob, InferenceJobCreate
from api.crud.model import get_model
from api.crud.job import create_inference_job
from api.dependencies import current_user_global_dep
from api.queue import get_enqueueing_function
from api.settings import version_config

router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.post("/predict", response_description="Predict using model model", response_model=InferenceJob)
def predict(
    request: Request,
    infer_job: InferenceJobCreate,
    db: Any = Depends(get_db),
    enqueueing_func: str = Depends(get_enqueueing_function),
):
    model = get_model(db, infer_job.model_id)
    if not model or model.user_id != request.state.current_user.username:
        raise HTTPException(status_code=404, detail=f"Model {infer_job.model_id} not found")

    attr_type_map = {item: (str, ...) for item in version_config[model.decode_version]['entrypoints']['fit']['params']}
    InferJobAttributes = pydantic.create_model('InferJobAttributes', **attr_type_map)
    try:
        InferJobAttributes.parse_obj(infer_job.attributes.dict())
    except pydantic.ValidationError as e:
        raise HTTPException(status_code=400, detail=e.errors())

    return create_inference_job(db, model, enqueueing_func, infer_job)
