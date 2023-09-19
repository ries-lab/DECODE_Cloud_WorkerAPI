FROM python:3.11-alpine

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY ./api ./api
COPY application_config.yaml application_config.yaml

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "80"]
