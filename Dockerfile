FROM python:3.10-alpine

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY ./workerfacing_api ./workerfacing_api

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "80"]
