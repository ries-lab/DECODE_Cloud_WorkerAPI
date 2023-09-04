# DECODE_CloudInterface

## Run locally
1. Copy the `.env.example` file to a `.env` file to the root of the directory.
2. Define the fields appropriately: `DATABASE_URL` and `QUEUE_DB_URL` are local databases, e.g. `sqlite:///./sql_app.db` and `sqlite:///./sql_queue.db`; `WORKERFACING_API_URL` is the url at which you will deploy the workerfacing API, `INTERNAL_API_KEY_SECRET` a string secret; the cognito fields define which cognito pool should be used (require an app client); `FILESYSTEM` is either local or s3 (in which case you need to specify `S3_BUCKET`); typically, `USER_DATA_ROOT_PATH="./user_data"` and `MODELS_ROOT_PATH="./models"`.
3. Start the userfacing API `uvicorn api.main:app --reload` and the workerfacing API `uvicorn workerfacing_api.main:workerfacing_app --reload`.
