import json
import os


filesystem = os.environ.get("FILESYSTEM")  # filesystem
s3_bucket = os.environ.get("S3_BUCKET")
models_root_path = os.environ.get("MODELS_ROOT_PATH")
user_data_root_path = os.environ.get("USER_DATA_ROOT_PATH")

max_retries = int(os.environ.get("MAX_RETRIES"))
timeout_failure = int(os.environ.get("TIMEOUT_FAILURE"))
retry_different = bool(int(os.environ.get("RETRY_DIFFERENT")))
queue_db_url = os.environ.get("QUEUE_DB_URL")  # RDB queue

get_userfacing_api_url = lambda: os.environ.get("USERFACING_API_URL")  # need to be able to change this at runtime
internal_api_key_secret = os.environ.get("INTERNAL_API_KEY_SECRET")


if "QUEUE_DB_SECRET" in os.environ:
    queue_db_secret = os.environ["QUEUE_DB_SECRET"]
    try:
        queue_db_secret = json.loads(queue_db_secret)["password"]  # AWS Secrets Manager
    except:
        pass
    queue_db_url = queue_db_url.format(json.loads(os.environ["QUEUE_DB_SECRET"])["password"])
