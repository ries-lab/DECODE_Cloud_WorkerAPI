import json
import os

# Data
filesystem = os.environ.get("FILESYSTEM")  # filesystem
s3_bucket = os.environ.get("S3_BUCKET")
s3_region = os.environ.get("S3_REGION")
user_data_root_path = os.environ.get("USER_DATA_ROOT_PATH")


# Queue
max_retries = int(os.environ.get("MAX_RETRIES", 2))
timeout_failure = int(os.environ.get("TIMEOUT_FAILURE", 300))
retry_different = bool(int(os.environ.get("RETRY_DIFFERENT", 1)))
queue_db_url = os.environ.get("QUEUE_DB_URL", "sqlite:///./sql_queue.db")  # RDB queue

if os.environ.get("QUEUE_DB_SECRET"):  # exists and not None
    queue_db_secret = os.environ["QUEUE_DB_SECRET"]
    try:
        queue_db_secret = json.loads(queue_db_secret)["password"]  # AWS Secrets Manager
    except json.JSONDecodeError:
        pass
    queue_db_url = queue_db_url.format(
        json.loads(os.environ["QUEUE_DB_SECRET"])["password"]
    )


# User-facing API
# need to be able to change this at runtime
def get_userfacing_api_url() -> str | None:
    return os.environ.get("USERFACING_API_URL")


internal_api_key_secret = os.environ.get("INTERNAL_API_KEY_SECRET")


# Authentication
cognito_user_pool_id = os.environ.get("COGNITO_USER_POOL_ID")
# default to avoid ConnectionError in tests when initializing `current_user_dep`
cognito_region = os.environ.get("COGNITO_REGION", "eu-central-1")
cognito_client_id = os.environ.get("COGNITO_CLIENT_ID")
cognito_secret = os.environ.get("COGNITO_SECRET")
if cognito_secret:  # exists and not None
    try:
        cognito_secret = json.loads(cognito_secret)["password"]  # AWS Secrets Manager
    except json.JSONDecodeError:
        pass
