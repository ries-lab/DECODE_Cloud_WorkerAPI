import os

database_url = os.environ.get("DATABASE_URL", "sqlite:///./sql_app.db")
local_queue = os.environ.get("LOCAL_QUEUE")
cloud_queue = os.environ.get("CLOUD_QUEUE")
any_queue = os.environ.get("ANY_QUEUE")

cognito_client_id = os.environ.get("COGNITO_CLIENT_ID")
cognito_user_pool_id = os.environ.get("COGNITO_USER_POOL_ID")
cognito_region = os.environ.get("COGNITO_REGION")
cognito_secret = os.environ.get("COGNITO_SECRET")

filesystem = os.environ.get("FILESYSTEM")
s3_bucket = os.environ.get("S3_BUCKET")
user_data_root_path = os.environ.get("USER_DATA_ROOT_PATH")
models_root_path = os.environ.get("MODELS_ROOT_PATH")
