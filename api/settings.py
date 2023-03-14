import os

database_url = os.environ.get("DATABASE_URL", "sqlite:///./sql_app.db")
workerfacing_api_url = os.environ.get("WORKERFACING_API_URL", "http://127.0.0.1:81")

cognito_client_id = os.environ.get("COGNITO_CLIENT_ID")
cognito_user_pool_id = os.environ.get("COGNITO_USER_POOL_ID")
cognito_region = os.environ.get("COGNITO_REGION")
cognito_secret = os.environ.get("COGNITO_SECRET")

filesystem = os.environ.get("FILESYSTEM")
s3_bucket = os.environ.get("S3_BUCKET")
user_data_root_path = os.environ.get("USER_DATA_ROOT_PATH")
models_root_path = os.environ.get("MODELS_ROOT_PATH")
