import json
import os
import yaml

database_url = os.environ.get("DATABASE_URL", "sqlite:///./sql_app.db")
if "DATABASE_SECRET" in os.environ:
    database_secret = os.environ["DATABASE_SECRET"]
    try:
        database_secret = json.loads(database_secret)["password"]  # AWS Secrets Manager
    except:
        pass
    database_url = database_url.format(database_secret)
workerfacing_api_url = os.environ.get("WORKERFACING_API_URL", "http://127.0.0.1:81")
workerfacing_api_key = os.environ.get("WORKERFACING_API_KEY", "")
try:
    workerfacing_api_key = json.loads(workerfacing_api_key)["password"]  # AWS Secrets Manager
except:
    pass

cognito_client_id = os.environ.get("COGNITO_CLIENT_ID")
cognito_user_pool_id = os.environ.get("COGNITO_USER_POOL_ID")
cognito_region = os.environ.get("COGNITO_REGION")
cognito_secret = os.environ.get("COGNITO_SECRET")
try:
    cognito_secret = json.loads(cognito_secret)["password"]  # AWS Secrets Manager
except:
    pass

filesystem = os.environ.get("FILESYSTEM")
s3_bucket = os.environ.get("S3_BUCKET")
user_data_root_path = os.environ.get("USER_DATA_ROOT_PATH")
models_root_path = os.environ.get("MODELS_ROOT_PATH")

version_config_file = os.environ.get("VERSION_CONFIG_FILE", os.path.join(os.path.dirname(__file__), "..", "version_config.yaml"))

with open(version_config_file) as f:
    version_config = yaml.safe_load(f)['versions']
