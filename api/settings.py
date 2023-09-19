import json
import os
from typing import Any
import yaml

database_url = os.environ.get("DATABASE_URL", "sqlite:///./sql_app.db")
if "DATABASE_SECRET" in os.environ:
    database_secret = os.environ["DATABASE_SECRET"]
    try:
        database_secret = json.loads(database_secret)["password"]  # AWS Secrets Manager
    except:
        pass
    database_url = database_url.format(database_secret)
workerfacing_api_url = os.environ.get("WORKERFACING_API_URL", "http://127.0.0.1:8001")
internal_api_key_secret = os.environ.get("INTERNAL_API_KEY_SECRET")
try:
    internal_api_key_secret = json.loads(internal_api_key_secret)["password"]  # AWS Secrets Manager
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
outputs_root_path = os.environ.get("OUTPUTS_ROOT_PATH")

application_config_file = os.environ.get("APPLICATION_CONFIG_FILE", os.path.join(os.path.dirname(__file__), "..", "application_config.yaml"))

class JITConfig(object):
    def __getattribute__(self, __name: str) -> Any:
        with open(application_config_file) as f:
            config = yaml.safe_load(f)
        if __name == 'config':
            return config
        return getattr(config, __name)
    
    def __getitem__(self, item):
        return self.config[item]

application_config = JITConfig()
