import json
import os

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
# local_queue = os.environ.get("LOCAL_QUEUE")  # SQS queue
# cloud_queue = os.environ.get("CLOUD_QUEUE")  # SQS queue
# any_queue = os.environ.get("ANY_QUEUE")  # SQS queue
