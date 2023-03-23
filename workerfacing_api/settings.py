import json
import os

queue_db_url = os.environ.get("QUEUE_DB_URL")  # RDB queue
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
