# DECODE_Cloud_WorkerAPI

Code for the worker-facing API of DECODE OpenCloud.  

The worker-facing API handles the communication with the workers.
The authenticated workers can:
 * handle jobs
   * pull jobs
   * update job status (on the background, the API checks whether pulled jobs have not received updates for some time and puts them back in the queue)
   * upload job results (via pre-signed urls)
 * download files (via pre-signed urls)

Behind the scenes, the API communicates with the user-facing API of DECODE OpenCloud.
It forwards the status updates to the user-facing API, and gets new jobs from it.

## Run locally
1. Copy the `.env.example` file to a `.env` file to the root of the directory.
2. Define the fields appropriately:
    - Data settings:
      - `FILESYSTEM`: one of `local` or `s3`, where the data is stored.
      - `S3_BUCKET`: if `FILESYSTEM==s3`, in what bucket the data is stored.
      - `USER_DATA_ROOT_PATH`: if `FILESYSTEM==local`, in what folder the data is stored. Relative paths can be used, but will only work with the worker-facing API if they start with `..` and the two repositories are in the same folder.
    - Job queue:
      - `QUEUE_DB_URL`: url of the queue database, e.g. `sqlite:///./sql_queue.db`.
      - `QUEUE_DB_SECRET`: secret to connect to the queue database, will be filled into the `QUEUE_DB_URL` in place of a `{}` placeholder. Can also be an AWS SecretsManager secret.
      - `MAX_RETRIES`: number of times a job will be retried after failing before it fails completely.
      - `TIMEOUT_FAILURE`: number of seconds after last pinging signal from worker before the job is considered as having silently failed.
      - `RETRY_DIFFERENT`: whether to only retry a failed job with a different worker (0 or 1).
    - User-facing API:
      - `USERFACING_API_URL`: url to use to connect to the user-facing API.
      - `INTERNAL_API_KEY_SECRET`: secret to authenticate to the user-facing API, and for the user-facing API to connect to this API, for internal endpoints. Can also be an AWS SecretsManager secret.
    - Authentication (at the moment, only AWS Cognito is supported):
      - `COGNITO_CLIENT_ID`: Cognito client ID.
      - `COGNITO_SECRET`: Secret for the client (if required). Can also be an AWS SecretsManager secret.
      - `COGNITO_USER_POOL_ID`: Cognito user pool ID.
      - `COGNITO_REGION`: Region for the user pool.
3. Start the user-facing API with `uvicorn workerfacing_api.main:workerfacing_app --reload --port 8001`.
4. You can view the API documentation at `http://localhost:8001/docs`.
