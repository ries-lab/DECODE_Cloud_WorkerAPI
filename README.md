# DECODE_Cloud_WorkerAPI

Code for the worker-facing API of [DECODE OpenCloud](https://github.com/ries-lab/DECODE_Cloud_Documentation).  

The worker-facing API handles the communication with the workers.
The authenticated workers can:
 * handle jobs
   * pull jobs
   * update job status (on the background, the API checks whether pulled jobs have not received updates for some time and puts them back in the queue)
   * upload job results (via pre-signed urls)
 * download files (via pre-signed urls)

Behind the scenes, the API communicates with the [user-facing API](https://github.com/ries-lab/DECODE_Cloud_UserAPI) of DECODE OpenCloud.
It forwards the status updates to the [user-facing API](https://github.com/ries-lab/DECODE_Cloud_UserAPI), and gets new jobs from it.

## Development guide

### Prepare the development environment
We use [poetry](https://python-poetry.org/) for dependency tracking.
See online guides on how to use it, but this setup should work:
 - `conda create -n "3-11-10" python=3.11.10`
 - `conda activate 3-11-10`
 - `pip install pipx`
 - `pipx install poetry`
 - `poetry env use /path/to/conda/env/bin/python`
 - `poetry install`
Afterwards, when you need a new package, use `poetry add [--group dev] <package>` to add new dependencies.
The `--group dev` option adds the dependency only for development (e.g., pre-commit hooks, pytest, ...).

Install the pre-commit hooks: `pre-commit install`.
These currently include ruff and mypy.

### Run locally

#### Define the environment variables
Copy the `.env.example` file to a `.env` file at the root of the directory and define its fields appropriately:
 - Data settings:
   - `FILESYSTEM`: one of `local` or `s3`, where the data is stored.
   - `S3_BUCKET`: if `FILESYSTEM==s3`, in what bucket the data is stored.
   - `S3_REGION`: if `FILESYSTEM==s3`, in what region the bucket lies.
   - `USER_DATA_ROOT_PATH`: base path of the data storage (e.g. `../user_data` for a local filesystem, or `user_data` for S3 storage).
 - Job queue:
   - `QUEUE_DB_URL`: url of the queue database (e.g. `sqlite:///./sql_app.db` for a local database, or `postgresql://postgres:{}@<db_url>:5432/<db_name>` for a PostgreSQL database on AWS RDS).
   - `QUEUE_DB_SECRET`: secret to connect to the queue database, will be filled into the `QUEUE_DB_URL` in place of a `{}` placeholder. Can also be the ARN of an AWS SecretsManager secret.
   - `MAX_RETRIES`: number of times a job will be retried after failing before it fails definitely.
   - `TIMEOUT_FAILURE`: number of seconds after the last "keepalive" pinging signal from worker before the job is considered as having silently failed.
   - `RETRY_DIFFERENT`: whether to only retry a failed job with a different worker.
 - User-facing API:
   - `USERFACING_API_URL`: url to use to connect to the [user-facing API](https://github.com/ries-lab/DECODE_Cloud_UserAPI).
   - `INTERNAL_API_KEY_SECRET`: secret to authenticate to the [user-facing API](https://github.com/ries-lab/DECODE_Cloud_UserAPI), and for the [user-facing API](https://github.com/ries-lab/DECODE_Cloud_UserAPI) to authenticate to this API, for internal endpoints. Can also be the ARN of an AWS SecretsManager secret.
 - Authentication (nly AWS Cognito is supported):
   - `COGNITO_CLIENT_ID`: Cognito client ID.
   - `COGNITO_SECRET`: Secret for the client (if required). Can also be the ARN of an AWS SecretsManager secret.
   - `COGNITO_USER_POOL_ID`: Cognito user pool ID.
   - `COGNITO_REGION`: Region for the user pool.

#### Start the user-facing API
`poetry run serve`

#### View the API documentation
You can find it at `<API_URL>/docs` (if running locally, `<API_URL>=localhost:81`).

#### Docker
Alternatively, you can build a Docker image.
For this, run `poetry run docker-build`:
This will create a Docker image named `workerfacing-api:<branch_name>`.  
To run the Docker container, use `poetry run docker-serve`.  
To stop and delete all containers for this package, use `poetry run docker-stop`.
If you want to additionally remove all images for this package and prune dangling images, run `poetry run docker-cleanup`.

### Tests
Run them with `poetry run pytest`.

Note that tests marked with `aws` are skipped by default, to avoid the need for an AWS setup.
They are however ran in the GitHub Action.
For this to work, they must have been ran once locally with an account with sufficient permissions (`poetry run pytest -m "aws"`), since for security reasons, the AWS account used on GitHub does not have permissions to create RDS instances.
