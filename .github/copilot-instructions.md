# DECODE Cloud Worker-Facing API

DECODE Cloud Worker-Facing API is a Python FastAPI application that handles communication between workers and the DECODE OpenCloud system. It manages job queuing, status updates, file operations, and authentication via AWS Cognito.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Bootstrap and Setup
NEVER CANCEL: All timing-sensitive operations below have been validated. Wait for completion.

- Install Poetry (exact version required): `pip install "poetry==2.1.3"` -- takes 15 seconds
  - If you get version mismatch errors, ensure you have exactly version 2.1.3
  - Check with: `poetry --version`
- Install dependencies: `poetry install` -- takes 17 seconds. NEVER CANCEL.
- Create environment file: `cp .env.example .env` and edit with valid AWS Cognito credentials
- Install pre-commit hooks: `poetry run pre-commit install` -- takes 1 second

### Code Quality and Testing
NEVER CANCEL: Set timeouts of 5+ minutes for mypy and test commands.

- Run linting: `poetry run ruff check .` -- takes 1 second
- Run format check: `poetry run ruff format --check .` -- takes 1 second
- Run type checking: `poetry run mypy .` -- takes 57 seconds. NEVER CANCEL. Set timeout to 5+ minutes.
- Run unit tests: `poetry run pytest tests/unit` -- takes 3.5 minutes. NEVER CANCEL. Set timeout to 10+ minutes.
- Run all tests (requires AWS): `poetry run pytest` -- includes integration tests, takes longer

### Application Runtime
CRITICAL: Application startup requires valid AWS Cognito credentials and network access to `cognito-idp.{region}.amazonaws.com`.

- Start development server: `poetry run serve` -- fails without valid AWS Cognito setup
- The application runs on `http://0.0.0.0:8001` by default
- Check API documentation at `http://localhost:8001/docs` when running

### Docker Operations
- Manual build: `docker build -t workerfacing-api:test --build-arg PYTHON_VERSION=3.11.10 .`
- Poetry scripts: `poetry run docker-build`, `poetry run docker-serve`, `poetry run docker-stop`, `poetry run docker-cleanup`
- Docker builds may fail due to network restrictions or invalid branch names in tags

## Validation Requirements

### ALWAYS run these commands before committing:
- `poetry run ruff check .` -- must pass
- `poetry run ruff format --check .` -- must pass  
- `poetry run mypy .` -- must pass, takes 57 seconds
- `poetry run pytest tests/unit` -- must pass, takes 3.5 minutes

### Manual Testing Scenarios
CRITICAL: The application cannot be fully tested without valid AWS credentials. For local development:

1. Ensure `.env` file has valid AWS Cognito settings:
   - `COGNITO_USER_POOL_ID`: Valid Cognito user pool ID
   - `COGNITO_REGION`: AWS region (e.g., `eu-central-1`)
   - `COGNITO_CLIENT_ID`: Valid Cognito client ID
   - `COGNITO_SECRET`: Valid Cognito client secret

2. Test API endpoints using the FastAPI docs interface at `/docs`
3. Verify worker authentication flow
4. Test job queue operations (requires database setup)
5. Test file upload/download operations (requires S3 or local filesystem)

## Environment Configuration

### Required Environment Variables
From `.env.example`, ensure these are properly configured:

- **Authentication**: `COGNITO_USER_POOL_ID`, `COGNITO_REGION`, `COGNITO_CLIENT_ID`, `COGNITO_SECRET`
- **Database**: `QUEUE_DB_URL` (SQLite or PostgreSQL), `QUEUE_DB_SECRET`
- **Storage**: `FILESYSTEM` (local/s3), `S3_BUCKET`, `USER_DATA_ROOT_PATH`
- **API Integration**: `USERFACING_API_URL`, `INTERNAL_API_KEY_SECRET`
- **Job Settings**: `MAX_RETRIES`, `TIMEOUT_FAILURE`, `RETRY_DIFFERENT`

### Python Version Requirements
- Project requires Python 3.11.10 exactly
- Poetry version 2.1.3 exactly (specified in pyproject.toml)
- Will work with Python 3.12+ if pyproject.toml is modified (change `python = "3.11.10"` to `python = "^3.11"`)

## Limitations and Known Issues

### Network/AWS Dependencies
- Application startup fails without network access to AWS Cognito
- Integration tests require AWS credentials and services
- Docker builds may fail in restricted network environments
- Unit tests work without AWS but integration tests require valid AWS setup

### Development Constraints
- Cannot run full application stack without valid AWS Cognito configuration
- Pre-signed URL functionality requires S3 access
- Job queue operations require database connectivity
- Authentication testing requires valid Cognito user pool

## Common Tasks

### Repository Structure
```
├── .env.example                    # Environment configuration template
├── .github/workflows/             # CI/CD workflows
├── .pre-commit-config.yaml        # Pre-commit hook configuration
├── Dockerfile                     # Container build configuration
├── README.md                      # Project documentation
├── poetry.lock                    # Locked dependency versions
├── pyproject.toml                 # Project and dependency configuration
├── scripts/                       # Helper scripts (serve.py, docker.py, setup.sh)
├── tests/                         # Test suite
│   ├── integration/              # Integration tests (require AWS)
│   └── unit/                     # Unit tests (no AWS required)
└── workerfacing_api/             # Main application code
    ├── core/                     # Core business logic
    ├── crud/                     # Database operations
    ├── endpoints/                # API endpoint definitions
    ├── schemas/                  # Pydantic models
    ├── dependencies.py           # FastAPI dependencies
    ├── main.py                   # Application entry point
    └── settings.py               # Configuration management
```

### Frequently Used Commands
- Check project status: `poetry run ruff check . && poetry run mypy . && poetry run pytest tests/unit`
- Reset environment: `rm -rf .venv && poetry install`
- View logs: Application logs to stdout when running `poetry run serve`
- Update dependencies: `poetry update` (then run `poetry lock`)

### CI/CD Integration
- GitHub Actions workflow runs on pushes/PRs to main branch
- Includes static code checks (ruff, mypy) with 10-minute timeout
- Runs full test suite including AWS integration tests with 60-minute timeout
- Uses Python 3.11.10 in CI environment
- AWS credentials required for integration test execution