POETRY_VERSION=$(grep -Po '(?<=^requires = \["poetry-core=)[^"]*' pyproject.toml)
pip install "poetry==$POETRY_VERSION"
poetry install --no-root --no-dev --no-interaction --no-ansi
