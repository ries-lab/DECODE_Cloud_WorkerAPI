POETRY_VERSION=$(grep -Po '(?<=^requires = \["poetry-core=)[^"]*' pyproject.toml)
pip install "poetry==$POETRY_VERSION"
poetry install --no-dev --no-interaction --no-ansi
