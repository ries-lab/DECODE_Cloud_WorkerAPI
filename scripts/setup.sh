POETRY_VERSION=$(grep '^requires-poetry' pyproject.toml | sed -E 's/.*=\s*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/')
pip install "poetry==$POETRY_VERSION"
poetry install --without dev --no-interaction --no-ansi
