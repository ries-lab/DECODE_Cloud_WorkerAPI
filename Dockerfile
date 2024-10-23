# Limitation:
#   - Big image size: uses poetry, gets the whole repo, ...
#   - No caching/multi-stage build: the whole image is rebuilt every time
# Done this way for easier mapping AWS AppRunner from GitHub source <-> Dockerfile

ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION}-slim as builder

WORKDIR /app

COPY . /app/
RUN chmod +x /app/scripts/setup.sh && /app/scripts/setup.sh
CMD ["poetry", "run", "serve"]
