FROM python:3.12.7-slim-bookworm

LABEL org.opencontainers.image.authors="svinther@gmail.com"
LABEL vendor="fastflow.dev"

RUN --mount=source=dist,target=/mnt/dist,type=bind \
    python -m pip install --no-cache --upgrade pip \
    && python -m pip install --no-cache /mnt/dist/*.whl

RUN adduser --disabled-password --disabled-login --home /app app && chown -R app.app /app
WORKDIR /app
USER app

CMD ["fastflow", "--namespace", "fastflow"]
