## build stage
#FROM python:3.10.6-alpine as builder
#
#WORKDIR /app
#
#ENV PYTHONDONTWRITEBYTECODE 1
#ENV PYTHONUNBUFFERED 1
#
#RUN apk add git
#
#RUN python -m pip install --upgrade pip \
#&& python -m pip install build
#
#COPY src .
#COPY pyproject.toml .
#COPY setup.py .
#RUN --mount=source=.git,target=.git,type=bind python -m build
#
# final stage
FROM python:3.10.6-alpine

LABEL org.opencontainers.image.authors="svinther@gmail.com"
LABEL vendor="fastflow.dev"

RUN --mount=source=dist,target=/mnt/dist,type=bind \
    python -m pip install --no-cache --upgrade pip \
    && python -m pip install --no-cache /mnt/dist/*.whl

RUN adduser -D -h /app app && chown -R app.app /app
WORKDIR /app
USER app

CMD ["fastflow", "--namespace", "fastflow"]
