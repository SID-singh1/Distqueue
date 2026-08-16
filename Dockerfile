FROM python:3.13-slim

WORKDIR /app

COPY . /app

RUN pip install .

# No CMD is specified here because the same image is used for the worker,
# scheduler, and monitor roles. The specific command to run is provided 
# per-service in docker-compose.yml.
