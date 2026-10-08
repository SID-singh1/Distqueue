# One image for every role (worker, scheduler, monitor, producer); the role
# is chosen by the command, e.g. `docker run distqueue:local worker`.
FROM python:3.13-slim

# PYTHONUNBUFFERED: print() output reaches `docker logs` immediately instead
#   of sitting in a block buffer until the process exits (or never, on SIGKILL).
# PYTHONDONTWRITEBYTECODE: no .pyc clutter in the container filesystem.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Layer caching: install third-party dependencies in their own layer, keyed
# only on pyproject.toml.  A stub package satisfies setuptools' discovery.
# Editing distqueue/*.py then rebuilds only the cheap layer below, instead
# of re-downloading redis and prometheus_client on every code change.
COPY pyproject.toml README.md ./
RUN mkdir distqueue && touch distqueue/__init__.py \
    && pip install . \
    && rm -rf distqueue build *.egg-info

COPY distqueue ./distqueue
RUN pip install --no-deps .

# Don't run as root: a compromised handler shouldn't own the container.
RUN useradd --create-home --uid 10001 distqueue
USER distqueue

EXPOSE 9100
ENTRYPOINT ["distqueue"]
CMD ["--help"]
