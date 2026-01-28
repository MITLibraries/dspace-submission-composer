FROM python:3.12-slim as build

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y git && \
    apt-get install -y curl && \
    apt-get install -y unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && ./aws/install 

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
ENV UV_SYSTEM_PYTHON=1
ENV PYTHONPATH=/app

WORKDIR /app

# Copy project metadata
COPY pyproject.toml ./

# Install package into system python
RUN uv pip install --system .

# Copy CLI application
COPY dsc ./dsc

ENTRYPOINT ["dsc"]