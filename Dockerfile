FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends git curl unzip && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://github.com/duckdb/duckdb/releases/download/v0.10.3/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip \
    && unzip /tmp/duckdb.zip -d /usr/local/bin/ \
    && rm /tmp/duckdb.zip
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
