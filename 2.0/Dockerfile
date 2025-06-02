FROM python:3.10-slim AS builder

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    cron \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code
COPY --from=builder /root/.local /root/.local
COPY . .

# Make all scripts in init_scripts executable
RUN chmod -R +x /code/init_scripts

# Configure cron
RUN chmod 0644 /code/crontab && \
    crontab /code/crontab && \
    touch /var/log/cron.log

# Set environment
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/code 
