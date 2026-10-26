FROM python:3.12-slim

# Debian's chromium + matching chromedriver (amd64 and arm64), so no Chrome download at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends chromium chromium-driver tzdata fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Finvasia login disabled for now - add it back to BROKERS here (or set BROKERS in .env) to re-enable.
ENV TZ=Asia/Kolkata \
    PYTHONUNBUFFERED=1 \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER=/usr/bin/chromedriver \
    BROKERS=zerodha,aliceblue

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY auto_login.py .

# Lambda entry point (awslambdaric = AWS's runtime client, lets a non-AWS base image run as a Lambda).
# docker-compose.yml overrides this to run the script directly for local runs.
ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["auto_login.lambda_handler"]
