FROM python:3.13.3-slim

# Install PostgreSQL client dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy remaining files
COPY ./main.py ./main.py
COPY ./gatherer ./gatherer

# Environment variables
ENV DATABASE_CONNECTION_URL=""
ENV MAX_THREADS=4
ENV WEATHERLINK_V1_ENDPOINT="https://api.weatherlink.com/v1/NoaaExt.json"
ENV WEATHERLINK_V2_ENDPOINT="https://api.weatherlink.com/v2/{mode}/{station_id}"
ENV HOLFUY_LIVE_ENDPOINT="http://api.holfuy.com/live/"
ENV HOLFUY_HISTORIC_ENDPOINT="http://api.holfuy.com/archive/"
ENV THINGSPEAK_ENDPOINT="https://api.thingspeak.com/channels"
ENV WUNDERGROUND_ENDPOINT="https://api.weather.com/v2/pws/observations/current"
ENV WUNDERGROUND_DAILY_ENDPOINT="https://api.weather.com/v2/pws/dailysummary/7day"
ENV ECOWITT_ENDPOINT="https://api.ecowitt.net/api/v3/device/real_time"
ENV ECOWITT_DAILY_ENDPOINT="https://api.ecowitt.net/api/v3/device/history"
ENV GOVEE_ENDPOINT="https://openapi.api.govee.com/router/api/v1/device/state"

ENV SENCROP_ENDPOINT="https://api.sencrop.com/v1"
ENV SENCROP_PARTNER_ID=""
ENV SENCROP_APPLICATION_ID=""
ENV SENCROP_APPLICATION_SECRET=""

# Set entrypoint
ENTRYPOINT ["python", "main.py", "--all"]