# Amplify -> Orion Eclipse migration pipeline -- local prototype UI
#
# Build:  docker compose up --build
# Then open:  http://localhost:8501
#
# Everything the app writes (output/, audit/, cutover/, quarantine/,
# core_models_registry.json) is bind-mounted by docker-compose.yml so your
# results persist on the host and survive container rebuilds.

FROM python:3.11-slim

WORKDIR /app

# System deps kept minimal -- pandas/openpyxl wheels cover everything needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Make sure the folders the app writes to exist even on a first-ever run
# (docker-compose's bind mounts create these on the host, but this covers
# `docker run` without compose too).
RUN mkdir -p output audit cutover quarantine

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
