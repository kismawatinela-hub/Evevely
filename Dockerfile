FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt main.py bot.py global_sites.json ./
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/data

CMD ["python", "-u", "main.py"]
