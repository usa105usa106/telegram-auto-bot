FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data /tmp/matplotlib

ENV PYTHONUNBUFFERED=1
ENV RAILWAY_VOLUME_MOUNT_PATH=/data
ENV MPLCONFIGDIR=/tmp/matplotlib

CMD ["python", "bot.py"]
