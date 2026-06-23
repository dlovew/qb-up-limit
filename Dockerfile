FROM python:3.11-slim

ENV TZ=Asia/Shanghai
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    qbittorrent-api==2024.2.59 \
    flask==3.0.2 \
    flask-cors==4.0.0 \
    pyyaml==6.0.1 \
    requests==2.31.0 \
    waitress==3.0.0 \
    tzdata

COPY app/ /app/

RUN mkdir -p /data /config

EXPOSE 8765

CMD ["python", "main.py"]
