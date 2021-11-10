# syntax=docker/dockerfile:1

FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .

RUN \
    apt-get update && \
    apt-get -y install libpq-dev gcc && \
    pip3 install -r requirements.txt

COPY . .

CMD ["python3", "app.py"]