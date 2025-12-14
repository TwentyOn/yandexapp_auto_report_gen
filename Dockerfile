FROM python:3.11-alpine AS builder
RUN apk add --no-cache build-base libpq git

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /app
ENV PYTHONPATH=/app
COPY . .
CMD ["python3", "-u", "main.py"]