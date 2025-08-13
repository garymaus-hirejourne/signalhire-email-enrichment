
FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
ENV SIGNALHIRE_RESULTS_CSV=/data/results.csv
RUN mkdir -p /data

EXPOSE 8080
CMD ["/bin/sh","-c","gunicorn webhook_server:app --bind 0.0.0.0:${PORT}"]
