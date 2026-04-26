FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    redis==5.0.1 \
    psycopg2-binary==2.9.9
COPY feature_store.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "feature_store.py"]
