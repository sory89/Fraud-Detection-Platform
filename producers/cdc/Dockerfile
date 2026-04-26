FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2
COPY cdc_producer.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "cdc_producer.py"]
