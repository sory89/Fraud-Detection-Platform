FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    scikit-learn==1.4.0 \
    numpy==1.26.4 \
    joblib==1.3.2
COPY train.py scorer.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "train.py"]
