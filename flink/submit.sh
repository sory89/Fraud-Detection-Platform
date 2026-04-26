#!/bin/bash
set -e
echo "==> Checking JARs in /opt/flink/lib/"
ls /opt/flink/lib/*.jar | grep -E "kafka|postgresql"
echo "==> Waiting for Kafka…"
sleep 20
echo "==> Starting Flink fraud detection pipeline…"
python3 /opt/flink-app/fraud_pipeline.py
