#!/bin/bash

TOPICS=("aapl" "amzn" "googl" "nvda" "tsla" "msft" "sma" "ema" "macd" "rsi", "websocket")

# Deletes:
for topic in "${TOPICS[@]}"; do
    docker exec -t kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$topic"
done