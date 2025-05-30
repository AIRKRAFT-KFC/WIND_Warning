#!/bin/bash

echo "🛫 Starting SWIFT Aircraft Monitor..."
echo "📡 Connecting to Kafka SWIFT topic"
echo ""

# Main (SWIFT Monitor) 실행
./gradlew run -PmainClass=org.example.payment_guard.Main
