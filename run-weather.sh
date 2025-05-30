#!/bin/bash

echo "🌤️  Starting Aviation Weather Monitor..."
echo "📡 Fetching data from aviationweather.gov API"
echo ""

# WeatherMonitorComplete 실행
./gradlew run -PmainClass=org.example.payment_guard.WeatherMonitorComplete
