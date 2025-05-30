package org.example.payment_guard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;

/**
 * SWIFT 항공기 데이터 실시간 모니터
 * -----------------------------------------
 * SWIFT 토픽에서 들어오는 항공기 TAIS 메시지를 실시간으로 터미널에 출력
 */
public class Main {

    // ---------- Kafka settings ----------
    private static final String BOOTSTRAP = "13.209.157.53:9092,15.164.111.153:9092,3.34.32.69:9092";
    private static final String TOPIC = "SWIFT";
    private static final String GROUP_ID = "swift-monitor-main-" + UUID.randomUUID();

    // ---------- PostgreSQL settings ----------
    private static final String JDBC_URL = "jdbc:postgresql://tgpostgresql.cr4guwc0um8m.ap-northeast-2.rds.amazonaws.com:5432/tgpostgreDB";
    private static final String JDBC_USER = "tgadmin";
    private static final String JDBC_PASS = "p12345678";

    // ---------- Wind Alert settings ----------
    private static final int WIND_ALERT_THRESHOLD = 20; // knots

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    // 풍속 데이터 클래스
    static class WindData {
        String stationCode;
        String stationName;
        double distance;
        int windSpeed;
        
        WindData(String stationCode, String stationName, double distance, int windSpeed) {
            this.stationCode = stationCode;
            this.stationName = stationName;
            this.distance = distance;
            this.windSpeed = windSpeed;
        }
    }

    public static void main(String[] args) {
        System.out.println("🛫 SWIFT Aircraft Data Monitor");
        System.out.println("📡 Kafka Cluster: " + BOOTSTRAP);
        System.out.println("📋 Topic: " + TOPIC);
        System.out.println("🔄 Group ID: " + GROUP_ID);
        System.out.println("═".repeat(80));

        // Kafka Consumer 구성
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            
            consumer.subscribe(Collections.singleton(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // 파티션 재할당시 정리 작업
                }
                
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // 최신 데이터부터 시작
                    consumer.seekToEnd(partitions);
                    System.out.println("✅ Connected to " + partitions.size() + " partition(s)");
                    partitions.forEach(tp -> {
                        long offset = consumer.position(tp);
                        System.out.printf("🔄 Starting at end offset %d for %s%n", offset, tp);
                    });
                    System.out.println("⏳ Waiting for aircraft messages...\n");
                }
            });

            long messageCount = 0;

            // 메인 수신 루프
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofSeconds(1));
                
                for (var record : records) {
                    try {
                        messageCount++;
                        String currentTime = TIME_FORMAT.format(Instant.now());
                        
                        // JSON 파싱 및 출력
                        JsonNode json = JSON_MAPPER.readTree(record.value());
                        printAircraftData(json, messageCount, currentTime);
                        
                        // 강풍 위험 감지
                        checkWindAlert(json, messageCount, currentTime);
                        
                    } catch (Exception e) {
                        System.err.println("❌ [ERROR] Failed to parse message #" + messageCount);
                        System.err.println("   Exception: " + e.getMessage());
                        System.err.println("   Raw data: " + record.value().substring(0, Math.min(200, record.value().length())) + "...");
                        System.out.println();
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("💥 [FATAL] Application error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 항공기 데이터를 구조화된 형태로 터미널에 출력
     */
    private static void printAircraftData(JsonNode json, long count, String time) {
        System.out.println("🛩️  [" + time + "] Message #" + count);
        
        // 메시지 기본 정보
        String messageType = json.path("messageType").asText("UNKNOWN");
        String source = json.path("source").asText("N/A");
        System.out.println("   📡 Type: " + messageType + " | Source: " + source);
        
        // 비행계획 정보
        JsonNode flightPlan = json.path("flightPlan");
        if (!flightPlan.isMissingNode()) {
            String callSign = flightPlan.path("callSign").asText("N/A");
            String aircraftType = flightPlan.path("aircraftType").asText("N/A");
            String flightRules = flightPlan.path("flightRules").asText("N/A");
            int assignedAlt = flightPlan.path("assignedAltitude").asInt(0);
            
            System.out.printf("   ✈️  %s (%s) | Rules: %s | Assigned Alt: %,d ft%n", 
                            callSign, aircraftType, flightRules, assignedAlt);
        }
        
        // 경로 정보
        JsonNode enhanced = json.path("enhanced");
        if (!enhanced.isMissingNode()) {
            String departure = enhanced.path("departureAirport").asText("N/A");
            String destination = enhanced.path("destinationAirport").asText("N/A");
            System.out.println("   🛫 " + departure + " ➜ " + destination);
        }
        
        // 실시간 추적 정보
        JsonNode track = json.path("track");
        if (!track.isMissingNode()) {
            double lat = track.path("latitude").asDouble(0.0);
            double lon = track.path("longitude").asDouble(0.0);
            int altitude = track.path("altitude").asInt(0);
            int verticalVel = track.path("verticalVelocity").asInt(0);
            String status = track.path("status").asText("N/A");
            String beacon = track.path("beaconCode").asText("N/A");
            String trackTime = track.path("time").asText("N/A");
            
            System.out.printf("   📍 Lat: %.5f, Lon: %.5f | Alt: %,d ft | V/S: %+d fpm%n", 
                            lat, lon, altitude, verticalVel);
            System.out.printf("   📊 Status: %s | Beacon: %s | Track Time: %s%n", 
                            status, beacon, trackTime);
        }
        
        // 레코드 정보
        JsonNode record = json.path("record");
        if (!record.isMissingNode()) {
            String seqNum = record.path("seqNum").asText("N/A");
            String type = record.path("type").asText("N/A");
            System.out.println("   🔢 Seq: " + seqNum + " | Record Type: " + type);
        }
        
        // 타임스탬프
        long timestamp = json.path("timestamp").asLong(0);
        if (timestamp > 0) {
            String msgTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                           .withZone(ZoneId.systemDefault())
                           .format(Instant.ofEpochMilli(timestamp));
            System.out.println("   ⏰ Message Time: " + msgTime);
        }
        
        System.out.println("   " + "─".repeat(60));
        System.out.println();
    }

    /**
     * 항공기 강풍 위험 감지
     */
    private static void checkWindAlert(JsonNode json, long messageCount, String currentTime) {
        try {
            // 항공기 위치 정보 추출
            JsonNode track = json.path("track");
            if (track.isMissingNode()) {
                return; // 위치 정보가 없으면 검사하지 않음
            }

            double latitude = track.path("latitude").asDouble(0.0);
            double longitude = track.path("longitude").asDouble(0.0);
            int altitude = track.path("altitude").asInt(0);

            // 유효한 위치 정보인지 확인
            if (latitude == 0.0 && longitude == 0.0) {
                return;
            }

            // 항공기 식별 정보
            String callSign = json.path("flightPlan").path("callSign").asText("UNKNOWN");
            
            // DB에서 해당 위치의 풍속 조회
            WindData windData = getWindSpeedAtLocation(latitude, longitude, altitude);
            
            // 강풍 알림 검사
            if (windData != null && windData.windSpeed >= WIND_ALERT_THRESHOLD) {
                printWindAlert(callSign, latitude, longitude, altitude, windData, messageCount, currentTime);
            }
            
        } catch (Exception e) {
            System.err.println("❌ [WIND CHECK ERROR] " + e.getMessage());
        }
    }

    /**
     * 특정 위치와 고도에서의 풍속 조회
     */
    private static WindData getWindSpeedAtLocation(double latitude, double longitude, int altitude) {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS)) {
            
            // 가장 가까운 기상 스테이션 찾기 (거리 계산)
            String nearestStationSQL = """
                SELECT station_code, station_name, latitude, longitude,
                       wind_3000ft, wind_6000ft, wind_9000ft, wind_12000ft, wind_18000ft,
                       wind_24000ft, wind_30000ft, wind_34000ft, wind_39000ft,
                       SQRT(POW(69.1 * (latitude - ?), 2) + POW(69.1 * (? - longitude) * COS(latitude / 57.3), 2)) AS distance
                FROM weather_stations 
                WHERE latitude != 0 AND longitude != 0
                ORDER BY distance
                LIMIT 1
                """;
            
            try (PreparedStatement stmt = conn.prepareStatement(nearestStationSQL)) {
                stmt.setDouble(1, latitude);
                stmt.setDouble(2, longitude);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String stationCode = rs.getString("station_code");
                        String stationName = rs.getString("station_name");
                        double distance = rs.getDouble("distance");
                        
                        // 고도에 따른 풍속 계산
                        int windSpeed = calculateWindSpeedByAltitude(rs, altitude);
                        
                        return new WindData(stationCode, stationName, distance, windSpeed);
                    }
                }
            }
            
        } catch (SQLException e) {
            System.err.println("❌ [DB ERROR] " + e.getMessage());
        }
        
        return null; // 데이터 없으면 null 반환
    }

    /**
     * 고도에 따른 풍속 계산 (가장 가까운 고도의 풍속 사용)
     */
    private static int calculateWindSpeedByAltitude(ResultSet rs, int altitude) throws SQLException {
        // 고도별 풍속 데이터 (feet)
        int[] altitudes = {3000, 6000, 9000, 12000, 18000, 24000, 30000, 34000, 39000};
        int[] windSpeeds = {
            rs.getInt("wind_3000ft"), rs.getInt("wind_6000ft"), rs.getInt("wind_9000ft"),
            rs.getInt("wind_12000ft"), rs.getInt("wind_18000ft"), rs.getInt("wind_24000ft"),
            rs.getInt("wind_30000ft"), rs.getInt("wind_34000ft"), rs.getInt("wind_39000ft")
        };
        
        // 가장 가까운 고도 찾기
        int closestIndex = 0;
        int minDifference = Math.abs(altitude - altitudes[0]);
        
        for (int i = 1; i < altitudes.length; i++) {
            int difference = Math.abs(altitude - altitudes[i]);
            if (difference < minDifference) {
                minDifference = difference;
                closestIndex = i;
            }
        }
        
        return windSpeeds[closestIndex];
    }

    /**
     * 강풍 알림 출력
     */
    private static void printWindAlert(String callSign, double latitude, double longitude, 
                                      int altitude, WindData windData, long messageCount, String currentTime) {
        System.out.println();
        System.out.println("🚨🚨🚨 WIND ALERT 🚨🚨🚨");
        System.out.println("⚠️  [" + currentTime + "] HIGH WIND WARNING - Message #" + messageCount);
        System.out.printf("✈️  Aircraft: %s%n", callSign);
        System.out.printf("📍 Position: %.5f°N, %.5f°W%n", latitude, Math.abs(longitude));
        System.out.printf("🔺 Altitude: %,d ft%n", altitude);
        System.out.printf("🏢 Nearest Station: %s (%s) - %.1f miles away%n", 
                         windData.stationCode, windData.stationName, windData.distance);
        System.out.printf("💨 Wind Speed: %d knots (Threshold: %d knots)%n", 
                         windData.windSpeed, WIND_ALERT_THRESHOLD);
        System.out.println("⚠️  CAUTION: Aircraft flying in high wind conditions!");
        System.out.println("🚨" + "=".repeat(60) + "🚨");
        System.out.println();
    }
}
