package org.example.payment_guard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;

/**
 * 완전한 WeatherMonitor - 전체 공항 좌표 포함
 */
public class WeatherMonitorComplete {

    // API 설정
    private static final String BASE_URL = "https://aviationweather.gov/api/data/windtemp";
    private static final String REGION = "us";
    private static final String LEVEL = "low";
    private static final String FCST = "06";

    // PostgreSQL 설정
    private static final String JDBC_URL = "jdbc:postgresql://tgpostgresql.cr4guwc0um8m.ap-northeast-2.rds.amazonaws.com:5432/tgpostgreDB";
    private static final String JDBC_USER = "tgadmin";
    private static final String JDBC_PASS = "p12345678";

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // 요청 간격 (초) - 6시간 = 21600초
    private static final int FETCH_INTERVAL = 21600;

    // 내부 데이터 클래스
    static class WeatherStationData {
        String stationCode;
        String stationName;
        double latitude;
        double longitude;
        int[] windSpeeds = new int[9];

        WeatherStationData(String code) {
            this.stationCode = code;
        }
    }

    public static void main(String[] args) {
        System.out.println("🌤️  Aviation Weather Monitor Started (Complete Version)");
        System.out.println("🌐 API: " + BASE_URL);
        System.out.println("⏱️  Update Interval: " + (FETCH_INTERVAL / 3600) + " hours");
        System.out.println("💾 Database: " + JDBC_URL);
        System.out.println("═".repeat(80));

        initializeDatabase();
        fetchAndStoreWeatherData();

        scheduler.scheduleAtFixedRate(
            WeatherMonitorComplete::fetchAndStoreWeatherData,
            FETCH_INTERVAL,
            FETCH_INTERVAL,
            TimeUnit.SECONDS
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Shutting down Weather Monitor...");
            scheduler.shutdown();
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Monitor interrupted");
        }
    }

    private static void initializeDatabase() {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS)) {
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS weather_stations (
                    id SERIAL PRIMARY KEY,
                    station_code VARCHAR(10) NOT NULL,
                    station_name VARCHAR(200),
                    latitude DECIMAL(10, 6),
                    longitude DECIMAL(10, 6),
                    wind_3000ft INTEGER DEFAULT 0,
                    wind_6000ft INTEGER DEFAULT 0,
                    wind_9000ft INTEGER DEFAULT 0,
                    wind_12000ft INTEGER DEFAULT 0,
                    wind_18000ft INTEGER DEFAULT 0,
                    wind_24000ft INTEGER DEFAULT 0,
                    wind_30000ft INTEGER DEFAULT 0,
                    wind_34000ft INTEGER DEFAULT 0,
                    wind_39000ft INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(station_code)
                )
                """;
            
            try (PreparedStatement stmt = conn.prepareStatement(createTableSQL)) {
                stmt.execute();
                System.out.println("✅ Database table 'weather_stations' ready");
            }
        } catch (SQLException e) {
            System.err.println("❌ Database initialization failed: " + e.getMessage());
        }
    }

    private static void fetchAndStoreWeatherData() {
        try {
            String apiUrl = String.format("%s?region=%s&level=%s&fcst=%s", BASE_URL, REGION, LEVEL, FCST);
            String currentTime = TIME_FORMAT.format(Instant.now());
            System.out.println("\n🌡️  [" + currentTime + "] Fetching Weather Data...");

            String response = makeHttpRequest(apiUrl);
            
            if (response != null && !response.trim().isEmpty()) {
                List<WeatherStationData> stations = parseWeatherData(response);
                saveToDatabase(stations);
            } else {
                System.out.println("❌ No data received from API");
            }
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
        }
    }

    private static String makeHttpRequest(String urlString) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        try {
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "*/*");
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(15000);

            int responseCode = conn.getResponseCode();
            System.out.println("📡 HTTP Response Code: " + responseCode);

            if (responseCode == HttpURLConnection.HTTP_OK) {
                StringBuilder response = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line).append("\n");
                    }
                }
                return response.toString();
            } else {
                System.err.println("❌ HTTP Error: " + responseCode);
                return null;
            }
        } finally {
            conn.disconnect();
        }
    }

    private static List<WeatherStationData> parseWeatherData(String data) {
        List<WeatherStationData> stations = new ArrayList<>();
        String[] lines = data.split("\n");
        boolean isDataSection = false;
        
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) continue;
            
            if (line.startsWith("FT ")) {
                isDataSection = true;
                continue;
            }
            
            if (isDataSection && line.length() > 4) {
                WeatherStationData station = parseStationData(line);
                if (station != null) {
                    stations.add(station);
                }
            }
        }
        
        System.out.println("📊 Parsed " + stations.size() + " weather stations");
        return stations;
    }

    private static WeatherStationData parseStationData(String line) {
        try {
            String[] parts = line.split("\\s+");
            if (parts.length < 2) return null;
            
            WeatherStationData station = new WeatherStationData(parts[0]);
            setStationCoordinates(station);
            
            for (int i = 1; i <= 9 && i < parts.length; i++) {
                station.windSpeeds[i-1] = extractWindSpeed(parts[i]);
            }
            
            return station;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 전체 공항 좌표 데이터 설정
     */
    private static void setStationCoordinates(WeatherStationData station) {
        switch (station.stationCode) {
            // A
            case "ABI" -> { station.latitude = 32.4113; station.longitude = -99.6819; station.stationName = "Abilene Regional Airport"; }
            case "ABQ" -> { station.latitude = 35.0402; station.longitude = -106.6090; station.stationName = "Albuquerque International Sunport"; }
            case "ABR" -> { station.latitude = 45.4491; station.longitude = -98.4218; station.stationName = "Aberdeen Regional Airport"; }
            case "ACK" -> { station.latitude = 41.2531; station.longitude = -70.0601; station.stationName = "Nantucket Memorial Airport"; }
            case "ACY" -> { station.latitude = 39.4576; station.longitude = -74.5772; station.stationName = "Atlantic City International Airport"; }
            case "AGC" -> { station.latitude = 40.3544; station.longitude = -79.9302; station.stationName = "Allegheny County Airport"; }
            case "ALB" -> { station.latitude = 42.7483; station.longitude = -73.8017; station.stationName = "Albany International Airport"; }
            case "ALS" -> { station.latitude = 37.4349; station.longitude = -105.8667; station.stationName = "San Luis Valley Rgnl Airport (Alamosa)"; }
            case "AMA" -> { station.latitude = 35.2194; station.longitude = -101.7059; station.stationName = "Rick Husband Amarillo Intl Airport"; }
            case "AST" -> { station.latitude = 46.1580; station.longitude = -123.8817; station.stationName = "Astoria Regional Airport"; }
            case "ATL" -> { station.latitude = 33.6404; station.longitude = -84.4280; station.stationName = "Hartsfield-Jackson Atlanta Intl Airport"; }
            case "AVP" -> { station.latitude = 41.3385; station.longitude = -75.7233; station.stationName = "Wilkes-Barre / Scranton Intl Airport"; }
            case "AXN" -> { station.latitude = 45.8663; station.longitude = -95.3947; station.stationName = "Chandler Field (Alexandria Muni)"; }
            
            // B  
            case "BAM" -> { station.latitude = 40.6339; station.longitude = -116.9644; station.stationName = "Battle Mountain VORTAC"; }
            case "BCE" -> { station.latitude = 37.7064; station.longitude = -112.1459; station.stationName = "Bryce Canyon Airport"; }
            case "BDL" -> { station.latitude = 41.9389; station.longitude = -72.6832; station.stationName = "Bradley International Airport"; }
            case "BFF" -> { station.latitude = 41.8740; station.longitude = -103.5955; station.stationName = "Western Nebraska Regional Airport (Scottsbluff)"; }
            case "BGR" -> { station.latitude = 44.8074; station.longitude = -68.8281; station.stationName = "Bangor International Airport"; }
            case "BHM" -> { station.latitude = 33.5629; station.longitude = -86.7535; station.stationName = "Birmingham-Shuttlesworth Intl Airport"; }
            case "BIH" -> { station.latitude = 37.3730; station.longitude = -118.3636; station.stationName = "Eastern Sierra Regional Airport (Bishop)"; }
            case "BIL" -> { station.latitude = 45.8077; station.longitude = -108.5428; station.stationName = "Billings Logan International Airport"; }
            case "BLH" -> { station.latitude = 33.6192; station.longitude = -114.7169; station.stationName = "Blythe Airport"; }
            case "BML" -> { station.latitude = 44.5785; station.longitude = -71.1765; station.stationName = "Berlin Regional Airport"; }
            case "BNA" -> { station.latitude = 36.1245; station.longitude = -86.6782; station.stationName = "Nashville International Airport"; }
            case "BOI" -> { station.latitude = 43.5644; station.longitude = -116.2228; station.stationName = "Boise Airport / Gowen Field"; }
            case "BOS" -> { station.latitude = 42.3656; station.longitude = -71.0096; station.stationName = "Boston Logan International Airport"; }
            case "BRL" -> { station.latitude = 40.7832; station.longitude = -91.1255; station.stationName = "Southeast Iowa Regional Airport (Burlington)"; }
            case "BRO" -> { station.latitude = 25.9068; station.longitude = -97.4258; station.stationName = "Brownsville / South Padre Island Intl Airport"; }
            case "BUF" -> { station.latitude = 42.9405; station.longitude = -78.7322; station.stationName = "Buffalo Niagara International Airport"; }
            
            case "CAE" -> { station.latitude = 33.9388; station.longitude = -81.1195; station.stationName = "Columbia Metropolitan Airport"; }
            case "CAR" -> { station.latitude = 46.8715; station.longitude = -68.0179; station.stationName = "Caribou Municipal Airport"; }
            case "CGI" -> { station.latitude = 37.2253; station.longitude = -89.5708; station.stationName = "Cape Girardeau Regional Airport"; }
            case "CHS" -> { station.latitude = 32.8986; station.longitude = -80.0405; station.stationName = "Charleston Intl Airport / JB Charleston"; }
            case "CLE" -> { station.latitude = 41.4117; station.longitude = -81.8498; station.stationName = "Cleveland Hopkins International Airport"; }
            case "CLL" -> { station.latitude = 30.5886; station.longitude = -96.3638; station.stationName = "Easterwood Field (College Station)"; }
            case "CMH" -> { station.latitude = 39.9980; station.longitude = -82.8919; station.stationName = "John Glenn Columbus International Airport"; }
            case "COU" -> { station.latitude = 38.8181; station.longitude = -92.2196; station.stationName = "Columbia Regional Airport"; }
            case "CRP" -> { station.latitude = 27.7704; station.longitude = -97.5012; station.stationName = "Corpus Christi International Airport"; }
            case "CRW" -> { station.latitude = 38.3731; station.longitude = -81.5932; station.stationName = "Yeager Airport (Charleston)"; }
            case "CSG" -> { station.latitude = 32.5163; station.longitude = -84.9389; station.stationName = "Columbus Airport"; }
            case "CVG" -> { station.latitude = 39.0488; station.longitude = -84.6678; station.stationName = "Cincinnati / Northern Kentucky Intl Airport"; }
            case "CZI" -> { station.latitude = 44.0581; station.longitude = -106.4719; station.stationName = "Crazy Woman VOR-DME"; }

            case "DAL" -> { station.latitude = 32.8471; station.longitude = -96.8518; station.stationName = "Dallas Love Field"; }
            case "DBQ" -> { station.latitude = 42.4020; station.longitude = -90.7092; station.stationName = "Dubuque Regional Airport"; }
            case "DEN" -> { station.latitude = 39.8561; station.longitude = -104.6737; station.stationName = "Denver International Airport"; }
            case "DIK" -> { station.latitude = 46.7978; station.longitude = -102.8019; station.stationName = "Dickinson Theodore Roosevelt Rgnl Airport"; }
            case "DLH" -> { station.latitude = 46.8420; station.longitude = -92.1936; station.stationName = "Duluth International Airport"; }
            case "DLN" -> { station.latitude = 45.2555; station.longitude = -112.5533; station.stationName = "Dillon Airport"; }
            case "DRT" -> { station.latitude = 29.3742; station.longitude = -100.9277; station.stationName = "Del Rio International Airport"; }
            case "DSM" -> { station.latitude = 41.5340; station.longitude = -93.6631; station.stationName = "Des Moines International Airport"; }

            case "ECK" -> { station.latitude = 42.8681; station.longitude = -83.2275; station.stationName = "Peck VORTAC"; }
            case "EKN" -> { station.latitude = 38.8894; station.longitude = -79.8570; station.stationName = "Elkins-Randolph County Airport"; }
            case "ELP" -> { station.latitude = 31.8072; station.longitude = -106.3781; station.stationName = "El Paso International Airport"; }
            case "ELY" -> { station.latitude = 39.2997; station.longitude = -114.8419; station.stationName = "Ely Airport / Yelland Field"; }
            case "EMI" -> { station.latitude = 39.6792; station.longitude = -77.3267; station.stationName = "Westminster (Emmitsburg) VORTAC"; }
            case "EVV" -> { station.latitude = 38.0370; station.longitude = -87.5324; station.stationName = "Evansville Regional Airport"; }
            case "EYW" -> { station.latitude = 24.5561; station.longitude = -81.7595; station.stationName = "Key West International Airport"; }

            case "FAT" -> { station.latitude = 36.7762; station.longitude = -119.7181; station.stationName = "Fresno Yosemite International Airport"; }
            case "FLO" -> { station.latitude = 34.1854; station.longitude = -79.7244; station.stationName = "Florence Regional Airport"; }
            case "FMN" -> { station.latitude = 36.7413; station.longitude = -108.2298; station.stationName = "Four Corners Regional Airport"; }
            case "FOT" -> { station.latitude = 40.5539; station.longitude = -124.1133; station.stationName = "Fortuna VORTAC / Rohnerville Airport"; }
            case "FSD" -> { station.latitude = 43.5820; station.longitude = -96.7420; station.stationName = "Sioux Falls Regional Airport"; }
            case "FSM" -> { station.latitude = 35.3366; station.longitude = -94.3675; station.stationName = "Fort Smith Regional Airport"; }
            case "FWA" -> { station.latitude = 40.9785; station.longitude = -85.1951; station.stationName = "Fort Wayne International Airport"; }

            case "GAG" -> { station.latitude = 36.2953; station.longitude = -99.7756; station.stationName = "Gage VORTAC / Gage Airport"; }
            case "GCK" -> { station.latitude = 37.9275; station.longitude = -100.7244; station.stationName = "Garden City Regional Airport"; }
            case "GEG" -> { station.latitude = 47.6198; station.longitude = -117.5336; station.stationName = "Spokane International Airport"; }
            case "GFK" -> { station.latitude = 47.9493; station.longitude = -97.1761; station.stationName = "Grand Forks International Airport"; }
            case "GGW" -> { station.latitude = 48.2125; station.longitude = -106.6150; station.stationName = "Glasgow Wokal Field Airport"; }
            case "GJT" -> { station.latitude = 39.1224; station.longitude = -108.5267; station.stationName = "Grand Junction Regional Airport"; }
            case "GLD" -> { station.latitude = 39.3706; station.longitude = -101.6989; station.stationName = "Goodland Municipal Airport"; }
            case "GPI" -> { station.latitude = 48.3105; station.longitude = -114.2561; station.stationName = "Glacier Park International Airport"; }
            case "GRB" -> { station.latitude = 44.4851; station.longitude = -88.1296; station.stationName = "Green Bay Austin Straubel Intl Airport"; }
            case "GRI" -> { station.latitude = 40.9675; station.longitude = -98.3096; station.stationName = "Central Nebraska Regional Airport"; }
            case "GSP" -> { station.latitude = 34.8957; station.longitude = -82.2189; station.stationName = "Greenville-Spartanburg Intl Airport"; }
            case "GTF" -> { station.latitude = 47.4820; station.longitude = -111.3706; station.stationName = "Great Falls International Airport"; }

            case "H51" -> { station.latitude = 29.7604; station.longitude = -95.3698; station.stationName = "Private Heliport (TX Gulf Coast)"; }
            case "H52" -> { station.latitude = 30.0966; station.longitude = -95.6155; station.stationName = "Tomball Regional Medical Center Heliport"; }
            case "H61" -> { station.latitude = 29.7604; station.longitude = -94.7635; station.stationName = "Private Heliport (TX Gulf)"; }
            case "HAT" -> { station.latitude = 35.2324; station.longitude = -75.6182; station.stationName = "Cape Hatteras VOR-DME"; }
            case "HOU" -> { station.latitude = 29.6454; station.longitude = -95.2789; station.stationName = "William P. Hobby Airport"; }
            case "HSV" -> { station.latitude = 34.6372; station.longitude = -86.7751; station.stationName = "Huntsville International Airport"; }

            case "ICT" -> { station.latitude = 37.6499; station.longitude = -97.4331; station.stationName = "Wichita Dwight D. Eisenhower National Airport"; }
            case "ILM" -> { station.latitude = 34.2706; station.longitude = -77.9026; station.stationName = "Wilmington International Airport"; }
            case "IMB" -> { station.latitude = 45.4033; station.longitude = -117.8581; station.stationName = "Kimball VORTAC (Imbler)"; }
            case "IND" -> { station.latitude = 39.7173; station.longitude = -86.2944; station.stationName = "Indianapolis International Airport"; }
            case "INK" -> { station.latitude = 31.7796; station.longitude = -103.2016; station.stationName = "Wink-Loving County Airport"; }
            case "INL" -> { station.latitude = 48.5662; station.longitude = -93.4031; station.stationName = "Falls International Airport"; }

            case "JAN" -> { station.latitude = 32.3112; station.longitude = -90.0759; station.stationName = "Jackson–Medgar Evers International Airport"; }
            case "JAX" -> { station.latitude = 30.4941; station.longitude = -81.6879; station.stationName = "Jacksonville International Airport"; }
            case "JFK" -> { station.latitude = 40.6413; station.longitude = -73.7781; station.stationName = "John F. Kennedy International Airport"; }
            case "JOT" -> { station.latitude = 41.5178; station.longitude = -88.1753; station.stationName = "Joliet VORTAC / Joliet Regional Airport"; }

            case "LAS" -> { station.latitude = 36.0840; station.longitude = -115.1537; station.stationName = "Harry Reid International Airport"; }
            case "LBB" -> { station.latitude = 33.6636; station.longitude = -101.8227; station.stationName = "Lubbock Preston Smith International Airport"; }
            case "LCH" -> { station.latitude = 30.1261; station.longitude = -93.2234; station.stationName = "Lake Charles Regional Airport"; }
            case "LIT" -> { station.latitude = 34.7294; station.longitude = -92.2243; station.stationName = "Clinton National Airport"; }
            case "LKV" -> { station.latitude = 42.1612; station.longitude = -120.3980; station.stationName = "Lake County Airport"; }
            case "LND" -> { station.latitude = 42.8153; station.longitude = -108.7300; station.stationName = "Hunt Field (Lander)"; }
            case "LOU" -> { station.latitude = 38.2280; station.longitude = -85.6636; station.stationName = "Bowman Field (Louisville)"; }
            case "LRD" -> { station.latitude = 27.5438; station.longitude = -99.4616; station.stationName = "Laredo International Airport"; }
            case "LSE" -> { station.latitude = 43.8792; station.longitude = -91.2568; station.stationName = "La Crosse Regional Airport"; }
            case "LWS" -> { station.latitude = 46.3745; station.longitude = -117.0153; station.stationName = "Lewiston–Nez Perce County Airport"; }

            case "MBW" -> { station.latitude = -37.9756; station.longitude = 145.1022; station.stationName = "Moorabbin Airport (AU)"; }
            case "MCW" -> { station.latitude = 43.1578; station.longitude = -93.3308; station.stationName = "Mason City Municipal Airport"; }
            case "MEM" -> { station.latitude = 35.0424; station.longitude = -89.9767; station.stationName = "Memphis International Airport"; }
            case "MGM" -> { station.latitude = 32.3006; station.longitude = -86.3940; station.stationName = "Montgomery Regional Airport"; }
            case "MIA" -> { station.latitude = 25.7933; station.longitude = -80.2906; station.stationName = "Miami International Airport"; }
            case "MKC" -> { station.latitude = 39.1238; station.longitude = -94.5928; station.stationName = "Kansas City Downtown Airport"; }
            case "MKG" -> { station.latitude = 43.1695; station.longitude = -86.2382; station.stationName = "Muskegon County Airport"; }
            case "MLB" -> { station.latitude = 28.1028; station.longitude = -80.6453; station.stationName = "Orlando Melbourne International Airport"; }
            case "MLS" -> { station.latitude = 46.4280; station.longitude = -105.8864; station.stationName = "Miles City Airport"; }
            case "MOT" -> { station.latitude = 48.2594; station.longitude = -101.2803; station.stationName = "Minot International Airport"; }
            case "MQT" -> { station.latitude = 46.3536; station.longitude = -87.3951; station.stationName = "Sawyer International Airport"; }
            case "MRF" -> { station.latitude = 30.3717; station.longitude = -104.0178; station.stationName = "Marfa Municipal Airport"; }
            case "MSP" -> { station.latitude = 44.8848; station.longitude = -93.2223; station.stationName = "Minneapolis–Saint Paul International Airport"; }
            case "MSY" -> { station.latitude = 29.9934; station.longitude = -90.2581; station.stationName = "Louis Armstrong New Orleans Intl Airport"; }

            case "OKC" -> { station.latitude = 35.3931; station.longitude = -97.6007; station.stationName = "Will Rogers World Airport"; }
            case "OMA" -> { station.latitude = 41.3032; station.longitude = -95.8941; station.stationName = "Eppley Airfield"; }
            case "ONL" -> { station.latitude = 42.4701; station.longitude = -98.6890; station.stationName = "O'Neill Municipal Airport"; }
            case "ONT" -> { station.latitude = 34.0560; station.longitude = -117.6012; station.stationName = "Ontario International Airport"; }
            case "ORF" -> { station.latitude = 36.8946; station.longitude = -76.2012; station.stationName = "Norfolk International Airport"; }
            case "OTH" -> { station.latitude = 43.4171; station.longitude = -124.2460; station.stationName = "Southwest Oregon Regional Airport"; }

            case "PDX" -> { station.latitude = 45.5898; station.longitude = -122.5951; station.stationName = "Portland International Airport"; }
            case "PFN" -> { station.latitude = 30.2121; station.longitude = -85.6828; station.stationName = "Panama City Bay County Intl Airport (closed)"; }
            case "PHX" -> { station.latitude = 33.4343; station.longitude = -112.0116; station.stationName = "Phoenix Sky Harbor International Airport"; }
            case "PIE" -> { station.latitude = 27.9106; station.longitude = -82.6874; station.stationName = "St. Pete–Clearwater International Airport"; }
            case "PIH" -> { station.latitude = 42.9098; station.longitude = -112.5959; station.stationName = "Pocatello Regional Airport"; }
            case "PIR" -> { station.latitude = 44.3827; station.longitude = -100.2856; station.stationName = "Pierre Regional Airport"; }
            case "PLB" -> { station.latitude = 44.6509; station.longitude = -73.4681; station.stationName = "Plattsburgh VORTAC / Clinton County Airport"; }
            case "PRC" -> { station.latitude = 34.6547; station.longitude = -112.4196; station.stationName = "Prescott Regional Airport"; }
            case "PSB" -> { station.latitude = 40.8953; station.longitude = -78.0072; station.stationName = "Philipsburg VORTAC"; }
            case "PSX" -> { station.latitude = 28.7275; station.longitude = -96.2511; station.stationName = "Palacios Municipal Airport"; }
            case "PUB" -> { station.latitude = 38.2891; station.longitude = -104.4967; station.stationName = "Pueblo Memorial Airport"; }
            case "PWM" -> { station.latitude = 43.6462; station.longitude = -70.3093; station.stationName = "Portland Intl Jetport"; }

            case "RAP" -> { station.latitude = 44.0453; station.longitude = -103.0574; station.stationName = "Rapid City Regional Airport"; }
            case "RBL" -> { station.latitude = 40.1506; station.longitude = -122.2517; station.stationName = "Red Bluff VORTAC / Red Bluff Muni Airport"; }
            case "RDM" -> { station.latitude = 44.2541; station.longitude = -121.1500; station.stationName = "Redmond Municipal Airport"; }
            case "RDU" -> { station.latitude = 35.8776; station.longitude = -78.7875; station.stationName = "Raleigh–Durham International Airport"; }
            case "RIC" -> { station.latitude = 37.5052; station.longitude = -77.3197; station.stationName = "Richmond International Airport"; }
            case "RKS" -> { station.latitude = 41.5942; station.longitude = -109.0651; station.stationName = "Southwest Wyoming Regional Airport"; }
            case "RNO" -> { station.latitude = 39.4991; station.longitude = -119.7681; station.stationName = "Reno–Tahoe International Airport"; }
            case "ROA" -> { station.latitude = 37.3255; station.longitude = -79.9754; station.stationName = "Roanoke–Blacksburg Regional Airport"; }
            case "ROW" -> { station.latitude = 33.3016; station.longitude = -104.5306; station.stationName = "Roswell Air Center"; }

            case "SAC" -> { station.latitude = 38.5125; station.longitude = -121.4944; station.stationName = "Sacramento Executive Airport"; }
            case "SAN" -> { station.latitude = 32.7336; station.longitude = -117.1897; station.stationName = "San Diego International Airport"; }
            case "SAT" -> { station.latitude = 29.5337; station.longitude = -98.4698; station.stationName = "San Antonio International Airport"; }
            case "SAV" -> { station.latitude = 32.1276; station.longitude = -81.2021; station.stationName = "Savannah/Hilton Head International Airport"; }
            case "SBA" -> { station.latitude = 34.4261; station.longitude = -119.8403; station.stationName = "Santa Barbara Airport"; }
            case "SEA" -> { station.latitude = 47.4502; station.longitude = -122.3088; station.stationName = "Seattle–Tacoma International Airport"; }
            case "SFO" -> { station.latitude = 37.6213; station.longitude = -122.3790; station.stationName = "San Francisco International Airport"; }
            case "SGF" -> { station.latitude = 37.2454; station.longitude = -93.3886; station.stationName = "Springfield–Branson National Airport"; }
            case "SHV" -> { station.latitude = 32.4466; station.longitude = -93.8256; station.stationName = "Shreveport Regional Airport"; }
            case "SIY" -> { station.latitude = 41.7799; station.longitude = -122.4701; station.stationName = "Siskiyou County Airport"; }
            case "SLC" -> { station.latitude = 40.7899; station.longitude = -111.9791; station.stationName = "Salt Lake City International Airport"; }
            case "SLN" -> { station.latitude = 38.7910; station.longitude = -97.6522; station.stationName = "Salina Regional Airport"; }
            case "SPI" -> { station.latitude = 39.8441; station.longitude = -89.6779; station.stationName = "Abraham Lincoln Capital Airport"; }
            case "SPS" -> { station.latitude = 33.9888; station.longitude = -98.4919; station.stationName = "Sheppard AFB / Wichita Falls Muni Airport"; }
            case "SSM" -> { station.latitude = 46.4751; station.longitude = -84.3686; station.stationName = "Sault Ste. Marie VOR-DME"; }
            case "STL" -> { station.latitude = 38.7487; station.longitude = -90.3700; station.stationName = "St. Louis Lambert International Airport"; }
            case "SYR" -> { station.latitude = 43.1112; station.longitude = -76.1063; station.stationName = "Syracuse Hancock International Airport"; }

            case "T01" -> { station.latitude = 30.0717; station.longitude = -95.5533; station.stationName = "David Wayne Hooks Memorial Airport"; }
            case "T06" -> { station.latitude = 29.9736; station.longitude = -96.9700; station.stationName = "La Grange Municipal Airport"; }
            case "T07" -> { station.latitude = 33.1817; station.longitude = -102.1967; station.stationName = "Terry County Airport"; }
            case "TCC" -> { station.latitude = 35.1828; station.longitude = -103.6039; station.stationName = "Tucumcari Municipal Airport"; }
            case "TLH" -> { station.latitude = 30.3965; station.longitude = -84.3503; station.stationName = "Tallahassee International Airport"; }
            case "TRI" -> { station.latitude = 36.4752; station.longitude = -82.4074; station.stationName = "Tri-Cities Airport"; }
            case "TUL" -> { station.latitude = 36.1984; station.longitude = -95.8881; station.stationName = "Tulsa International Airport"; }
            case "TUS" -> { station.latitude = 32.1161; station.longitude = -110.9410; station.stationName = "Tucson International Airport"; }
            case "TVC" -> { station.latitude = 44.7414; station.longitude = -85.5822; station.stationName = "Traverse City Cherry Capital Airport"; }
            case "TYS" -> { station.latitude = 35.8110; station.longitude = -83.9940; station.stationName = "Knoxville McGhee Tyson Airport"; }

            case "WJF" -> { station.latitude = 34.7411; station.longitude = -118.2192; station.stationName = "General William J. Fox Field"; }
            case "YKM" -> { station.latitude = 46.5682; station.longitude = -120.5439; station.stationName = "Yakima Air Terminal"; }
            case "ZUN" -> { station.latitude = 35.0831; station.longitude = -108.7914; station.stationName = "Zuni VORTAC"; }

            case "2XG" -> { station.latitude = 38.4828; station.longitude = -94.3483; station.stationName = "Drexel Ranch Airpark"; }
            case "4J3" -> { station.latitude = 32.4870; station.longitude = -80.9506; station.stationName = "Ridgeland–Claude Dean Airport"; }

            default -> { 
                station.latitude = 0.0; 
                station.longitude = 0.0; 
                station.stationName = "Unknown Station"; 
            }
        }
    }

    private static void saveToDatabase(List<WeatherStationData> stations) {
        if (stations.isEmpty()) return;

        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS)) {
            String upsertSQL = """
                INSERT INTO weather_stations (
                    station_code, station_name, latitude, longitude,
                    wind_3000ft, wind_6000ft, wind_9000ft, wind_12000ft, wind_18000ft,
                    wind_24000ft, wind_30000ft, wind_34000ft, wind_39000ft,
                    last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (station_code) 
                DO UPDATE SET
                    station_name = EXCLUDED.station_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    wind_3000ft = EXCLUDED.wind_3000ft,
                    wind_6000ft = EXCLUDED.wind_6000ft,
                    wind_9000ft = EXCLUDED.wind_9000ft,
                    wind_12000ft = EXCLUDED.wind_12000ft,
                    wind_18000ft = EXCLUDED.wind_18000ft,
                    wind_24000ft = EXCLUDED.wind_24000ft,
                    wind_30000ft = EXCLUDED.wind_30000ft,
                    wind_34000ft = EXCLUDED.wind_34000ft,
                    wind_39000ft = EXCLUDED.wind_39000ft,
                    last_updated = CURRENT_TIMESTAMP
                """;

            try (PreparedStatement stmt = conn.prepareStatement(upsertSQL)) {
                for (WeatherStationData station : stations) {
                    stmt.setString(1, station.stationCode);
                    stmt.setString(2, station.stationName);
                    stmt.setDouble(3, station.latitude);
                    stmt.setDouble(4, station.longitude);
                    
                    // 9개 고도별 풍속 데이터
                    for (int i = 0; i < 9; i++) {
                        stmt.setInt(5 + i, station.windSpeeds[i]);
                    }
                    
                    stmt.addBatch();
                }
                
                int[] results = stmt.executeBatch();
                System.out.println("✅ Saved " + results.length + " weather stations to database");
                
                // 샘플 데이터 출력
                printSampleData(stations);
                
            }
        } catch (SQLException e) {
            System.err.println("❌ Database save failed: " + e.getMessage());
        }
    }

    private static void printSampleData(List<WeatherStationData> stations) {
        System.out.println("💨 Sample Wind Speed Data (first 5 stations):");
        System.out.println("   STN     3000  6000  9000 12000 18000 24000 30000 34000 39000");
        System.out.println("   " + "─".repeat(65));
        
        for (int i = 0; i < Math.min(5, stations.size()); i++) {
            WeatherStationData station = stations.get(i);
            System.out.printf("   %-4s ", station.stationCode);
            for (int wind : station.windSpeeds) {
                System.out.printf("%5d ", wind);
            }
            System.out.println();
        }
        System.out.println("   " + "─".repeat(65));
    }

    private static int extractWindSpeed(String data) {
        try {
            if (data == null || data.trim().isEmpty() || data.equals("9900")) {
                return 0;
            }
            
            if (data.length() < 4) {
                return 0;
            }
            
            String speedStr = data.substring(2, 4);
            return Integer.parseInt(speedStr);
            
        } catch (Exception e) {
            return 0;
        }
    }
}
