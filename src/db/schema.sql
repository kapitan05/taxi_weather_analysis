-- ============================================================
-- NYC Weather vs Taxi Trips — Data Warehouse Schema
-- Fact constellation: shared dims (date, time), two fact tables.
-- Idempotent: safe to re-run (applied by the db-init service).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- ------------------------------------------------------------
-- STAGING — raw landing zone written by PySpark JDBC.
-- Column types match the Parquet / API source exactly.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging.fact_trip (
    vendor_id                 INTEGER,
    tpep_pickup_datetime      TIMESTAMP,
    tpep_dropoff_datetime     TIMESTAMP,
    passenger_count           REAL,
    trip_distance             REAL,
    rate_code_id              REAL,
    store_and_fwd_flag        TEXT,
    pu_location_id            INTEGER,
    do_location_id            INTEGER,
    payment_type              INTEGER,
    fare_amount               REAL,
    extra                     REAL,
    mta_tax                   REAL,
    tip_amount                REAL,
    tolls_amount              REAL,
    improvement_surcharge     REAL,
    total_amount              REAL,
    congestion_surcharge      REAL,
    airport_fee               REAL,
    ingested_at               TIMESTAMP DEFAULT NOW()
);

-- Open-Meteo 15-minute observations (aggregated to hours in dwh.fact_weather)
CREATE TABLE IF NOT EXISTS staging.fact_weather (
    time                      TIMESTAMP,
    temperature_2m            REAL,
    precipitation             REAL,
    weathercode               INTEGER,
    windspeed_10m             REAL,
    ingested_at               TIMESTAMP DEFAULT NOW()
);

-- TLC taxi zone lookup (feeds dwh.dim_location)
CREATE TABLE IF NOT EXISTS staging.zone_lookup (
    location_id               INTEGER,
    borough                   TEXT,
    zone                      TEXT,
    service_zone              TEXT,
    ingested_at               TIMESTAMP DEFAULT NOW()
);

-- ------------------------------------------------------------
-- DWH — fact constellation consumed by Tableau.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        BIGINT  PRIMARY KEY,            -- YYYYMMDD
    full_date       DATE    NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day             INTEGER NOT NULL,
    holiday_flag    BOOLEAN NOT NULL DEFAULT FALSE,
    holiday_name    VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dwh.dim_time (
    time_key        BIGINT      PRIMARY KEY,        -- HHMMSS (minute grain, SS=00)
    hour            BIGINT      NOT NULL,
    minute          SMALLINT    NOT NULL,
    time_of_day     VARCHAR(20) NOT NULL            -- Night / Morning / Afternoon / Evening
);

-- SCD2 (type 2) dimension: one row per version of a taxi zone.
-- location_sk = surrogate (per-version identity); location_id = TLC business key
-- (stable across versions). service_zone may change over time -> new version.
CREATE TABLE IF NOT EXISTS dwh.dim_location (
    location_sk     BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id     INTEGER      NOT NULL,           -- business / natural key (TLC LocationID)
    borough         VARCHAR(60)  NOT NULL,
    zone            VARCHAR(100) NOT NULL,
    service_zone    VARCHAR(60)  NOT NULL,
    valid_from      DATE         NOT NULL DEFAULT DATE '1900-01-01',
    valid_to        DATE         NOT NULL DEFAULT DATE '9999-12-31',
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    version         INTEGER      NOT NULL DEFAULT 1,
    UNIQUE (location_id, valid_from)
);

-- Exactly one current row per business key.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_location_current
    ON dwh.dim_location (location_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_dim_location_id
    ON dwh.dim_location (location_id, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS dwh.dim_weather_type (
    weather_type_key  BIGINT       PRIMARY KEY,     -- = WMO weather code
    condition_name    VARCHAR(100) NOT NULL,
    description       VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS dwh.fact_trip (
    date_key            BIGINT          NOT NULL REFERENCES dwh.dim_date (date_key),
    time_key            BIGINT          NOT NULL REFERENCES dwh.dim_time (time_key),
    -- Holds the TLC LocationID business key. No FK: dim_location is SCD2 and
    -- location_id is not unique there. Resolve to the live version via
    -- (pu_location_key = dim_location.location_id AND dim_location.is_current).
    -- Referential integrity is enforced by src/quality/checks.py.
    pu_location_key     BIGINT,
    do_location_key     BIGINT,
    trip_distance       NUMERIC(8,2)    NOT NULL,
    fare_amount         NUMERIC(19,4)   NOT NULL,
    tip_amount          NUMERIC(19,4)   NOT NULL,
    total_amount        NUMERIC(19,4)   NOT NULL,
    trip_duration_sec   INTEGER         NOT NULL,
    passenger_count     INTEGER,
    loaded_at           TIMESTAMP       DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.fact_weather (
    date_key            BIGINT          NOT NULL REFERENCES dwh.dim_date (date_key),
    time_key            BIGINT          NOT NULL REFERENCES dwh.dim_time (time_key),   -- hourly grain (MMSS=0000)
    weather_type_key    BIGINT          NOT NULL REFERENCES dwh.dim_weather_type (weather_type_key),
    temperature         NUMERIC(5,2)    NOT NULL,
    precipitation       NUMERIC(6,2)    NOT NULL,
    wind_speed          NUMERIC(5,2)    NOT NULL,
    loaded_at           TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (date_key, time_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_trip_date     ON dwh.fact_trip (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_trip_time     ON dwh.fact_trip (time_key);
CREATE INDEX IF NOT EXISTS idx_fact_trip_pu_loc   ON dwh.fact_trip (pu_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_trip_do_loc   ON dwh.fact_trip (do_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_weather_date  ON dwh.fact_weather (date_key);

-- ------------------------------------------------------------
-- Static dimension seeds
-- ------------------------------------------------------------

-- dim_time: every minute of the day
INSERT INTO dwh.dim_time (time_key, hour, minute, time_of_day)
SELECT
    h * 10000 + m * 100,
    h,
    m,
    CASE
        WHEN h BETWEEN 0  AND 5  THEN 'Night'
        WHEN h BETWEEN 6  AND 11 THEN 'Morning'
        WHEN h BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END
FROM generate_series(0, 23) AS h, generate_series(0, 59) AS m
ON CONFLICT (time_key) DO NOTHING;

-- dim_weather_type: WMO weather interpretation codes (Open-Meteo weathercode)
INSERT INTO dwh.dim_weather_type (weather_type_key, condition_name, description) VALUES
    (0,  'Clear',        'Clear sky'),
    (1,  'Mainly Clear', 'Mainly clear'),
    (2,  'Partly Cloudy','Partly cloudy'),
    (3,  'Overcast',     'Overcast'),
    (45, 'Fog',          'Fog'),
    (48, 'Fog',          'Depositing rime fog'),
    (51, 'Drizzle',      'Light drizzle'),
    (53, 'Drizzle',      'Moderate drizzle'),
    (55, 'Drizzle',      'Dense drizzle'),
    (56, 'Drizzle',      'Light freezing drizzle'),
    (57, 'Drizzle',      'Dense freezing drizzle'),
    (61, 'Rain',         'Slight rain'),
    (63, 'Rain',         'Moderate rain'),
    (65, 'Rain',         'Heavy rain'),
    (66, 'Rain',         'Light freezing rain'),
    (67, 'Rain',         'Heavy freezing rain'),
    (71, 'Snow',         'Slight snowfall'),
    (73, 'Snow',         'Moderate snowfall'),
    (75, 'Snow',         'Heavy snowfall'),
    (77, 'Snow',         'Snow grains'),
    (80, 'Rain Showers', 'Slight rain showers'),
    (81, 'Rain Showers', 'Moderate rain showers'),
    (82, 'Rain Showers', 'Violent rain showers'),
    (85, 'Snow Showers', 'Slight snow showers'),
    (86, 'Snow Showers', 'Heavy snow showers'),
    (95, 'Thunderstorm', 'Thunderstorm'),
    (96, 'Thunderstorm', 'Thunderstorm with slight hail'),
    (99, 'Thunderstorm', 'Thunderstorm with heavy hail')
ON CONFLICT (weather_type_key) DO NOTHING;
