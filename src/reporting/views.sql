-- ============================================================
-- Reporting layer (BI) — rpt schema.
-- Stable, denormalised views over dwh.* that back the business
-- reports / figures. dim_location is SCD2, so every join uses the
-- live version (location_id + is_current).
-- Idempotent: CREATE OR REPLACE.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS rpt;

-- Trips enriched with the hourly weather observation + live zone attributes.
-- fact_trip is minute-grain; weather is hourly, so the trip time_key is
-- truncated to the hour ((time_key / 10000) * 10000) for the join.
CREATE OR REPLACE VIEW rpt.v_trip_enriched AS
SELECT
    f.date_key,
    d.full_date,
    d.year,
    d.month,
    t.hour,
    t.time_of_day,
    pu.borough        AS pickup_borough,
    pu.zone           AS pickup_zone,
    pu.service_zone   AS pickup_service_zone,
    f.trip_distance,
    f.fare_amount,
    f.tip_amount,
    f.total_amount,
    f.trip_duration_sec,
    f.passenger_count,
    w.temperature,
    w.precipitation,
    w.wind_speed,
    wt.condition_name AS weather_condition
FROM dwh.fact_trip f
JOIN dwh.dim_date d ON f.date_key = d.date_key
JOIN dwh.dim_time t ON f.time_key = t.time_key
LEFT JOIN dwh.dim_location pu
       ON f.pu_location_key = pu.location_id AND pu.is_current
LEFT JOIN dwh.fact_weather w
       ON f.date_key = w.date_key
      AND (f.time_key / 10000) * 10000 = w.time_key
LEFT JOIN dwh.dim_weather_type wt
       ON w.weather_type_key = wt.weather_type_key;

-- Report 1 — precipitation impact on trip volume (per day).
CREATE OR REPLACE VIEW rpt.v_precip_trips AS
SELECT
    d.full_date,
    COALESCE(wd.precipitation, 0)                       AS daily_precipitation,
    CASE
        WHEN COALESCE(wd.precipitation, 0) = 0   THEN 'Dry'
        WHEN COALESCE(wd.precipitation, 0) < 5   THEN 'Light'
        WHEN COALESCE(wd.precipitation, 0) < 20  THEN 'Moderate'
        ELSE 'Heavy'
    END                                                 AS precip_band,
    COUNT(f.date_key)                                   AS trip_count
FROM dwh.dim_date d
LEFT JOIN dwh.fact_trip f ON f.date_key = d.date_key
LEFT JOIN (
    SELECT date_key, SUM(precipitation) AS precipitation
    FROM dwh.fact_weather GROUP BY date_key
) wd ON wd.date_key = d.date_key
GROUP BY d.full_date, wd.precipitation;

-- Report 2 — average temperature vs average trip duration.
CREATE OR REPLACE VIEW rpt.v_temp_duration AS
SELECT
    width_bucket(temperature, -10, 40, 10) AS temp_bucket,
    time_of_day,
    COUNT(*)                               AS trip_count,
    AVG(trip_duration_sec) / 60.0          AS avg_duration_min,
    AVG(temperature)                       AS avg_temperature
FROM rpt.v_trip_enriched
WHERE temperature IS NOT NULL
GROUP BY width_bucket(temperature, -10, 40, 10), time_of_day;

-- Report 3 — monthly trip trend.
CREATE OR REPLACE VIEW rpt.v_monthly_trips AS
SELECT
    year,
    month,
    COUNT(*)            AS trip_count,
    SUM(total_amount)   AS total_revenue,
    AVG(trip_distance)  AS avg_distance
FROM rpt.v_trip_enriched
GROUP BY year, month
ORDER BY year, month;

-- Report 4 — daily operational KPIs + per-borough split for the zone map.
CREATE OR REPLACE VIEW rpt.v_daily_kpi AS
SELECT
    full_date,
    pickup_borough,
    COUNT(*)                       AS trip_count,
    SUM(total_amount)              AS total_revenue,
    AVG(trip_distance)             AS avg_distance,
    AVG(trip_duration_sec) / 60.0  AS avg_duration_min,
    AVG(tip_amount)                AS avg_tip
FROM rpt.v_trip_enriched
GROUP BY full_date, pickup_borough;

-- Report 5 — seasonal comparison (meteorological seasons).
CREATE OR REPLACE VIEW rpt.v_seasonal AS
SELECT
    CASE
        WHEN month IN (12, 1, 2)  THEN 'Winter'
        WHEN month IN (3, 4, 5)   THEN 'Spring'
        WHEN month IN (6, 7, 8)   THEN 'Summer'
        ELSE 'Autumn'
    END                            AS season,
    COUNT(*)                       AS trip_count,
    AVG(total_amount)              AS avg_fare,
    AVG(trip_distance)             AS avg_distance,
    AVG(temperature)               AS avg_temperature
FROM rpt.v_trip_enriched
GROUP BY 1;

-- Report 6 — hourly weather report.
CREATE OR REPLACE VIEW rpt.v_hourly_weather AS
SELECT
    d.full_date,
    t.hour,
    w.temperature,
    w.precipitation,
    w.wind_speed,
    wt.condition_name
FROM dwh.fact_weather w
JOIN dwh.dim_date d ON w.date_key = d.date_key
JOIN dwh.dim_time t ON w.time_key = t.time_key
JOIN dwh.dim_weather_type wt ON w.weather_type_key = wt.weather_type_key
ORDER BY d.full_date, t.hour;
