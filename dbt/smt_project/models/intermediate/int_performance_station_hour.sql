{{ config(
    materialized = 'incremental',
    unique_key = ['station_number', 'date_hour', 'part_number'],
    on_schema_change = 'sync_all_columns'
) }}

-- ==========================================================
-- Model: int_performance_station_hour
-- Description: Calculates hourly performance per station and part
--              by comparing actual vs. ideal cycle times.
-- ==========================================================

-- 1️⃣ Aggregate actual cycle times
WITH station_actCT1 AS (
    SELECT  
        station_number, 
        date_trunc('hour', date_created) AS date_hour,
        part_number,
        ROUND(SUM(cycle_time), 2) AS sum_cycle_time,
        ROUND(AVG(cycle_time), 2) AS avg_cycle_time,
        COUNT(serial_number) AS count_bookings
    FROM {{ source('public', 'station_records') }}
    
    {% if is_incremental() %}
        -- Only load new data since last update
        WHERE date_created > (SELECT MAX(date_hour) FROM {{ this }})
    {% endif %}
    
    GROUP BY station_number, date_trunc('hour', date_created), part_number
),

-- 2️⃣ Join with ideal cycle times
station_actCT AS (
    SELECT
        a.station_number,
        a.date_hour,
        a.part_number,
        a.sum_cycle_time,
        a.avg_cycle_time,
        a.count_bookings,
        i.ideal_cycle_time
    FROM station_actCT1 AS a
    LEFT JOIN {{ source('public', 'ideal_cycle_times') }} AS i 
      ON a.station_number = i.station_number
)

-- 3️⃣ Calculate performance KPI
SELECT 
    station_number,
    date_hour,
    part_number,
    avg_cycle_time, 
    ideal_cycle_time,
    count_bookings,
    ROUND(
        (ideal_cycle_time * count_bookings) / NULLIF(sum_cycle_time, 0) * 100,
        2
    ) AS performance_percent
FROM station_actCT
ORDER BY date_hour, station_number, part_number
