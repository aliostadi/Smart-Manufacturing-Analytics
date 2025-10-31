{{ config(
    materialized = 'incremental',
    unique_key = ['station_number', 'date_hour'],
    on_schema_change = 'sync_all_columns'
) }}

-- ==========================================================
-- Model: int_availability_station_hour
-- Description: Calculates hourly availability per station
--              by summing time differences between consecutive
--              records within each hour.
-- ==========================================================

WITH diffs AS (
    SELECT
        station_number,
        date_trunc('hour', date_created) AS date_hour,
        EXTRACT(EPOCH FROM (date_created - LAG(date_created) OVER (
            PARTITION BY station_number, date_trunc('hour', date_created)
            ORDER BY date_created
        ))) AS diff_seconds
    FROM {{ source('public', 'station_records') }}

    {% if is_incremental() %}
        -- Only process new records since last load
        WHERE date_created > (SELECT MAX(date_hour) FROM {{ this }})
    {% endif %}
)

SELECT
    station_number,
    date_hour,
    ROUND(SUM(diff_seconds), 2) AS uptime_seconds,
    ROUND(SUM(diff_seconds) / (60*60), 4) AS availability
FROM diffs
GROUP BY station_number, date_hour
ORDER BY date_hour, station_number
