{{ config(
    materialized = 'incremental',
    unique_key = ['station_number', 'date_hour'],
    on_schema_change = 'sync_all_columns'
) }}

WITH base AS (
    SELECT
        station_number,
        serial_number,
        book_state,
        part_number,
        test_count,
        date_created,
        date_trunc('hour', date_created) AS date_hour,
        MAX(test_count) OVER (
            PARTITION BY station_number, serial_number
        ) AS max_test_count
    FROM {{ source('public', 'station_records') }}
    
    {% if is_incremental() %}
        -- 🔹 Only load new records since last loaded timestamp
        WHERE date_created > (SELECT MAX(date_created) FROM {{ this }})
    {% endif %}
),

last_test_result AS (
    SELECT *
    FROM base
    WHERE test_count = max_test_count
),

pcbs AS (
    SELECT
        station_number,
        part_number,
        date_hour,
        COUNT(DISTINCT serial_number) AS total_pcb,
        COUNT(DISTINCT serial_number) FILTER (WHERE book_state = 'FAIL') AS fail_pcb,
        COUNT(DISTINCT serial_number) FILTER (WHERE book_state = 'PASS') AS pass_pcb
    FROM last_test_result
    GROUP BY station_number, date_hour,part_number
),

bookings AS (
    SELECT
        station_number,
        date_trunc('hour', date_created) AS date_hour,
        part_number,
        COUNT(serial_number) AS total_bookings,
        COUNT(serial_number) FILTER (WHERE book_state = 'FAIL') AS fail_bookings,
        COUNT(serial_number) FILTER (WHERE book_state = 'PASS') AS pass_bookings
    FROM {{ source('public', 'station_records') }}
    
    {% if is_incremental() %}
        -- 🔹 Apply same filter for incremental logic
        WHERE date_created > (SELECT MAX(date_created) FROM {{ this }})
    {% endif %}
    
    GROUP BY station_number, date_trunc('hour', date_created),part_number
)

SELECT
    b.station_number,
    b.part_number,
    b.date_hour,
    b.total_bookings,
    b.fail_bookings,
    b.pass_bookings,
    COALESCE(p.total_pcb, 0) AS total_pcb,
    COALESCE(p.fail_pcb, 0) AS fail_pcb,
    COALESCE(p.pass_pcb, 0) AS pass_pcb
FROM bookings b
LEFT JOIN pcbs p
  ON b.station_number = p.station_number
 AND b.date_hour = p.date_hour AND b.part_number=p.part_number
ORDER BY b.station_number, b.date_hour , b.part_number
