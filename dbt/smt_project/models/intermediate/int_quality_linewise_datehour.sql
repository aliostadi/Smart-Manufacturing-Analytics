{{ config(
    materialized = 'incremental',
    unique_key = 'date_hour',
    on_schema_change = 'sync_all_columns'
) }}

-- ==========================================================
-- Model: int_quality_linewise_datehour
-- Description: Aggregates linewise PCB pass/fail metrics and
-- First Pass Yield (FPY) from intermediate model and source.
-- ==========================================================

WITH 
linewise_quality AS (
   SELECT
    date_hour,part_number,

    -- ✅ Count PCBs that passed AOI2 (final check)
    SUM(pass_pcb) FILTER (WHERE station_number = '1008') AS pass_pcb_linewise,

    -- ✅ Count PCBs that failed in SPI or AOI2
    SUM(fail_pcb) FILTER (WHERE station_number IN ('1003', '1008')) AS scrap_pcb_linewise,

    -- ✅ Total PCBs = PASS in AOI2 + FAIL in SPI or AOI2
    SUM(pass_pcb) FILTER (WHERE station_number = '1008')
    + SUM(fail_pcb) FILTER (WHERE station_number IN ('1003', '1008')) AS total_pcb_linewise,

    -- ✅ Percentages (avoid division by zero)
    ROUND(
        100.0 * SUM(pass_pcb) FILTER (WHERE station_number = '1008')
        / NULLIF(
            SUM(pass_pcb) FILTER (WHERE station_number = '1008')
            + SUM(fail_pcb) FILTER (WHERE station_number IN ('1003', '1008')),
            0
        ),
    2) AS pass_pcb_linewise_percent,

    ROUND(
        100.0 * SUM(fail_pcb) FILTER (WHERE station_number IN ('1003', '1008'))
        / NULLIF(
            SUM(pass_pcb) FILTER (WHERE station_number = '1008')
            + SUM(fail_pcb) FILTER (WHERE station_number IN ('1003', '1008')),
            0
        ),
    2) AS scrap_pcb_linewise_percent

FROM {{ ref('int_quality_station_hour') }}
GROUP BY date_hour,part_number
),

pcb_fail_summary AS (
    SELECT
        serial_number,
        part_number,
        MAX(date_created) AS last_test_time,
        COUNT(*) FILTER (WHERE book_state = 'FAIL') AS fail_count
    FROM {{ source('public', 'station_records') }}
    {% if is_incremental() %}
        WHERE date_created > (SELECT MAX(date_hour) FROM {{ this }})
    {% endif %}
    GROUP BY serial_number,part_number
),

fpy_grouped AS (
    SELECT
        date_trunc('hour', last_test_time) AS date_hour,
        part_number,
        COUNT(*) FILTER (WHERE fail_count = 0) AS fpy_pcbs,       -- passed every test
        COUNT(*) FILTER (WHERE fail_count > 0) AS reworked_pcbs,  -- had at least one fail
        COUNT(*) AS total_pcbs
    FROM pcb_fail_summary
    GROUP BY date_trunc('hour', last_test_time),part_number
)

SELECT
    f.date_hour,
    f.fpy_pcbs,
    ROUND(100.0 * f.fpy_pcbs / NULLIF(q.total_pcb_linewise, 0), 2) AS fpy_percent,
	q.pass_pcb_linewise,
    q.part_number,
	q.scrap_pcb_linewise,
	q.total_pcb_linewise,
	q.pass_pcb_linewise_percent,
	q.scrap_pcb_linewise_percent
FROM fpy_grouped AS f
JOIN linewise_quality AS q ON f.date_hour = q.date_hour and f.part_number=q.part_number
ORDER BY date_hour
