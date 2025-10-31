




CREATE MATERIALIZED VIEW public.quality_linewise_datehour AS
with 
linewise_quality  as(
   SELECT
    date_hour,

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

FROM public.quality_station_hour
GROUP BY date_hour) ,

pcb_fail_summary AS (
    SELECT
        serial_number,
        MAX(date_created) AS last_test_time,
        COUNT(*) FILTER (WHERE book_state = 'FAIL') AS fail_count
    FROM public.station_records
    GROUP BY serial_number
),

fpy_grouped AS (
    SELECT
        date_trunc('hour', last_test_time) AS date_hour,
        COUNT(*) FILTER (WHERE fail_count = 0) AS fpy_pcbs,   -- passed every test
        COUNT(*) FILTER (WHERE fail_count > 0) AS reworked_pcbs, -- had at least one fail
        COUNT(*) AS total_pcbs
    FROM pcb_fail_summary
    GROUP BY date_trunc('hour', last_test_time)
)

SELECT
    f.date_hour,
    f.fpy_pcbs,
    ROUND(100.0 * f.fpy_pcbs / NULLIF(q.total_pcb_linewise, 0), 2) AS fpy_percent,
	q.pass_pcb_linewise,
	q.scrap_pcb_linewise,
	q.total_pcb_linewise,
	q.pass_pcb_linewise_percent,
	q.scrap_pcb_linewise_percent
FROM fpy_grouped as f join linewise_quality as q on f.date_hour=q.date_hour
ORDER BY date_hour;
