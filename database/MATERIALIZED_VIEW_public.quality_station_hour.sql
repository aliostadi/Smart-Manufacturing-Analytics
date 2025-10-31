
CREATE MATERIALIZED VIEW public.quality_station_hour AS
WITH 
-- 🧱 1️⃣ Base CTE with hourly truncation and max test count
base AS (
    SELECT
        station_number,
        serial_number,
        book_state,
        test_count,
        date_created,
        date_trunc('hour', date_created) AS date_hour,
        MAX(test_count) OVER (
            PARTITION BY station_number, serial_number, date_trunc('hour', date_created)
            ORDER BY date_created
        ) AS max_test_count
    FROM public.station_records
),

-- 🧩 2️⃣ Keep only the latest test per PCB per station/hour
last_test_result AS (
    SELECT *
    FROM base
    WHERE test_count = max_test_count
),

-- 🧮 3️⃣ Aggregate distinct PCB-level results
pcbs AS (
    SELECT
        station_number,
        date_hour,
        COUNT(DISTINCT serial_number) AS total_pcb,
        COUNT(DISTINCT serial_number) FILTER (WHERE book_state = 'FAIL') AS fail_pcb,
        COUNT(DISTINCT serial_number) FILTER (WHERE book_state = 'PASS') AS pass_pcb
    FROM last_test_result
    GROUP BY station_number, date_hour
),

-- 📊 4️⃣ Aggregate booking-level results (all test attempts)
bookings AS (
    SELECT
        station_number,
        date_trunc('hour', date_created) AS date_hour,
        COUNT(serial_number) AS total_bookings,
        COUNT(serial_number) FILTER (WHERE book_state = 'FAIL') AS fail_bookings,
        COUNT(serial_number) FILTER (WHERE book_state = 'PASS') AS pass_bookings
    FROM public.station_records
    GROUP BY station_number, date_trunc('hour', date_created)
)

-- 🧩 5️⃣ Join both
SELECT
    b.station_number,
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
 AND b.date_hour = p.date_hour
ORDER BY  b.station_number,b.date_hour;
