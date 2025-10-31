-- ==========================================
-- SMT Production Database Initialization Script (with Retest Support)
-- ==========================================

-- Create main fact table for all inspection records (including retests)
CREATE TABLE IF NOT EXISTS station_records (
    booking_id      VARCHAR(30) PRIMARY KEY,       -- unique per event
    serial_number   VARCHAR(16) NOT NULL,          -- same PCB across multiple tests
    station_number  VARCHAR(4) NOT NULL,           -- station where event occurred
    book_state      VARCHAR(4) NOT NULL,           -- PASS / FAIL
    date_created    TIMESTAMP NOT NULL,            -- event time
    part_number     VARCHAR(10) NOT NULL,          -- part code
    cycle_time      DECIMAL(10,2) NOT NULL,        -- seconds
    test_count      INTEGER DEFAULT 1              -- NEW: test attempt number (1st, 2nd, 3rd, etc.)
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_serial_number   ON station_records(serial_number);
CREATE INDEX IF NOT EXISTS idx_station_number  ON station_records(station_number);
CREATE INDEX IF NOT EXISTS idx_date_created    ON station_records(date_created);
CREATE INDEX IF NOT EXISTS idx_part_number     ON station_records(part_number);
CREATE INDEX IF NOT EXISTS idx_book_state      ON station_records(book_state);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_serial_station  ON station_records(serial_number, station_number);
CREATE INDEX IF NOT EXISTS idx_date_station    ON station_records(date_created, station_number);

-- ==========================================
-- Station Dimension Table
-- ==========================================
CREATE TABLE IF NOT EXISTS stations (
    station_number  VARCHAR(4) PRIMARY KEY,
    station_name    VARCHAR(50) NOT NULL,
    lane            VARCHAR(20),
    line            VARCHAR(20),
    description     TEXT
);

-- Insert or update station metadata
INSERT INTO stations (station_number, station_name, lane, line, description)
VALUES
    ('1001', 'Loader', 'Lane 1', 'Line A', 'PCB Loading Station'),
    ('1002', 'Printer', 'Lane 1', 'Line A', 'Solder Paste Printer'),
    ('1003', 'SPI', 'Lane 1', 'Line A', 'Solder Paste Inspection'),
    ('1004', 'Pick & Place 1', 'Lane 1', 'Line A', 'Component Placement - Standard'),
    ('1005', 'Pick & Place 2', 'Lane 1', 'Line A', 'Component Placement - Fine Pitch'),
    ('1006', 'Reflow Oven', 'Lane 1', 'Line A', 'Reflow Oven'),
    ('1007', 'AOI 1', 'Lane 1', 'Line A', 'Automated Optical Inspection - Post Reflow'),
    ('1008', 'AOI 2', 'Lane 1', 'Line A', 'Automated Optical Inspection - Final')
ON CONFLICT (station_number) DO NOTHING;




-- Ideal time table

CREATE TABLE IF NOT EXISTS public.ideal_cycle_times (
    station_number VARCHAR(10) PRIMARY KEY,
    ideal_cycle_time DECIMAL(10,2) NOT NULL
);
INSERT INTO public.ideal_cycle_times (station_number, ideal_cycle_time) VALUES
('1001', 3.5),
('1002', 9.0),
('1003', 4.0),
('1004', 21.0),
('1005', 20.0),
('1006', 240.0),
('1007', 3.5),
('1008', 5.0)
ON CONFLICT (station_number) DO UPDATE 
SET ideal_cycle_time = EXCLUDED.ideal_cycle_time;


-- ==========================================
-- Permissions
-- ==========================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO smtadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO smtadmin;
