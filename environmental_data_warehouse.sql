Create Table r1(
    Indicator_ID int NOT NULL,
    Name VARCHAR(100) NOT NULL,
    Measure VARCHAR(100),
    Measure_Info VARCHAR(100),
    PRIMARY KEY (Indicator_ID)
);

COPY r1 FROM '/Users/mayu/Downloads/piyushmo_ughaskata_mythrish_final_project_phase_1/R1_no_duplicates.csv'
DELIMITER ','  
CSV HEADER;

SELECT * from r1;

Create Table r2(
    Geo_Place_Name VARCHAR(100),
    Geo_Join_ID FLOAT NOT NULL,
    PRIMARY KEY (Geo_place_name)
);

COPY r2 FROM '/Users/mayu/Downloads/piyushmo_ughaskata_mythrish_final_project_phase_1/R2_no_duplicates.csv'
DELIMITER ','  
CSV HEADER;

SELECT * from r2;

Create Table r3(
    Start_Date VARCHAR(100) NOT NULL,
    Time_Period VARCHAR(100),
    PRIMARY KEY(Time_period)
);

COPY r3 FROM '/Users/mayu/Downloads/piyushmo_ughaskata_mythrish_final_project_phase_1/R3_no_duplicates.csv'
DELIMITER ','  
CSV HEADER;

SELECT * from r3;

Create Table r4(
    Time_Period VARCHAR(100),
    Geo_Place_Name VARCHAR(100),
    Geo_Type_Name VARCHAR(100) NULL,
    Indicator_ID int,
    Unique_ID VARCHAR(100),
    Data_Value FLOAT NOT NULL,
    PRIMARY KEY(Unique_id),
    FOREIGN KEY(Time_period) REFERENCES r3(Time_period) ON DELETE CASCADE,
    FOREIGN KEY(Geo_Place_Name) REFERENCES r2(Geo_Place_Name) ON DELETE CASCADE,
    FOREIGN KEY(Indicator_ID) REFERENCES r1(Indicator_ID) ON DELETE SET NULL
);

COPY r4 FROM '/Users/mayu/Downloads/piyushmo_ughaskata_mythrish_final_project_phase_1/R4_no_duplicates.csv'
DELIMITER ','  
CSV HEADER;

SELECT * from r4;

CREATE TYPE regulation_level AS ENUM ('Lenient', 'Moderate', 'Strict');
CREATE TYPE compliance_level AS ENUM ('Low Compliance', 'Moderate Compliance', 'High Compliance');

CREATE TABLE compliance_data (
    environmental_regulation_level regulation_level NOT NULL,
    regulatory_stringency compliance_level NOT NULL
);

COPY compliance_data(environmental_regulation_level, regulatory_stringency)
FROM '/Users/mayu/Downloads/piyushmo_ughaskata_mythrish_final_project_phase_1/R6_expanded.csv' DELIMITER ',' CSV HEADER;

select * from compliance_data;

CREATE TABLE urbanization_levels (
    level_id SERIAL PRIMARY KEY,
    level_name VARCHAR(20) NOT NULL UNIQUE
);

INSERT INTO urbanization_levels (level_name) VALUES
    ('Highly Urbanized'),
    ('Moderately Urbanized'),
    ('Rural');

CREATE TABLE city_population_data (
    population_density INTEGER NOT NULL CHECK (population_density >= 0),
    urbanization_level VARCHAR(20) NOT NULL REFERENCES urbanization_levels(level_name)
);

copy city_population_data(population_density, urbanization_level) 
FROM '/Users/mayu/Downloads/piyushmo_ughaskata_mythrish_final_project_phase_1/R5.csv' 
WITH (FORMAT CSV, HEADER);

select * from city_population_data;

CREATE TYPE emission_type AS ENUM ('Particulate Matter', 'CO2', 'SOx', 'NOx');

CREATE TYPE climate_zone AS ENUM ('Temperate', 'Arid', 'Continental', 'Tropical', 'Polar');

CREATE TABLE air_quality_data (
    city_population_density INTEGER NOT NULL ,
    environmental_regulation_level regulation_level NOT NULL,
    emission_type emission_type NOT NULL,
    air_quality_index INTEGER NOT NULL ,
    climate_zone climate_zone NOT NULL,
    public_awareness_score DECIMAL(4,2) NOT NULL ,
    monitoring_stations INTEGER NOT NULL ,
    climate_adaptation_index DECIMAL(5,2) NOT NULL
);

COPY air_quality_data(
    city_population_density,
    environmental_regulation_level,
    emission_type,
    air_quality_index,
    climate_zone,
    public_awareness_score,
    monitoring_stations,
    climate_adaptation_index
)
FROM '/Users/mayu/Downloads/piyushmo_ughaskata_mythrish_final_project_phase_1/R7.csv' DELIMITER ',' CSV HEADER;

select * from air_quality_data;

CREATE TABLE has_indicator (
  Indicator_ID INT    NOT NULL
    REFERENCES r1(Indicator_ID)
    ON DELETE CASCADE,
  Unique_ID    VARCHAR(100) NOT NULL
    REFERENCES r4(Unique_ID)
    ON DELETE CASCADE,
  PRIMARY KEY (Indicator_ID, Unique_ID)
);

INSERT INTO has_indicator
  SELECT r1.Indicator_ID, r4.Unique_ID
    FROM r1
    JOIN r4 USING (Indicator_ID);

SELECT * FROM has_indicator;

CREATE TABLE located_in (
  Geo_Place_Name VARCHAR(100) NOT NULL
    REFERENCES r2(Geo_Place_Name)
    ON DELETE CASCADE,
  Unique_ID      VARCHAR(100) NOT NULL
    REFERENCES r4(Unique_ID)
    ON DELETE CASCADE,
  PRIMARY KEY (Geo_Place_Name, Unique_ID)
);

INSERT INTO located_in
  SELECT r2.Geo_Place_Name, r4.Unique_ID
    FROM r2
    JOIN r4 USING (Geo_Place_Name);

SELECT * FROM located_in;

CREATE TABLE occurs_during (
  Unique_ID   VARCHAR(100) NOT NULL
    REFERENCES r4(Unique_ID)
    ON DELETE CASCADE,
  Time_Period VARCHAR(100) NOT NULL
    REFERENCES r3(Time_Period)
    ON DELETE CASCADE,
  PRIMARY KEY (Unique_ID, Time_Period)
);

INSERT INTO occurs_during
  SELECT r4.Unique_ID, r3.Time_Period
    FROM r4
    JOIN r3 USING (Time_Period);

SELECT * FROM occurs_during;

CREATE table policies(
    city_population_density VARCHAR(100),
    environmental_regulation_level regulation_level NOT NULL,
    regulatory_stringency compliance_level NOT NULL,
    PRIMARY KEY(city_population_density)
);

CREATE table effect(
    population_density INTEGER NOT NULL CHECK (population_density >= 0),
    urbanization_level VARCHAR(20) NOT NULL REFERENCES urbanization_levels(level_name),
    city_population_density VARCHAR(100),
    PRIMARY KEY(city_population_density)
);

-- 1) INSERTIONS

-- a) Add a new indicator to the master list
-- find the current max, then +1:
WITH max_id AS (
  SELECT COALESCE(MAX(Indicator_ID), 0) + 1 AS next_id
    FROM r1
)
INSERT INTO r1 (Indicator_ID, Name, Measure, Measure_Info)
SELECT next_id, 'Your New Indicator', 'Unit', 'Info'
  FROM max_id;

-- 2) DELETIONS

-- a) Remove an obsolete indicator (cascades its usage in r4 → has_indicator)
DELETE FROM r1
WHERE Indicator_ID = 648;

-- b) Delete a single measurement record by its Unique_ID
DELETE FROM r4
WHERE Unique_ID = '221812';


-- 3) UPDATES

-- a) Correct a mis‑recorded Data_Value in r4
UPDATE r4
SET Data_Value = 29.5
WHERE Unique_ID = '179772';

-- b) Bump public awareness scores in all Temperate‑zone records
UPDATE air_quality_data
SET public_awareness_score = public_awareness_score + 1.0
WHERE climate_zone = 'Temperate';

-- 1) JOIN: show each fact with its indicator name and geography
SELECT
  r4.unique_id,
  r2.geo_place_name,
  r1.name        AS indicator_name,
  r4.data_value
FROM r4
  JOIN r2 ON r4.geo_place_name = r2.geo_place_name
  JOIN r1 ON r4.indicator_id  = r1.indicator_id
ORDER BY r2.geo_place_name, r1.name;

-- 2) ORDER BY + LIMIT: top 10 worst air‐quality readings
SELECT
  city_population_density,
  emission_type,
  air_quality_index,
  climate_zone
FROM air_quality_data
ORDER BY air_quality_index DESC
LIMIT 10;


-- 3) GROUP BY + HAVING: average AQI per climate zone, but only zones above 100
SELECT
  climate_zone,
  AVG(air_quality_index)::NUMERIC(6,2) AS avg_aqi
FROM air_quality_data
GROUP BY climate_zone
HAVING AVG(air_quality_index) > 100
ORDER BY avg_aqi DESC;

-- 4) SUBQUERY in WHERE: list all densities whose monitoring station count is above the overall average
SELECT DISTINCT
  city_population_density
FROM air_quality_data AS a
WHERE monitoring_stations > (
    SELECT AVG(monitoring_stations)
      FROM air_quality_data
);

-- 1) Insert a new indicator
CREATE OR REPLACE PROCEDURE sp_add_indicator(
  p_indicator_id   INT,
  p_name           VARCHAR,
  p_measure        VARCHAR,
  p_info           VARCHAR
)
LANGUAGE SQL
AS $$
  INSERT INTO r1(indicator_id, name, measure, measure_info)
  VALUES (p_indicator_id, p_name, p_measure, p_info);
$$;

-- 2) Update an indicator’s Measure_Info
CREATE OR REPLACE PROCEDURE sp_update_indicator_info(
  p_indicator_id   INT,
  p_info           VARCHAR
)
LANGUAGE SQL
AS $$
  UPDATE r1
     SET measure_info = p_info
   WHERE indicator_id = p_indicator_id;
$$;

-- 3) Delete an indicator
CREATE OR REPLACE PROCEDURE sp_delete_indicator(
  p_indicator_id   INT
)
LANGUAGE SQL
AS $$
  DELETE FROM r1
   WHERE indicator_id = p_indicator_id;
$$;

-- 4) Select (return) a single indicator by ID
CREATE OR REPLACE FUNCTION fn_get_indicator(
  p_indicator_id   INT
) RETURNS TABLE(
  indicator_id   INT,
  name           VARCHAR,
  measure        VARCHAR,
  measure_info   VARCHAR
)
LANGUAGE SQL
AS $$
  SELECT indicator_id, name, measure, measure_info
    FROM r1
   WHERE indicator_id = p_indicator_id;
$$;

-- Insert:
CALL sp_add_indicator(71001, 'New Air Pollutant', 'Mean', 'units per km²');

-- Update:
CALL sp_update_indicator_info(701, 'updated info text');

-- Delete:
CALL sp_delete_indicator(71001);

-- Select:
SELECT * FROM fn_get_indicator(386);

--Task 6
-- 1) Audit table to capture trigger activity
CREATE TABLE audit_log (
  log_id    SERIAL    PRIMARY KEY,
  action    TEXT      NOT NULL,
  action_ts TIMESTAMP NOT NULL DEFAULT now()
);

-- 2) Trigger function: log every r1 insert
CREATE OR REPLACE FUNCTION trg_r1_insert_audit()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO audit_log(action)
    VALUES ('Inserted r1.id=' || NEW.indicator_id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger
DROP TRIGGER IF EXISTS r1_audit ON r1;
CREATE TRIGGER r1_audit
  AFTER INSERT ON r1
  FOR EACH ROW
  EXECUTE FUNCTION trg_r1_insert_audit();

-- 3) Demonstrate transactional rollback of both data AND trigger work

-- clean slate
TRUNCATE TABLE
  audit_log,
  r4,
  r1
RESTART IDENTITY
CASCADE;

-- begin transaction
BEGIN;

  -- first insert is valid → trigger fires, writes to audit_log
  INSERT INTO r1(indicator_id, name, measure, measure_info)
  VALUES (2723, 'Test Pollutant', 'Mean', 'ppb');

  -- second insert duplicates the PK → this will ERROR
  INSERT INTO r1(indicator_id, name, measure, measure_info)
  VALUES (2723, 'Test Pollutant Duplicate', 'Mean', 'ppb');

COMMIT; 

-- verify: no rows in audit_log because the trigger’s insert was rolled back
SELECT * FROM audit_log;

--Task 7

EXPLAIN ANALYZE
SELECT
  r4.unique_id,
  r2.geo_place_name,
  r1.name        AS indicator_name,
  r4.data_value
FROM r4
  JOIN r2 ON r4.geo_place_name = r2.geo_place_name
  JOIN r1 ON r4.indicator_id  = r1.indicator_id
ORDER BY r2.geo_place_name, r1.name;

-- We add these indexes so that PostgreSQL can quickly locate matching rows in the large fact table r4
-- during the JOINs, avoiding expensive full-table scans and nested-loop lookups on r4’s join columns.

-- 1) index r4.geo_place_name so the JOIN → r2 can use an index scan
CREATE INDEX idx_r4_geo ON r4(geo_place_name);

-- 2) index r4.indicator_id so the JOIN → r1 can use an index scan
CREATE INDEX idx_r4_ind ON r4(indicator_id);


EXPLAIN ANALYZE
SELECT
  climate_zone,
  AVG(air_quality_index) AS avg_aqi
FROM air_quality_data
GROUP BY climate_zone
HAVING AVG(air_quality_index) > 100
ORDER BY avg_aqi DESC;

-- We create a btree index on the GROUP BY column so PostgreSQL can quickly locate and group rows by climate_zone,
-- reducing the need for a full table scan when computing aggregates.
CREATE INDEX idx_aq_climate ON air_quality_data(climate_zone);

EXPLAIN ANALYZE
SELECT
  city_population_density,
  emission_type,
  air_quality_index
FROM air_quality_data
ORDER BY air_quality_index DESC
LIMIT 10;


-- We build a descending btree index on air_quality_index so PostgreSQL can fetch the highest AQI values
-- directly from the index for Top‑N queries, avoiding a full table scan and expensive sort.
CREATE INDEX idx_aq_index_desc
  ON air_quality_data (air_quality_index DESC);
  
-- Refresh planner statistics so the new indexes are taken into account
VACUUM ANALYZE r4;
VACUUM ANALYZE air_quality_data;

-- 1) Re‑run Query 1: JOIN + ORDER BY
EXPLAIN ANALYZE
SELECT
  r4.unique_id,
  r2.geo_place_name,
  r1.name        AS indicator_name,
  r4.data_value
FROM r4
  JOIN r2 ON r4.geo_place_name = r2.geo_place_name
  JOIN r1 ON r4.indicator_id   = r1.indicator_id
ORDER BY r2.geo_place_name, r1.name;

-- 2) Re‑run Query 2: GROUP BY + HAVING
EXPLAIN ANALYZE
SELECT
  climate_zone,
  AVG(air_quality_index)::NUMERIC(6,2) AS avg_aqi
FROM air_quality_data
GROUP BY climate_zone
HAVING AVG(air_quality_index) > 100
ORDER BY avg_aqi DESC;

-- 3) Re‑run Query 3: ORDER BY + LIMIT (Top 10)
EXPLAIN ANALYZE
SELECT
  city_population_density,
  emission_type,
  air_quality_index
FROM air_quality_data
ORDER BY air_quality_index DESC
LIMIT 10;



