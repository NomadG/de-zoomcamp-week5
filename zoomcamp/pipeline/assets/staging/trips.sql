/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table
  strategy: merge
  primary_key: trip_id
  #incremental_key: extracted_at

depends:
  - ingestion.trips
  - ingestion.payment_lookup

columns:
  - name: trip_id
    type: VARCHAR
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: vendor_id
    type: BIGINT
  - name: pickup_datetime
    type: TIMESTAMP
  - name: dropoff_datetime
    type: TIMESTAMP
  - name: passenger_count
    type: DOUBLE
  - name: trip_distance
    type: DOUBLE
  - name: rate_code_id
    type: DOUBLE
  - name: store_and_fwd_flag
    type: VARCHAR
  - name: pickup_location_id
    type: BIGINT
  - name: dropoff_location_id
    type: BIGINT
  - name: payment_type_id
    type: BIGINT
  - name: payment_type
    type: VARCHAR
  - name: fare_amount
    type: DOUBLE
  - name: extra
    type: DOUBLE
  - name: mta_tax
    type: DOUBLE
  - name: tip_amount
    type: DOUBLE
  - name: tolls_amount
    type: DOUBLE
  - name: improvement_surcharge
    type: DOUBLE
  - name: total_amount
    type: DOUBLE
  - name: congestion_surcharge
    type: DOUBLE
  - name: airport_fee
    type: DOUBLE
  - name: extracted_at
    type: TIMESTAMP
  - name: source_taxi_type
    type: VARCHAR
  - name: source_month
    type: VARCHAR

custom_checks:
  - name: row_count_positive
    description: ensures staging.trips is not empty
    query: SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM staging.trips
    value: 1


@bruin */

-- Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Create surrogate key from available IDs (MD5 hash of VendorID + timestamps + passenger_count)
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)\
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

SELECT
  -- Create a surrogate primary key by hashing a combination of identifiers
  MD5(CONCAT(
    COALESCE(CAST(t.vendor_id AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.ratecode_id AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.pu_location_id AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.do_location_id AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.payment_type AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.tpep_pickup_datetime AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.tpep_dropoff_datetime AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.passenger_count AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.trip_distance AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.fare_amount AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.extra AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.mta_tax AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.tip_amount AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.tolls_amount AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.improvement_surcharge AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.total_amount AS VARCHAR), ''),
    '|',
    COALESCE(CAST(t.congestion_surcharge AS VARCHAR), '')
  )) AS trip_id,
  t.vendor_id,
  t.tpep_pickup_datetime AS pickup_datetime,
  t.tpep_dropoff_datetime AS dropoff_datetime,
  t.passenger_count,
  t.trip_distance,
  t.ratecode_id AS rate_code_id,
  t.store_and_fwd_flag,
  t.pu_location_id AS pickup_location_id,
  t.do_location_id AS dropoff_location_id,
  t.payment_type AS payment_type_id,
  COALESCE(p.payment_type_name, 'Unknown') AS payment_type,
  t.fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  t.improvement_surcharge,
  t.total_amount,
  t.congestion_surcharge,
  t.airport_fee,
  t.extracted_at,
  t.source_taxi_type,
  t.source_month
FROM ingestion.trips AS t
LEFT JOIN ingestion.payment_lookup AS p
  ON t.payment_type = p.payment_type_id
{# WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}' #}
