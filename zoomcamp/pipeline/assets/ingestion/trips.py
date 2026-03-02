"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: vendor_id
    type: BIGINT
  - name: tpep_pickup_datetime
    type: TIMESTAMP
  - name: tpep_dropoff_datetime
    type: TIMESTAMP
  - name: passenger_count
    type: DOUBLE
  - name: trip_distance
    type: DOUBLE
  - name: ratecode_id
    type: DOUBLE
  - name: store_and_fwd_flag
    type: VARCHAR
  - name: pu_location_id
    type: BIGINT
  - name: do_location_id
    type: BIGINT
  - name: payment_type
    type: BIGINT
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

@bruin"""

# Imports required by the materialize implementation
import os
import json
from datetime import date, datetime
from io import BytesIO
from typing import List

import pandas as pd
import requests


# Only implement `materialize()` because this asset uses Python materialization
def _month_range(start_date: date, end_date: date) -> List[date]:
  months = []
  cur = date(start_date.year, start_date.month, 1)
  end = date(end_date.year, end_date.month, 1)
  while cur <= end:
    months.append(cur)
    if cur.month == 12:
      cur = date(cur.year + 1, 1, 1)
    else:
      cur = date(cur.year, cur.month + 1, 1)
  return months


def materialize():
  """Download monthly NYC taxi parquet files for the run window and return a DataFrame.

  Behavior:
  - Reads `BRUIN_START_DATE` and `BRUIN_END_DATE` environment variables (YYYY-MM-DD).
  - Reads optional `BRUIN_VARS` JSON to get `taxi_types` (list of taxi types, e.g. ["yellow"]).
  - For each taxi type and month in the window, attempts to download the parquet file from
    the public NYC taxi dataset CDN and concatenates the results.

  Returns a pandas.DataFrame which Bruin will append into the configured DuckDB table.
  """
  # read window
  start_s = os.environ.get("BRUIN_START_DATE")
  end_s = os.environ.get("BRUIN_END_DATE")
  if not start_s or not end_s:
    today = date.today()
    start = date(today.year, max(1, today.month - 1), 1)
    end = date(today.year, today.month, 1)
  else:
    start = date.fromisoformat(start_s)
    end = date.fromisoformat(end_s)

  # taxi types from BRUIN_VARS or default to yellow
  taxi_types = ["yellow"]
  bruin_vars = os.environ.get("BRUIN_VARS")
  if bruin_vars:
    try:
      vars_obj = json.loads(bruin_vars)
      if isinstance(vars_obj.get("taxi_types"), list):
        taxi_types = vars_obj.get("taxi_types")
    except Exception:
      pass

  months = _month_range(start, end)
  dfs = []
  for taxi in taxi_types:
    for m in months:
      year = m.year
      mon = f"{m.month:02d}"
      url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{mon}.parquet"
      try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        buf = BytesIO(resp.content)
        # prefer parquet (pyarrow) reading
        try:
          df = pd.read_parquet(buf, engine="pyarrow")
        except Exception:
          # fallback to read_parquet without engine or try csv
          try:
            buf.seek(0)
            df = pd.read_parquet(buf)
          except Exception:
            # last resort: try reading as CSV
            buf.seek(0)
            df = pd.read_csv(buf)
        if not df.empty:
          df["extracted_at"] = datetime.utcnow()
          df["source_taxi_type"] = taxi
          df["source_month"] = f"{year}-{mon}"
          dfs.append(df)
      except Exception:
        # skip missing months or network issues
        continue

  if not dfs:
    return pd.DataFrame()

  result = pd.concat(dfs, ignore_index=True, sort=False)
  return result
