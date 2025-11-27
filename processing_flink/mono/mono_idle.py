#!/usr/bin/env python3
"""
Batch idle detection (mono) — full-run latency; outputs gaps ordered by detection time (start_ts).

Usage:
python3 batch_idle_mono_ordered.py --input generation/mono_signal.csv --threshold-secs 30

Outputs:
QUERY_LATENCY_MS:<ms>
RESULT_ROWS:<n>

Then prints each gap row (caller, prev_end, start_ts, idle_ms) in chronological order.
"""
import argparse
import time
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="generation/mono_signal.csv")
    p.add_argument("--threshold-secs", type=int, default=30, help="idle threshold (seconds)")
    args = p.parse_args()

    threshold_ms = int(args.threshold_secs * 1000)

    # Batch TableEnvironment
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    tenv = TableEnvironment.create(settings)

    # Register CSV as table
    ddl = f"""
    CREATE TABLE mono_table (
      unique_id BIGINT,
      start_ts BIGINT,
      end_ts BIGINT,
      caller BIGINT,
      callee BIGINT,
      disposition STRING,
      imei BIGINT
    ) WITH (
      'connector' = 'filesystem',
      'path' = '{args.input}',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true'
    )
    """
    tenv.execute_sql(ddl)

    # SQL: compute prev_end per caller, filter gaps >= threshold, order by start_ts (chronological gap detection)
    sql = f"""
    SELECT caller, prev_end, start_ts, (start_ts - prev_end) AS idle_ms
    FROM (
      SELECT caller, start_ts, end_ts,
             LAG(end_ts) OVER (PARTITION BY caller ORDER BY start_ts) AS prev_end
      FROM mono_table
    )
    WHERE prev_end IS NOT NULL AND (start_ts - prev_end) >= {threshold_ms}
    ORDER BY start_ts
    """

    start = time.time()
    result = tenv.execute_sql(sql)

    # collect and print rows (in ORDER BY start_ts)
    count = 0
    for row in result.collect():
        # row is Row(caller, prev_end, start_ts, idle_ms)
        # print(f"{row[0]},{row[1]},{row[2]},{row[3]}")
        count += 1

    end = time.time()
    latency_ms = int((end - start) * 1000)
    print(f"QUERY_LATENCY_MS:{latency_ms}")
    print(f"RESULT_ROWS:{count}")

if __name__ == "__main__":
    main()
