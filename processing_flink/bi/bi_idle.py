import argparse
import time
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="generation/bi_signal.csv")
    p.add_argument("--threshold-secs", type=int, default=30, help="idle threshold (seconds)")
    args = p.parse_args()

    threshold_ms = int(args.threshold_secs * 1000)

    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    tenv = TableEnvironment.create(settings)

    # Create bi_table from CSV — renamed column 'timestamp' -> 'event_ts' (timestamp is reserved)
    ddl = f"""
    CREATE TABLE bi_table (
      unique_id BIGINT,
      event_type INT,
      caller BIGINT,
      callee BIGINT,
      event_ts BIGINT,
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

    sql = f"""
    WITH
      starts AS (
        SELECT unique_id, caller, event_ts AS start_ts
        FROM bi_table
        WHERE event_type = 0
      ),
      ends AS (
        SELECT unique_id, event_ts AS end_ts
        FROM bi_table
        WHERE event_type = 1
      ),
      intervals AS (
        SELECT s.unique_id, s.caller, s.start_ts, e.end_ts
        FROM starts s
        JOIN ends e
          ON s.unique_id = e.unique_id
      ),
      with_prev AS (
        SELECT caller, start_ts, end_ts,
               LAG(end_ts) OVER (PARTITION BY caller ORDER BY start_ts) AS prev_end
        FROM intervals
      )
    SELECT caller, prev_end, start_ts, (start_ts - prev_end) AS idle_ms
    FROM with_prev
    WHERE prev_end IS NOT NULL AND (start_ts - prev_end) >= {threshold_ms}
    ORDER BY start_ts
    """

    start = time.time()
    result = tenv.execute_sql(sql)

    count = 0
    for row in result.collect():
        # Uncomment to print rows:
        # print(f"{row[0]},{row[1]},{row[2]},{row[3]}")
        count += 1

    end = time.time()
    latency_ms = int((end - start) * 1000)
    print(f"QUERY_LATENCY_MS:{latency_ms}")
    print(f"RESULT_ROWS:{count}")

if __name__ == "__main__":
    main()
