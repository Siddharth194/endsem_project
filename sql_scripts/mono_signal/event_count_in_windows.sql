WITH Expanded AS (
  SELECT
    unique_id,
    start_ts,
    end_ts,
    generate_series(
      (start_ts / 30000) * 30000,
      (end_ts   / 30000) * 30000,
      30000
    )::bigint AS WindowStart
  FROM mono_signal
)
SELECT
  WindowStart,
  COUNT(DISTINCT(unique_id)) AS ActiveCalls
FROM Expanded
GROUP BY WindowStart
ORDER BY WindowStart;