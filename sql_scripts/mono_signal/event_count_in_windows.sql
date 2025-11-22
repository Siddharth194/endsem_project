WITH Expanded AS (
  SELECT
    unique_id,
    start_ts,
    end_ts,
    generate_series(
      (start_ts / 300000) * 300000,
      (end_ts   / 300000) * 300000,
      300000
    )::bigint AS WindowStart
  FROM mono_signal
)
SELECT
  WindowStart,
  COUNT(*) AS ActiveCalls
FROM Expanded
GROUP BY WindowStart
ORDER BY WindowStart;
