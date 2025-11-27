SELECT
  FLOOR(start_ts / 300000) AS WindowID,
  COUNT(*) AS ActiveCalls
FROM mono_signal
GROUP BY WindowID
ORDER BY WindowID;