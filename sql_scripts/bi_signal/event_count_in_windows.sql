WITH Calls AS (
    SELECT
        s.unique_id,
        s.timestamp AS start_ts,
        e.timestamp AS end_ts
    FROM bi_signal s
    JOIN bi_signal e
        ON s.unique_id = e.unique_id
       AND s.event_type = 0
       AND e.event_type = 1
       AND e.timestamp > s.timestamp
),
Expanded AS (
    SELECT
        unique_id,
        start_ts,
        end_ts,
        generate_series(
            (start_ts / 300000) * 300000,
            (end_ts   / 300000) * 300000,
            300000
        )::bigint AS WindowStart
    FROM Calls
)
SELECT
    WindowStart,
    COUNT(*) AS ActiveCalls
FROM Expanded
GROUP BY WindowStart
ORDER BY WindowStart;
