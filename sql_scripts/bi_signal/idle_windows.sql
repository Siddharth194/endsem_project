WITH Calls AS (
    SELECT
        s.unique_id,
        s.caller,
        s.timestamp AS start_ts,
        (
            SELECT e.timestamp
            FROM bi_signal e
            WHERE e.unique_id = s.unique_id
              AND e.event_type = 1
              AND e.timestamp > s.timestamp
            ORDER BY e.timestamp
            LIMIT 1
        ) AS end_ts
    FROM bi_signal s
    WHERE s.event_type = 0
)

SELECT
    a.caller,
    a.end_ts   AS PrevEnd,
    b.start_ts AS NextStart,
    b.start_ts - a.end_ts AS IdleGap
FROM Calls a
JOIN Calls b
    ON  b.caller   = a.caller
    AND b.start_ts > a.end_ts
WHERE NOT EXISTS (
    SELECT 1
    FROM Calls c
    WHERE c.caller   = a.caller
      AND c.start_ts > a.end_ts
      AND c.start_ts < b.start_ts
)
AND b.start_ts - a.end_ts > 900000
ORDER BY a.caller, a.end_ts;
