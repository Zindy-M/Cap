-- FILE: dbt/models/prepared_layers/cdr_tower_sessions.sql
-- CDR Data - 2: Tower interaction sessions

{{ config(
    materialized='table',
    schema='prepared_layers'
) }}

WITH tower_interactions AS (
    SELECT 
        msisdn,
        tower_id,
        event_datetime,
        LAG(tower_id) OVER (PARTITION BY msisdn ORDER BY event_datetime) as prev_tower,
        LAG(event_datetime) OVER (PARTITION BY msisdn ORDER BY event_datetime) as prev_time,
        LEAD(tower_id) OVER (PARTITION BY msisdn ORDER BY event_datetime) as next_tower,
        LEAD(event_datetime) OVER (PARTITION BY msisdn ORDER BY event_datetime) as next_time
    FROM (
        SELECT DISTINCT msisdn, tower_id, event_datetime 
        FROM cdr_data.cdr_data
        UNION
        SELECT DISTINCT msisdn, tower_id, start_time as event_datetime 
        FROM cdr_data.cdr_voice
    ) combined
),

session_starts AS (
    SELECT 
        msisdn,
        tower_id,
        event_datetime as session_start,
        CASE 
            WHEN prev_tower IS NULL THEN TRUE
            WHEN prev_tower != tower_id THEN TRUE
            WHEN next_tower = tower_id THEN TRUE
            ELSE FALSE
        END as is_session_start
    FROM tower_interactions
    WHERE next_tower = tower_id OR prev_tower IS NULL
),

session_ends AS (
    SELECT 
        msisdn,
        tower_id,
        event_datetime as session_end,
        CASE 
            WHEN next_tower IS NULL THEN TRUE
            WHEN next_tower != tower_id THEN TRUE
            WHEN prev_tower = tower_id THEN TRUE
            ELSE FALSE
        END as is_session_end
    FROM tower_interactions
    WHERE prev_tower = tower_id OR next_tower IS NULL
)

SELECT 
    s.msisdn,
    s.tower_id,
    s.session_start,
    e.session_end,
    EXTRACT(EPOCH FROM (e.session_end - s.session_start)) / 3600 as duration_hours
FROM session_starts s
JOIN session_ends e 
    ON s.msisdn = e.msisdn 
    AND s.tower_id = e.tower_id
    AND e.session_end >= s.session_start
    AND s.is_session_start = TRUE
    AND e.is_session_end = TRUE
WHERE s.session_start < e.session_end