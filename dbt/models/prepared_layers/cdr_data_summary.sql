-- FILE: dbt/models/prepared_layers/cdr_data_summary.sql
-- CDR Data - 1: Summarize CDR data per 15 min, 30 min and 1 hr

{{ config(
    materialized='table',
    schema='prepared_layers'
) }}

WITH data_summary AS (
    SELECT 
        msisdn,
        data_type,
        DATE_TRUNC('hour', event_datetime) + 
            INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM event_datetime) / 15) as time_15min,
        DATE_TRUNC('hour', event_datetime) + 
            INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM event_datetime) / 30) as time_30min,
        DATE_TRUNC('hour', event_datetime) as time_1hr,
        SUM(up_bytes) as total_up_bytes,
        SUM(down_bytes) as total_down_bytes,
        COUNT(*) as session_count
    FROM cdr_data.cdr_data
    GROUP BY msisdn, data_type, time_15min, time_30min, time_1hr
),

voice_summary AS (
    SELECT 
        msisdn,
        call_type,
        DATE_TRUNC('hour', start_time) + 
            INTERVAL '15 minutes' * FLOOR(EXTRACT(MINUTE FROM start_time) / 15) as time_15min,
        DATE_TRUNC('hour', start_time) + 
            INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM start_time) / 30) as time_30min,
        DATE_TRUNC('hour', start_time) as time_1hr,
        SUM(call_duration_sec) as total_duration_sec,
        COUNT(*) as call_count
    FROM cdr_data.cdr_voice
    GROUP BY msisdn, call_type, time_15min, time_30min, time_1hr
)

SELECT 
    '15min' as interval_type,
    msisdn,
    time_15min as datetime,
    data_type as usage_type,
    'data' as category,
    total_up_bytes,
    total_down_bytes,
    total_up_bytes + total_down_bytes as total_bytes,
    0 as call_duration_sec,
    session_count
FROM data_summary

UNION ALL

SELECT 
    '15min' as interval_type,
    msisdn,
    time_15min as datetime,
    call_type as usage_type,
    'voice' as category,
    0 as total_up_bytes,
    0 as total_down_bytes,
    0 as total_bytes,
    total_duration_sec as call_duration_sec,
    call_count as session_count
FROM voice_summary

UNION ALL

SELECT 
    '30min' as interval_type,
    msisdn,
    time_30min as datetime,
    data_type as usage_type,
    'data' as category,
    total_up_bytes,
    total_down_bytes,
    total_up_bytes + total_down_bytes as total_bytes,
    0 as call_duration_sec,
    session_count
FROM data_summary

UNION ALL

SELECT 
    '30min' as interval_type,
    msisdn,
    time_30min as datetime,
    call_type as usage_type,
    'voice' as category,
    0 as total_up_bytes,
    0 as total_down_bytes,
    0 as total_bytes,
    total_duration_sec as call_duration_sec,
    call_count as session_count
FROM voice_summary

UNION ALL

SELECT 
    '1hr' as interval_type,
    msisdn,
    time_1hr as datetime,
    data_type as usage_type,
    'data' as category,
    total_up_bytes,
    total_down_bytes,
    total_up_bytes + total_down_bytes as total_bytes,
    0 as call_duration_sec,
    session_count
FROM data_summary

UNION ALL

SELECT 
    '1hr' as interval_type,
    msisdn,
    time_1hr as datetime,
    call_type as usage_type,
    'voice' as category,
    0 as total_up_bytes,
    0 as total_down_bytes,
    0 as total_bytes,
    total_duration_sec as call_duration_sec,
    call_count as session_count
FROM voice_summary