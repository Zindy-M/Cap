-- FILE: dbt/models/prepared_layers/crm_flattened.sql
-- CRM Data - 1: Flattened CRM with running balance

{{ config(
    materialized='table',
    schema='prepared_layers'
) }}

WITH account_info AS (
    SELECT 
        a.account_id,
        a.owner_name,
        a.email,
        a.phone_number,
        ad.street_address,
        ad.city,
        ad.state,
        ad.postal_code,
        ad.country,
        d.device_id,
        d.device_name,
        d.device_type,
        d.device_os
    FROM crm_data.accounts a
    LEFT JOIN crm_data.addresses ad ON a.account_id = ad.account_id
    LEFT JOIN crm_data.devices d ON a.account_id = d.account_id
),

usage_costs AS (
    SELECT 
        c.msisdn,
        DATE_TRUNC('hour', c.event_datetime) as hour,
        -- Data costs: 49 ZAR per GB
        SUM(c.up_bytes + c.down_bytes) * (49.0 / (1024.0 * 1024.0 * 1024.0)) as data_cost_zar
    FROM cdr_data.cdr_data c
    GROUP BY c.msisdn, hour
),

voice_costs AS (
    SELECT 
        v.msisdn,
        DATE_TRUNC('hour', v.start_time) as hour,
        -- Voice costs: 1 ZAR per minute = 1/60 ZAR per second
        SUM(v.call_duration_sec) * (1.0 / 60.0) as voice_cost_zar
    FROM cdr_data.cdr_voice v
    GROUP BY v.msisdn, hour
),

combined_costs AS (
    SELECT 
        COALESCE(u.msisdn, v.msisdn) as msisdn,
        COALESCE(u.hour, v.hour) as hour,
        COALESCE(u.data_cost_zar, 0) as data_cost_zar,
        COALESCE(v.voice_cost_zar, 0) as voice_cost_zar,
        COALESCE(u.data_cost_zar, 0) + COALESCE(v.voice_cost_zar, 0) as total_cost_zar
    FROM usage_costs u
    FULL OUTER JOIN voice_costs v ON u.msisdn = v.msisdn AND u.hour = v.hour
),

running_balance AS (
    SELECT 
        c.*,
        SUM(c.total_cost_zar) OVER (
            PARTITION BY c.msisdn 
            ORDER BY c.hour 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_cost_zar,
        -- Convert ZAR to WAK (assuming 1 ZAR = 1 WAK for this project)
        c.total_cost_zar as total_cost_wak,
        SUM(c.total_cost_zar) OVER (
            PARTITION BY c.msisdn 
            ORDER BY c.hour 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_cost_wak
    FROM combined_costs c
)

SELECT 
    rb.msisdn,
    rb.hour as datetime,
    ai.account_id,
    ai.owner_name,
    ai.email,
    ai.phone_number,
    ai.street_address,
    ai.city,
    ai.state,
    ai.postal_code,
    ai.country,
    ai.device_id,
    ai.device_name,
    ai.device_type,
    ai.device_os,
    rb.data_cost_zar,
    rb.voice_cost_zar,
    rb.total_cost_zar,
    rb.cumulative_cost_zar,
    rb.data_cost_wak,
    rb.voice_cost_wak,
    rb.total_cost_wak,
    rb.cumulative_cost_wak
FROM running_balance rb
LEFT JOIN account_info ai ON rb.msisdn = ai.phone_number
ORDER BY rb.msisdn, rb.hour