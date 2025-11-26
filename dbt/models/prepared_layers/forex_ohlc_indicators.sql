-- FILE: dbt/models/prepared_layers/forex_ohlc_indicators.sql
-- Forex Data - 1: OHLC summaries with technical indicators

{{ config(
    materialized='table',
    schema='prepared_layers'
) }}

WITH tick_data AS (
    SELECT 
        timestamp,
        pair_name,
        bid_price,
        ask_price,
        (bid_price + ask_price) / 2 as mid_price
    FROM forex_data.forex_ticks
),

-- M1 (1 minute) candles
m1_candles AS (
    SELECT 
        pair_name,
        DATE_TRUNC('minute', timestamp) as candle_time,
        FIRST_VALUE(mid_price) OVER w as open_price,
        MAX(mid_price) OVER w as high_price,
        MIN(mid_price) OVER w as low_price,
        LAST_VALUE(mid_price) OVER w as close_price,
        COUNT(*) OVER w as tick_count
    FROM tick_data
    WINDOW w AS (
        PARTITION BY pair_name, DATE_TRUNC('minute', timestamp)
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),

m1_distinct AS (
    SELECT DISTINCT ON (pair_name, candle_time)
        pair_name,
        candle_time,
        open_price,
        high_price,
        low_price,
        close_price
    FROM m1_candles
),

-- M30 (30 minute) candles
m30_candles AS (
    SELECT 
        pair_name,
        DATE_TRUNC('hour', timestamp) + 
            INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM timestamp) / 30) as candle_time,
        FIRST_VALUE(mid_price) OVER w as open_price,
        MAX(mid_price) OVER w as high_price,
        MIN(mid_price) OVER w as low_price,
        LAST_VALUE(mid_price) OVER w as close_price
    FROM tick_data
    WINDOW w AS (
        PARTITION BY pair_name, DATE_TRUNC('hour', timestamp) + 
            INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM timestamp) / 30)
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),

m30_distinct AS (
    SELECT DISTINCT ON (pair_name, candle_time)
        pair_name,
        candle_time,
        open_price,
        high_price,
        low_price,
        close_price
    FROM m30_candles
),

-- H1 (1 hour) candles
h1_candles AS (
    SELECT 
        pair_name,
        DATE_TRUNC('hour', timestamp) as candle_time,
        FIRST_VALUE(mid_price) OVER w as open_price,
        MAX(mid_price) OVER w as high_price,
        MIN(mid_price) OVER w as low_price,
        LAST_VALUE(mid_price) OVER w as close_price
    FROM tick_data
    WINDOW w AS (
        PARTITION BY pair_name, DATE_TRUNC('hour', timestamp)
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),

h1_distinct AS (
    SELECT DISTINCT ON (pair_name, candle_time)
        pair_name,
        candle_time,
        open_price,
        high_price,
        low_price,
        close_price
    FROM h1_candles
),

-- Calculate EMA and ATR for M1
m1_with_indicators AS (
    SELECT 
        *,
        'M1' as timeframe,
        AVG(open_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as ema_8,
        AVG(open_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as ema_21,
        AVG(high_price - low_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as atr_8,
        AVG(high_price - low_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as atr_21
    FROM m1_distinct
),

-- Calculate EMA and ATR for M30
m30_with_indicators AS (
    SELECT 
        *,
        'M30' as timeframe,
        AVG(open_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as ema_8,
        AVG(open_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as ema_21,
        AVG(high_price - low_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as atr_8,
        AVG(high_price - low_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as atr_21
    FROM m30_distinct
),

-- Calculate EMA and ATR for H1
h1_with_indicators AS (
    SELECT 
        *,
        'H1' as timeframe,
        AVG(open_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as ema_8,
        AVG(open_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as ema_21,
        AVG(high_price - low_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) as atr_8,
        AVG(high_price - low_price) OVER (
            PARTITION BY pair_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as atr_21
    FROM h1_distinct
)

-- Combine all timeframes
SELECT * FROM m1_with_indicators
UNION ALL
SELECT * FROM m30_with_indicators
UNION ALL
SELECT * FROM h1_with_indicators
ORDER BY pair_name, timeframe, candle_time