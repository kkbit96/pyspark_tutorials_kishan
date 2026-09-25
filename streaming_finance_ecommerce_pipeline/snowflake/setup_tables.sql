-- =============================================================================
-- Snowflake DDL & Setup Script for Real-Time Streaming Data Pipeline
-- Integrates PySpark Structured Streaming, Kafka, and Snowflake.
-- Supports:
--   1. Financial Fraud Detection & Transaction Velocity
--   2. E-Commerce Clickstream, Cart Abandonment & Dynamic Pricing Surge
-- =============================================================================

-- 1. Create Database, Schema, and Warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS STREAMING_ANALYTICS_DB;
USE DATABASE STREAMING_ANALYTICS_DB;

CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;


-- =============================================================================
-- 2. FINANCE TABLES
-- =============================================================================

-- Table 1: Complete Financial Transaction Log with Risk Scores
CREATE TABLE IF NOT EXISTS FINANCIAL_TRANSACTIONS (
    transaction_id          VARCHAR(64) NOT NULL,
    user_id                 VARCHAR(64) NOT NULL,
    card_number_masked      VARCHAR(32),
    amount                  NUMBER(18, 2) NOT NULL,
    currency                VARCHAR(10) DEFAULT 'USD',
    merchant_id             VARCHAR(64),
    merchant_name           VARCHAR(128),
    merchant_category       VARCHAR(64),
    location_country        VARCHAR(32),
    location_city           VARCHAR(64),
    ip_address              VARCHAR(64),
    device_id               VARCHAR(64),
    device_os               VARCHAR(32),
    timestamp               TIMESTAMP_NTZ NOT NULL,
    is_international        BOOLEAN,
    fraud_reasons           ARRAY,
    risk_score              FLOAT,
    risk_level              VARCHAR(32),
    action_taken            VARCHAR(32),
    processed_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_transactions PRIMARY KEY (transaction_id)
)
CLUSTER BY (DATE(timestamp), user_id);


-- Table 2: High-Risk Real-Time Fraud Alerts (Blocked or Flagged for Manual Review)
CREATE TABLE IF NOT EXISTS FRAUD_ALERTS (
    alert_id                VARCHAR(64) NOT NULL,
    transaction_id          VARCHAR(64) NOT NULL,
    user_id                 VARCHAR(64) NOT NULL,
    amount                  NUMBER(18, 2) NOT NULL,
    risk_score              FLOAT NOT NULL,
    risk_level              VARCHAR(32) NOT NULL,
    fraud_reasons           ARRAY,
    velocity_count_5m       INT,
    velocity_amount_5m      NUMBER(18, 2),
    action_taken            VARCHAR(32) NOT NULL,
    alert_timestamp         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fraud_alerts PRIMARY KEY (alert_id)
)
CLUSTER BY (DATE(alert_timestamp), risk_level);


-- =============================================================================
-- 3. E-COMMERCE TABLES
-- =============================================================================

-- Table 3: E-Commerce Clickstream & Navigation Activity
CREATE TABLE IF NOT EXISTS ECOMMERCE_CLICKSTREAM (
    event_id                VARCHAR(64) NOT NULL,
    session_id              VARCHAR(64) NOT NULL,
    user_id                 VARCHAR(64) NOT NULL,
    event_type              VARCHAR(32) NOT NULL,
    product_id              VARCHAR(64),
    product_name            VARCHAR(128),
    category                VARCHAR(64),
    price                   NUMBER(18, 2),
    quantity                INT DEFAULT 0,
    dwell_time_seconds      INT DEFAULT 0,
    referrer                VARCHAR(128),
    device_type             VARCHAR(32),
    ip_address              VARCHAR(64),
    timestamp               TIMESTAMP_NTZ NOT NULL,
    ingested_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_clickstream PRIMARY KEY (event_id)
)
CLUSTER BY (DATE(timestamp), category);


-- Table 4: Dynamic Pricing Signals & Real-time Demand Surge
CREATE TABLE IF NOT EXISTS DYNAMIC_PRICING_SIGNALS (
    window_start            TIMESTAMP_NTZ NOT NULL,
    window_end              TIMESTAMP_NTZ NOT NULL,
    product_id              VARCHAR(64) NOT NULL,
    category                VARCHAR(64),
    total_views             INT NOT NULL,
    total_add_to_cart       INT NOT NULL,
    conversion_rate         FLOAT,
    demand_surge_multiplier FLOAT NOT NULL,
    recommended_price_adjustment FLOAT,
    calculated_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE(window_start), product_id);


-- Table 5: Real-Time User Cart Activity & Abandonment Risk Tracking
CREATE TABLE IF NOT EXISTS CART_ACTIVITY_SUMMARY (
    session_id              VARCHAR(64) NOT NULL,
    user_id                 VARCHAR(64) NOT NULL,
    items_added             INT DEFAULT 0,
    items_removed           INT DEFAULT 0,
    checkouts_completed     INT DEFAULT 0,
    total_cart_value        NUMBER(18, 2) DEFAULT 0.0,
    is_abandonment_risk     BOOLEAN DEFAULT FALSE,
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_cart_summary PRIMARY KEY (session_id, user_id)
);


-- =============================================================================
-- 4. ANALYTIC VIEWS
-- =============================================================================

-- Real-time Fraud Executive Summary View
CREATE OR REPLACE VIEW V_REALTIME_FRAUD_MONITOR AS
SELECT
    DATE_TRUNC('hour', alert_timestamp) AS alert_hour,
    risk_level,
    action_taken,
    COUNT(*) AS total_incidents,
    SUM(amount) AS total_blocked_amount,
    AVG(risk_score) AS avg_risk_score
FROM FRAUD_ALERTS
GROUP BY 1, 2, 3
ORDER BY alert_hour DESC;

-- Real-time Top Surging Products for Dynamic Pricing
CREATE OR REPLACE VIEW V_TOP_SURGE_PRODUCTS AS
SELECT
    product_id,
    category,
    demand_surge_multiplier,
    recommended_price_adjustment,
    total_views,
    total_add_to_cart,
    calculated_at
FROM DYNAMIC_PRICING_SIGNALS
WHERE window_end >= DATEADD('minute', -30, CURRENT_TIMESTAMP())
ORDER BY demand_surge_multiplier DESC, total_add_to_cart DESC;
