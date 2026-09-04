-- ============================================================================
-- Snowflake DDL: Retail Data Platform
--
-- Design: classic Kimball star schema.
--   - Fact tables store measurable events at a documented grain.
--   - Dimension tables store descriptive attributes, keyed by durable
--     surrogate keys (not the natural/business keys from source systems),
--     so history and slow changes can be tracked independently of
--     upstream ID reuse.
--
-- Layering:
--   RAW    -> landing zone for data loaded straight from S3/Spark (STG_*)
--   dbt then builds staging -> intermediate -> marts on top of RAW,
--   materializing FACT_/DIM_ tables in the ANALYTICS schema.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS RETAIL_PLATFORM;
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for retail platform ELT - small, auto-suspending to control cost';

CREATE SCHEMA IF NOT EXISTS RETAIL_PLATFORM.RAW;
CREATE SCHEMA IF NOT EXISTS RETAIL_PLATFORM.ANALYTICS;

USE DATABASE RETAIL_PLATFORM;
USE SCHEMA RAW;

-- ----------------------------------------------------------------------------
-- RAW staging tables (loaded by Spark / Airflow, 1:1 with S3 processed data)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RAW.STG_TRANSACTIONS (
    event_id                STRING,
    event_type              STRING,
    event_timestamp         TIMESTAMP_TZ,
    source                  STRING,
    transaction_id          STRING,
    customer_id             STRING,
    product_id              STRING,
    store_id                STRING,
    quantity                NUMBER,
    unit_price              NUMBER(12, 2),
    total_amount            NUMBER(12, 2),
    payment_method          STRING,
    transaction_timestamp   TIMESTAMP_TZ,
    transaction_type        STRING,
    _loaded_at              TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.STG_CUSTOMERS (
    customer_id         STRING,
    customer_name       STRING,
    age_group           STRING,
    postcode_area       STRING,
    customer_segment    STRING,
    registration_date   DATE,
    _loaded_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.STG_PRODUCTS (
    product_id      STRING,
    product_name     STRING,
    category         STRING,
    subcategory      STRING,
    brand            STRING,
    price            NUMBER(12, 2),
    cost             NUMBER(12, 2),
    stock_quantity   NUMBER,
    _loaded_at       TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.STG_STORES (
    store_id     STRING,
    store_name   STRING,
    city         STRING,
    region       STRING,
    latitude     FLOAT,
    longitude    FLOAT,
    _loaded_at   TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- ANALYTICS layer: dimensional model
-- (dbt materializes these; DDL shown here documents the intended shape and
--  grain for anyone reading the warehouse without dbt installed)
-- ----------------------------------------------------------------------------

USE SCHEMA ANALYTICS;

-- Grain: one row per unique product, current attributes only (Type-1 SCD).
CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_PRODUCT (
    product_sk       NUMBER AUTOINCREMENT PRIMARY KEY, -- surrogate key
    product_id       STRING NOT NULL,                   -- business key
    product_name     STRING,
    category         STRING,
    subcategory      STRING,
    brand            STRING,
    price            NUMBER(12, 2),
    cost             NUMBER(12, 2)
);

-- Grain: one row per unique customer, current attributes only (Type-1 SCD).
CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_CUSTOMER (
    customer_sk        NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id        STRING NOT NULL,
    customer_name      STRING,
    age_group          STRING,
    postcode_area      STRING,
    customer_segment   STRING,
    registration_date  DATE
);

-- Grain: one row per store.
CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_STORE (
    store_sk     NUMBER AUTOINCREMENT PRIMARY KEY,
    store_id     STRING NOT NULL,
    store_name   STRING,
    city         STRING,
    region       STRING,
    latitude     FLOAT,
    longitude    FLOAT
);

-- Grain: one row per calendar date.
CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_DATE (
    date_sk       NUMBER PRIMARY KEY,     -- YYYYMMDD
    full_date     DATE,
    day_of_week   STRING,
    day_number    NUMBER,
    month_number  NUMBER,
    month_name    STRING,
    quarter       NUMBER,
    year          NUMBER,
    is_weekend    BOOLEAN
);

-- Grain: one row per minute-of-day (00:00-23:59), used for hour/minute rollups.
CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_TIME (
    time_sk    NUMBER PRIMARY KEY,   -- HHMM
    hour       NUMBER,
    minute     NUMBER,
    period     STRING   -- 'Morning' | 'Afternoon' | 'Evening' | 'Night'
);

-- Grain: ONE ROW PER COMPLETED RETAIL TRANSACTION (a single line-level
-- purchase, refund, or cancellation event). Not aggregated.
CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_TRANSACTIONS (
    transaction_sk   NUMBER AUTOINCREMENT PRIMARY KEY,
    transaction_id   STRING NOT NULL,
    customer_sk      NUMBER REFERENCES ANALYTICS.DIM_CUSTOMER(customer_sk),
    product_sk       NUMBER REFERENCES ANALYTICS.DIM_PRODUCT(product_sk),
    store_sk         NUMBER REFERENCES ANALYTICS.DIM_STORE(store_sk),
    date_sk          NUMBER REFERENCES ANALYTICS.DIM_DATE(date_sk),
    time_sk          NUMBER REFERENCES ANALYTICS.DIM_TIME(time_sk),
    quantity         NUMBER,
    unit_price       NUMBER(12, 2),
    total_amount     NUMBER(12, 2),
    transaction_type STRING
);

-- Grain: ONE ROW PER PAYMENT ATTEMPT associated with a transaction.
CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_PAYMENTS (
    payment_sk       NUMBER AUTOINCREMENT PRIMARY KEY,
    transaction_id   STRING NOT NULL,
    payment_method   STRING,
    amount           NUMBER(12, 2),
    date_sk          NUMBER REFERENCES ANALYTICS.DIM_DATE(date_sk)
);

-- Grain: ONE ROW PER REFUND EVENT (subset of transactions where
-- transaction_type = 'refund').
CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_REFUNDS (
    refund_sk        NUMBER AUTOINCREMENT PRIMARY KEY,
    transaction_id   STRING NOT NULL,
    customer_sk      NUMBER REFERENCES ANALYTICS.DIM_CUSTOMER(customer_sk),
    product_sk       NUMBER REFERENCES ANALYTICS.DIM_PRODUCT(product_sk),
    amount           NUMBER(12, 2),
    date_sk          NUMBER REFERENCES ANALYTICS.DIM_DATE(date_sk)
);
