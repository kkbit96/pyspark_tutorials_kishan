# Real-Time Streaming Data Pipeline: PySpark + Kafka + Snowflake

A production-grade, modular real-time streaming data pipeline leveraging **PySpark Structured Streaming**, **Apache Kafka**, and **Snowflake**.

This pipeline handles dual real-world domains:
1. **Financial Fraud Detection & Transaction Velocity**: Real-time fraud scoring, card velocity attacks, rapid micro-batch checks, and instant blocking of unauthorized charges.
2. **E-Commerce Real-Time Clickstream & Dynamic Pricing**: User interaction tracking (views, searches, cart activity, abandonment) with sliding window demand surge metrics for real-time dynamic pricing.

---

## 🏛 Architecture Diagram

```
+---------------------------------------------------------------------------------------+
|                                    EVENT PRODUCERS                                     |
|                                                                                       |
|   +---------------------------------------+   +-----------------------------------+   |
|   |         Financial Transactions        |   |         E-Commerce Events         |   |
|   |  (Card swipes, online payments,       |   |  (Clicks, cart adds, checkouts,   |   |
|   |   wire transfers, crypto purchases)   |   |   dwell times, product searches)  |   |
|   +---------------------------------------+   +-----------------------------------+   |
+-----------------------------------|-----------------------------------|---------------+
                                    |                                   |
                                    v                                   v
+---------------------------------------------------------------------------------------+
|                                     APACHE KAFKA                                      |
|                                                                                       |
|   Topic: financial_transactions               Topic: ecommerce_events                 |
+-----------------------------------|-----------------------------------|---------------+
                                    |                                   |
                                    v                                   v
+---------------------------------------------------------------------------------------+
|                              PYSPARK STRUCTURED STREAMING                              |
|                                                                                       |
|   [Finance Pipeline]                          [E-Commerce Pipeline]                   |
|   * JSON Deserialization & Validation         * Watermarked Event Stream (3 min)      |
|   * Event Watermarking (2 min)                * 5-Min Sliding Window Demand Surge     |
|   * Rule & Risk Scoring Engine                * Real-Time Conversion & Pricing Score  |
|   * Microbatch Velocity Analysis              * Session Cart & Abandonment Analytics  |
+-----------------------------------|-----------------------------------|---------------+
                                    | foreachBatch (Snowflake Connector)
                                    v
+---------------------------------------------------------------------------------------+
|                                       SNOWFLAKE                                       |
|                                                                                       |
|   * FINANCIAL_TRANSACTIONS (All enriched logs with risk scores)                       |
|   * FRAUD_ALERTS           (Critical & high-risk alerts for blocked charges)          |
|   * ECOMMERCE_CLICKSTREAM  (Sanitized user clickstream for recommendation models)     |
|   * DYNAMIC_PRICING_SIGNALS(Real-time surge multipliers by product/window)            |
|   * CART_ACTIVITY_SUMMARY  (Session cart velocity & abandonment risks)                |
+---------------------------------------------------------------------------------------+
```

---

## 📁 Project Directory Structure

```
streaming_finance_ecommerce_pipeline/
├── README.md                              # Pipeline documentation and operations guide
├── requirements.txt                       # Python dependencies
├── config/
│   ├── __init__.py
│   ├── pipeline_config.json               # Config file (Kafka, Snowflake, thresholds)
│   └── pipeline_config.py                 # Configuration loader with env var support
├── schemas/
│   ├── __init__.py
│   └── data_schemas.py                    # PySpark StructTypes for financial & ecom events
├── utils/
│   ├── __init__.py
│   ├── spark_session_builder.py           # SparkSession builder with Kafka & Snowflake JARs
│   └── snowflake_sink.py                  # foreachBatch microbatch Snowflake sink (with test staging)
├── producers/
│   ├── __init__.py
│   └── mock_stream_producer.py            # Stream generator simulating normal & fraud traffic
├── pipelines/
│   ├── __init__.py
│   ├── finance_fraud_pipeline.py          # Real-time financial fraud detection pipeline
│   ├── ecommerce_clickstream_pipeline.py  # E-commerce clickstream & dynamic pricing pipeline
│   └── main_streaming_app.py              # Master pipeline orchestrator runner
├── snowflake/
│   └── setup_tables.sql                   # Snowflake DDL tables, clustering, and views
└── tests/
    ├── __init__.py
    └── test_local_streaming.py            # Unit and transformation test suite
```

---

## 🔍 Pipeline Logic & Domain Details

### 1. Finance: Real-Time Fraud Detection
- **Schema**: `transaction_id`, `user_id`, `card_number_masked`, `amount`, `merchant_category`, `location_country`, `device_os`, `timestamp`, `is_international`.
- **Heuristic Scoring Engine**:
  - High amount rule: `amount >= $2,500.0` (+40 risk points)
  - High risk merchant category: `crypto`, `wire_transfer`, `gambling` (+30 risk points)
  - Cross-border international transactions (+20 risk points)
  - Suspicious / unknown device OS (+15 risk points)
- **Velocity Attacks**: Microbatch partition window tracking transactions per user/card. If $\ge 5$ transactions occur in a short window, risk escalates to `CRITICAL` and action taken is `BLOCKED`.
- **Output Routing**:
  - Full audit trail $\to$ `FINANCIAL_TRANSACTIONS`
  - High-risk / blocked actions $\to$ `FRAUD_ALERTS`

### 2. E-Commerce: Clickstream, Cart Velocity, & Dynamic Pricing
- **Schema**: `event_id`, `session_id`, `user_id`, `event_type` (`view`, `search`, `add_to_cart`, `remove_from_cart`, `checkout`), `product_id`, `category`, `price`, `quantity`, `dwell_time_seconds`, `referrer`, `timestamp`.
- **Sliding Window Surge Pricing Engine**:
  - 5-minute sliding window with 1-minute slide duration.
  - Aggregates `total_views` and `total_add_to_cart`.
  - Calculates `conversion_rate` and weighted `demand_intensity_score`.
  - Computes `demand_surge_multiplier` (e.g. 1.10x to 1.20x for surging products, 0.95x incentive discount for cold items).
- **Cart Abandonment Tracking**:
  - Tracks items added vs removed and checkout completion per session.
  - Flags `is_abandonment_risk` for real-time recovery push notifications.
- **Output Routing**:
  - Clickstream $\to$ `ECOMMERCE_CLICKSTREAM`
  - Windowed surge signals $\to$ `DYNAMIC_PRICING_SIGNALS`
  - Session cart states $\to$ `CART_ACTIVITY_SUMMARY`

---

## 🚀 How to Run the Pipeline

### Step 1: Install Dependencies
```bash
pip install -r streaming_finance_ecommerce_pipeline/requirements.txt
```

### Step 2: Initialize Snowflake Tables
Run the SQL script located at:
`streaming_finance_ecommerce_pipeline/snowflake/setup_tables.sql` in your Snowflake worksheet.

### Step 3: Configure Parameters
Edit `streaming_finance_ecommerce_pipeline/config/pipeline_config.json` or export environment variables:
```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SNOWFLAKE_URL="<your_account>.snowflakecomputing.com"
export SNOWFLAKE_USER="<your_user>"
export SNOWFLAKE_PASSWORD="<your_password>"
export SNOWFLAKE_DATABASE="STREAMING_ANALYTICS_DB"
export SNOWFLAKE_SCHEMA="PUBLIC"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
```

> **Note:** If Snowflake credentials remain as defaults (`your_account...`), the sink automatically activates **Local Mock Mode**, logging records to stdout and persisting Parquet files under `/tmp/snowflake_local_staging/` without crashing!

### Step 4: Run the Event Producer
Produce live events to Kafka topics or print to stdout:
```bash
# Publish events to Kafka:
python streaming_finance_ecommerce_pipeline/producers/mock_stream_producer.py --mode kafka --bootstrap-servers localhost:9092

# Or preview stream data in stdout:
python streaming_finance_ecommerce_pipeline/producers/mock_stream_producer.py --mode stdout --max-events 20
```

### Step 5: Launch the PySpark Streaming Application

Run via Python:
```bash
# Run both pipelines simultaneously
python -m streaming_finance_ecommerce_pipeline.pipelines.main_streaming_app --pipeline both

# Run only finance fraud pipeline
python -m streaming_finance_ecommerce_pipeline.pipelines.main_streaming_app --pipeline finance

# Run only ecommerce pipeline
python -m streaming_finance_ecommerce_pipeline.pipelines.main_streaming_app --pipeline ecommerce
```

Or submit to a Spark cluster using `spark-submit`:
```bash
spark-submit \
  --master "local[*]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:snowflake-jdbc:3.14.0,net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.5 \
  streaming_finance_ecommerce_pipeline/pipelines/main_streaming_app.py \
  --pipeline both
```
