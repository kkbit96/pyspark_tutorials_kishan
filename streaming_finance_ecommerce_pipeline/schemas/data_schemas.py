"""
Data Schemas for Finance and E-Commerce Streaming Pipelines

Defines Spark SQL StructTypes used to deserialize Kafka message payloads,
enforce strict validation, drop malformed inputs, and maintain clean typing
across the pipeline.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
    TimestampType,
    ArrayType
)


# ==============================================================================
# 1. FINANCIAL TRANSACTIONS SCHEMA
# Used for real-time fraud detection, card velocity checks, and geovelocity analysis.
# ==============================================================================
FINANCIAL_TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("card_number_masked", StringType(), True),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),  # e.g., 'crypto', 'jewelry', 'groceries'
    StructField("location_country", StringType(), True),
    StructField("location_city", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_os", StringType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("is_international", BooleanType(), True)
])


# ==============================================================================
# 2. E-COMMERCE CLICKSTREAM & CART EVENTS SCHEMA
# Used for real-time click tracking, cart abandonment, dynamic pricing, and affinity.
# ==============================================================================
ECOMMERCE_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),       # 'view', 'search', 'add_to_cart', 'remove_from_cart', 'checkout'
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),          # e.g., 'electronics', 'fashion', 'home'
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("dwell_time_seconds", IntegerType(), True),
    StructField("referrer", StringType(), True),
    StructField("device_type", StringType(), True),       # 'mobile', 'desktop', 'tablet'
    StructField("ip_address", StringType(), True),
    StructField("timestamp", TimestampType(), False)
])


# ==============================================================================
# 3. HIGH-RISK FRAUD ALERT SCHEMA
# Enriched output format written to Snowflake FRAUD_ALERTS table.
# ==============================================================================
FRAUD_ALERT_SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("risk_score", DoubleType(), False),       # 0.0 to 100.0 scale
    StructField("risk_level", StringType(), False),       # 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    StructField("fraud_reasons", ArrayType(StringType()), True),
    StructField("velocity_count_5m", IntegerType(), True),
    StructField("velocity_amount_5m", DoubleType(), True),
    StructField("action_taken", StringType(), False),     # 'BLOCKED', 'FLAGGED_FOR_REVIEW', 'NOTIFY_USER'
    StructField("alert_timestamp", TimestampType(), False)
])


# ==============================================================================
# 4. DYNAMIC PRICING & DEMAND SURGE SCHEMA
# Aggregated metrics per product window written to Snowflake DYNAMIC_PRICING_SIGNALS.
# ==============================================================================
DYNAMIC_PRICING_SCHEMA = StructType([
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), True),
    StructField("total_views", IntegerType(), False),
    StructField("total_add_to_cart", IntegerType(), False),
    StructField("conversion_rate", DoubleType(), True),
    StructField("demand_surge_multiplier", DoubleType(), False),
    StructField("recommended_price_adjustment", DoubleType(), True),
    StructField("calculated_at", TimestampType(), False)
])
