"""
Real-Time Financial Fraud Detection & Transaction Velocity Pipeline

Consumes live financial transaction events from Apache Kafka, applies
real-time fraud detection heuristics and velocity windowing, assigns risk
scores, flags or blocks unauthorized transactions instantly, and sinks
results into Snowflake (FINANCIAL_TRANSACTIONS and FRAUD_ALERTS tables).
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, when, lit, array, array_compact, current_timestamp,
    concat, expr, count, sum as spark_sum, max as spark_max, window
)
from pyspark.sql.window import Window
from pyspark.sql.streaming import StreamingQuery

from streaming_finance_ecommerce_pipeline.config.pipeline_config import PipelineConfig
from streaming_finance_ecommerce_pipeline.schemas.data_schemas import (
    FINANCIAL_TRANSACTION_SCHEMA
)
from streaming_finance_ecommerce_pipeline.utils.snowflake_sink import SnowflakeStreamSink

logger = logging.getLogger(__name__)


class FinanceFraudPipeline:
    """
    Structured Streaming pipeline for real-time transaction scoring,
    fraud detection, and Snowflake ingestion.
    """

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.kafka_opts = config.kafka_options
        self.pipeline_params = config.pipeline_params
        self.tables = config.snowflake_tables
        self.sink = SnowflakeStreamSink(config.snowflake_options)

    def read_kafka_stream(self) -> DataFrame:
        """
        Creates streaming DataFrame connected to Kafka topic for financial transactions.
        """
        logger.info(f"Connecting to Kafka topic '{self.kafka_opts['financial_topic']}' "
                    f"at {self.kafka_opts['bootstrap_servers']}...")

        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_opts["bootstrap_servers"])
            .option("subscribe", self.kafka_opts["financial_topic"])
            .option("startingOffsets", self.kafka_opts.get("starting_offsets", "latest"))
            .option("failOnDataLoss", self.kafka_opts.get("fail_on_data_loss", "false"))
            .option("maxOffsetsPerTrigger", self.kafka_opts.get("max_offsets_per_trigger", 10000))
            .load()
        )

    def transform_and_score_transactions(self, raw_kafka_df: DataFrame) -> DataFrame:
        """
        Extracts JSON payload, enforces schema, applies watermarking, and calculates
        real-time fraud risk scores and action triggers.
        """
        # 1. Deserialize Kafka value from JSON
        parsed_df = (
            raw_kafka_df
            .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_arrival_time")
            .select(from_json(col("json_value"), FINANCIAL_TRANSACTION_SCHEMA).alias("tx"))
            .select("tx.*")
            .filter(col("transaction_id").isNotNull() & col("amount").isNotNull())
        )

        # 2. Watermark on transaction event timestamp
        watermark_delay = self.pipeline_params.get("watermark_delay_finance", "2 minutes")
        watermarked_df = parsed_df.withWatermark("timestamp", watermark_delay)

        # 3. Anomaly Rules & Heuristics Scoring
        high_amount_limit = self.pipeline_params.get("high_single_transaction_threshold", 2500.0)

        # Build fraud reason tags array
        enriched_df = watermarked_df.withColumn(
            "rule_high_amount",
            when(col("amount") >= high_amount_limit, lit("HIGH_AMOUNT_EXCEEDED")).otherwise(lit(None))
        ).withColumn(
            "rule_high_risk_category",
            when(col("merchant_category").isin("crypto", "wire_transfer", "gambling", "luxury_goods"),
                 lit("HIGH_RISK_MERCHANT")).otherwise(lit(None))
        ).withColumn(
            "rule_international",
            when(col("is_international") == True, lit("INTERNATIONAL_CROSS_BORDER")).otherwise(lit(None))
        ).withColumn(
            "rule_suspicious_device",
            when(col("device_os").isNull() | (col("device_os") == "unknown"),
                 lit("UNIDENTIFIED_DEVICE")).otherwise(lit(None))
        )

        # Aggregate reasons into an array
        enriched_df = enriched_df.withColumn(
            "fraud_reasons",
            array_compact(array(
                col("rule_high_amount"),
                col("rule_high_risk_category"),
                col("rule_international"),
                col("rule_suspicious_device")
            ))
        )

        # Compute Composite Risk Score (0.0 - 100.0)
        risk_score_expr = (
            lit(10.0)
            + when(col("amount") >= high_amount_limit, lit(40.0)).otherwise(lit(0.0))
            + when(col("merchant_category").isin("crypto", "wire_transfer", "gambling"), lit(30.0)).otherwise(lit(0.0))
            + when(col("is_international") == True, lit(20.0)).otherwise(lit(0.0))
            + when(col("device_os").isNull() | (col("device_os") == "unknown"), lit(15.0)).otherwise(lit(0.0))
        )

        scored_df = (
            enriched_df
            .withColumn("risk_score", expr(f"least(100.0, {risk_score_expr.expr})"))
            .withColumn(
                "risk_level",
                when(col("risk_score") >= 75.0, lit("CRITICAL"))
                .when(col("risk_score") >= 50.0, lit("HIGH"))
                .when(col("risk_score") >= 30.0, lit("MEDIUM"))
                .otherwise(lit("LOW"))
            )
            .withColumn(
                "action_taken",
                when(col("risk_level") == "CRITICAL", lit("BLOCKED"))
                .when(col("risk_level") == "HIGH", lit("FLAGGED_FOR_REVIEW"))
                .when(col("risk_level") == "MEDIUM", lit("NOTIFY_USER"))
                .otherwise(lit("APPROVED"))
            )
            .withColumn("processed_at", current_timestamp())
            .drop("rule_high_amount", "rule_high_risk_category", "rule_international", "rule_suspicious_device")
        )

        return scored_df

    def process_microbatch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Microbatch processor that:
        1. Analyzes rapid transaction velocity per user within the microbatch
        2. Routes all transactions to Snowflake FINANCIAL_TRANSACTIONS
        3. Filters high-risk / blocked events to Snowflake FRAUD_ALERTS
        """
        if batch_df.isEmpty():
            logger.debug(f"[Finance Stream Batch {batch_id}] Empty batch. Skipping.")
            return

        logger.info(f"[Finance Stream Batch {batch_id}] Processing {batch_df.count()} financial records...")

        # Cache micro-batch to prevent duplicate evaluations
        batch_df.cache()

        try:
            # Calculate microbatch velocity (e.g. multiple rapid transactions from same user/card)
            window_user = Window.partitionBy("user_id")
            velocity_df = (
                batch_df
                .withColumn("velocity_count_microbatch", count("transaction_id").over(window_user))
                .withColumn("velocity_amount_microbatch", spark_sum("amount").over(window_user))
            )

            # Escalate risk if velocity count exceeds threshold in this microbatch
            velocity_threshold = self.pipeline_params.get("fraud_velocity_threshold_count", 5)
            final_batch_df = velocity_df.withColumn(
                "action_taken",
                when(col("velocity_count_microbatch") >= velocity_threshold, lit("BLOCKED"))
                .otherwise(col("action_taken"))
            ).withColumn(
                "risk_level",
                when(col("velocity_count_microbatch") >= velocity_threshold, lit("CRITICAL"))
                .otherwise(col("risk_level"))
            )

            # Target 1: Store complete enriched transaction audit trail
            tx_table = self.tables.get("raw_financial_transactions", "FINANCIAL_TRANSACTIONS")
            self.sink.write_microbatch(
                final_batch_df.drop("velocity_count_microbatch", "velocity_amount_microbatch"),
                batch_id,
                target_table=tx_table,
                write_mode="append"
            )

            # Target 2: Filter for real-time critical fraud alerts
            fraud_alerts_df = (
                final_batch_df
                .filter(col("action_taken").isin("BLOCKED", "FLAGGED_FOR_REVIEW"))
                .select(
                    concat(lit("ALT-"), col("transaction_id")).alias("alert_id"),
                    col("transaction_id"),
                    col("user_id"),
                    col("amount"),
                    col("risk_score"),
                    col("risk_level"),
                    col("fraud_reasons"),
                    col("velocity_count_microbatch").alias("velocity_count_5m"),
                    col("velocity_amount_microbatch").alias("velocity_amount_5m"),
                    col("action_taken"),
                    col("processed_at").alias("alert_timestamp")
                )
            )

            fraud_count = fraud_alerts_df.count()
            if fraud_count > 0:
                logger.warning(f"[Finance Stream Batch {batch_id}] ⚠️ DETECTED {fraud_count} FRAUD ALERTS!")
                alerts_table = self.tables.get("fraud_alerts", "FRAUD_ALERTS")
                self.sink.write_microbatch(
                    fraud_alerts_df,
                    batch_id,
                    target_table=alerts_table,
                    write_mode="append"
                )

        finally:
            batch_df.unpersist()

    def start_stream(self, custom_checkpoint_dir: Optional[str] = None) -> StreamingQuery:
        """
        Launches the financial fraud structured stream.
        """
        checkpoint_dir = custom_checkpoint_dir or self.config.get_checkpoint_path("finance_fraud_query")
        trigger_time = self.pipeline_params.get("trigger_interval", "10 seconds")

        logger.info(f"Starting Finance Fraud Stream with checkpoint: {checkpoint_dir}")

        raw_stream = self.read_kafka_stream()
        scored_stream = self.transform_and_score_transactions(raw_stream)

        query = (
            scored_stream.writeStream
            .queryName("FinancialFraudStream")
            .trigger(processingTime=trigger_time)
            .foreachBatch(self.process_microbatch)
            .option("checkpointLocation", checkpoint_dir)
            .start()
        )

        return query
