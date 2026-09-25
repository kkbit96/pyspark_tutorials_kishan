"""
Real-Time E-Commerce Clickstream, Cart Velocity, & Dynamic Pricing Pipeline

Consumes live clickstream events from Apache Kafka (Amazon/Shopify style),
tracks user interaction sequences, computes sliding window demand surge metrics
for dynamic pricing, identifies cart activity/abandonment patterns, and streams
enriched datasets into Snowflake tables.
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, when, lit, current_timestamp,
    sum as spark_sum, count, window, round as spark_round, expr
)
from pyspark.sql.streaming import StreamingQuery

from streaming_finance_ecommerce_pipeline.config.pipeline_config import PipelineConfig
from streaming_finance_ecommerce_pipeline.schemas.data_schemas import (
    ECOMMERCE_EVENT_SCHEMA
)
from streaming_finance_ecommerce_pipeline.utils.snowflake_sink import SnowflakeStreamSink

logger = logging.getLogger(__name__)


class EcommerceStreamingPipeline:
    """
    Structured Streaming pipeline for e-commerce event ingestion,
    windowed dynamic pricing signal computation, and Snowflake loading.
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
        Creates streaming DataFrame subscribed to Kafka ecommerce events topic.
        """
        logger.info(f"Connecting to Kafka topic '{self.kafka_opts['ecommerce_topic']}' "
                    f"at {self.kafka_opts['bootstrap_servers']}...")

        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_opts["bootstrap_servers"])
            .option("subscribe", self.kafka_opts["ecommerce_topic"])
            .option("startingOffsets", self.kafka_opts.get("starting_offsets", "latest"))
            .option("failOnDataLoss", self.kafka_opts.get("fail_on_data_loss", "false"))
            .option("maxOffsetsPerTrigger", self.kafka_opts.get("max_offsets_per_trigger", 10000))
            .load()
        )

    def parse_and_watermark_events(self, raw_kafka_df: DataFrame) -> DataFrame:
        """
        Extracts JSON payload, enforces schema, and applies watermark on event timestamp.
        """
        parsed_df = (
            raw_kafka_df
            .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_arrival_time")
            .select(from_json(col("json_value"), ECOMMERCE_EVENT_SCHEMA).alias("evt"))
            .select("evt.*")
            .filter(col("event_id").isNotNull() & col("event_type").isNotNull())
        )

        watermark_delay = self.pipeline_params.get("watermark_delay_ecommerce", "3 minutes")
        return parsed_df.withWatermark("timestamp", watermark_delay)

    def compute_dynamic_pricing_signals(self, event_stream_df: DataFrame) -> DataFrame:
        """
        Calculates sliding window demand velocity for products to generate real-time
        dynamic pricing surge signals.
        """
        window_duration = self.pipeline_params.get("ecommerce_window_duration", "5 minutes")
        slide_duration = self.pipeline_params.get("ecommerce_slide_duration", "1 minute")
        surge_threshold = self.pipeline_params.get("dynamic_pricing_surge_threshold", 10)

        aggregated_df = (
            event_stream_df
            .filter(col("product_id").isNotNull())
            .groupBy(
                window(col("timestamp"), window_duration, slide_duration),
                col("product_id"),
                col("category")
            )
            .agg(
                spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
                spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("total_add_to_cart"),
                spark_sum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("total_checkouts")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("product_id"),
                col("category"),
                col("total_views"),
                col("total_add_to_cart"),
                # Conversion rate (add to cart vs views)
                spark_round(
                    when(col("total_views") > 0, col("total_add_to_cart") / col("total_views"))
                    .otherwise(lit(0.0)), 4
                ).alias("conversion_rate"),
                # Demand Surge Score: weighted combination of views and cart additions
                (col("total_views") + col("total_add_to_cart") * 3).alias("demand_intensity_score")
            )
            # Dynamic pricing multiplier: e.g., 1.05x to 1.25x based on high velocity demand
            .withColumn(
                "demand_surge_multiplier",
                when(col("demand_intensity_score") >= surge_threshold * 2, lit(1.20))
                .when(col("demand_intensity_score") >= surge_threshold, lit(1.10))
                .when(col("demand_intensity_score") <= 2, lit(0.95))  # Discount incentive for cold products
                .otherwise(lit(1.00))
            )
            .withColumn(
                "recommended_price_adjustment",
                spark_round((col("demand_surge_multiplier") - lit(1.0)) * 100, 2)
            )
            .withColumn("calculated_at", current_timestamp())
            .drop("demand_intensity_score")
        )

        return aggregated_df

    def process_clickstream_microbatch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Microbatch sink for raw clickstream events and session-level cart summary.
        """
        if batch_df.isEmpty():
            logger.debug(f"[Ecommerce Stream Batch {batch_id}] Empty batch. Skipping.")
            return

        logger.info(f"[Ecommerce Stream Batch {batch_id}] Processing {batch_df.count()} clickstream events...")

        batch_df.cache()
        try:
            # 1. Write raw clickstream events to Snowflake
            clickstream_table = self.tables.get("raw_ecommerce_events", "ECOMMERCE_CLICKSTREAM")
            enriched_clickstream = batch_df.withColumn("ingested_at", current_timestamp())
            self.sink.write_microbatch(
                enriched_clickstream,
                batch_id,
                target_table=clickstream_table,
                write_mode="append"
            )

            # 2. Compute session cart metrics within the microbatch
            cart_events = batch_df.filter(col("event_type").isin("add_to_cart", "remove_from_cart", "checkout"))
            if not cart_events.isEmpty():
                cart_summary = (
                    cart_events
                    .groupBy("session_id", "user_id")
                    .agg(
                        spark_sum(when(col("event_type") == "add_to_cart", col("quantity")).otherwise(0)).alias("items_added"),
                        spark_sum(when(col("event_type") == "remove_from_cart", col("quantity")).otherwise(0)).alias("items_removed"),
                        spark_sum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("checkouts_completed"),
                        spark_sum(when(col("event_type") == "add_to_cart", col("price") * col("quantity")).otherwise(0.0)).alias("total_cart_value")
                    )
                    .withColumn(
                        "is_abandonment_risk",
                        when((col("items_added") > col("items_removed")) & (col("checkouts_completed") == 0), lit(True))
                        .otherwise(lit(False))
                    )
                    .withColumn("updated_at", current_timestamp())
                )

                cart_table = self.tables.get("cart_activity_summary", "CART_ACTIVITY_SUMMARY")
                self.sink.write_microbatch(
                    cart_summary,
                    batch_id,
                    target_table=cart_table,
                    write_mode="append"
                )
        finally:
            batch_df.unpersist()

    def process_dynamic_pricing_microbatch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Microbatch sink for windowed dynamic pricing surge signals.
        """
        if batch_df.isEmpty():
            return

        pricing_table = self.tables.get("dynamic_pricing_signals", "DYNAMIC_PRICING_SIGNALS")
        logger.info(f"[Dynamic Pricing Batch {batch_id}] Writing {batch_df.count()} pricing signals to '{pricing_table}'...")
        self.sink.write_microbatch(
            batch_df,
            batch_id,
            target_table=pricing_table,
            write_mode="append"
        )

    def start_clickstream_stream(self, custom_checkpoint_dir: Optional[str] = None) -> StreamingQuery:
        """
        Starts the event ingestion and cart analytics streaming query.
        """
        checkpoint_dir = custom_checkpoint_dir or self.config.get_checkpoint_path("ecommerce_clickstream_query")
        trigger_time = self.pipeline_params.get("trigger_interval", "10 seconds")

        logger.info(f"Starting Ecommerce Clickstream Stream with checkpoint: {checkpoint_dir}")
        raw_stream = self.read_kafka_stream()
        parsed_stream = self.parse_and_watermark_events(raw_stream)

        return (
            parsed_stream.writeStream
            .queryName("EcommerceClickstreamStream")
            .trigger(processingTime=trigger_time)
            .foreachBatch(self.process_clickstream_microbatch)
            .option("checkpointLocation", checkpoint_dir)
            .start()
        )

    def start_dynamic_pricing_stream(self, custom_checkpoint_dir: Optional[str] = None) -> StreamingQuery:
        """
        Starts the windowed dynamic pricing demand surge streaming query.
        """
        checkpoint_dir = custom_checkpoint_dir or self.config.get_checkpoint_path("ecommerce_dynamic_pricing_query")
        trigger_time = self.pipeline_params.get("trigger_interval", "15 seconds")

        logger.info(f"Starting Dynamic Pricing Windowed Stream with checkpoint: {checkpoint_dir}")
        raw_stream = self.read_kafka_stream()
        parsed_stream = self.parse_and_watermark_events(raw_stream)
        pricing_signals = self.compute_dynamic_pricing_signals(parsed_stream)

        return (
            pricing_signals.writeStream
            .queryName("EcommerceDynamicPricingStream")
            .trigger(processingTime=trigger_time)
            .foreachBatch(self.process_dynamic_pricing_microbatch)
            .option("checkpointLocation", checkpoint_dir)
            .outputMode("update")
            .start()
        )
