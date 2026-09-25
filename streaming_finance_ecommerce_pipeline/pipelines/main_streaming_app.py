"""
Master Streaming Application Entrypoint

Coordinates and launches PySpark Structured Streaming jobs for Finance Fraud Detection
and E-Commerce Real-time Clickstream / Dynamic Pricing pipelines.

Usage:
    # Run both pipelines
    python -m streaming_finance_ecommerce_pipeline.pipelines.main_streaming_app --pipeline both

    # Run only finance fraud pipeline
    python -m streaming_finance_ecommerce_pipeline.pipelines.main_streaming_app --pipeline finance

    # Run only ecommerce pipeline
    python -m streaming_finance_ecommerce_pipeline.pipelines.main_streaming_app --pipeline ecommerce
"""

import sys
import os
import argparse
import logging
import signal
from typing import List
from pyspark.sql.streaming import StreamingQuery

from streaming_finance_ecommerce_pipeline.config.pipeline_config import PipelineConfig
from streaming_finance_ecommerce_pipeline.utils.spark_session_builder import create_spark_streaming_session
from streaming_finance_ecommerce_pipeline.pipelines.finance_fraud_pipeline import FinanceFraudPipeline
from streaming_finance_ecommerce_pipeline.pipelines.ecommerce_clickstream_pipeline import EcommerceStreamingPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("MainStreamingApp")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PySpark Kafka-Snowflake Streaming Pipeline")
    parser.add_argument(
        "--pipeline",
        choices=["finance", "ecommerce", "both"],
        default="both",
        help="Specify which streaming pipeline to execute (default: both)"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Custom path to pipeline_config.json"
    )
    parser.add_argument(
        "--master",
        type=str,
        default="local[*]",
        help="Spark master URL (default: local[*])"
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    logger.info("=================================================================")
    logger.info("   LAUNCHING REAL-TIME PYSPARK - KAFKA - SNOWFLAKE PIPELINE      ")
    logger.info(f"   Selected Pipeline Mode : {args.pipeline.upper()}               ")
    logger.info("=================================================================")

    # 1. Load Configurations
    config = PipelineConfig(args.config) if args.config else PipelineConfig()

    # 2. Build Spark Session
    spark = create_spark_streaming_session(
        app_name=f"Streaming_{args.pipeline.capitalize()}_Pipeline",
        spark_master=args.master
    )

    active_queries: List[StreamingQuery] = []

    def signal_handler(sig, frame):
        logger.warning("\n[STOP SIGNAL RECEIVED] Gracefully terminating streaming queries...")
        for q in active_queries:
            if q.isActive:
                logger.info(f"Stopping query: {q.name}")
                q.stop()
        spark.stop()
        logger.info("Spark session closed. Exiting.")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 3. Launch Finance Pipeline
        if args.pipeline in ("finance", "both"):
            logger.info("Initializing Financial Fraud Detection Pipeline...")
            finance_pipe = FinanceFraudPipeline(spark, config)
            q_finance = finance_pipe.start_stream()
            active_queries.append(q_finance)
            logger.info(f"Finance query started: {q_finance.name} [ID: {q_finance.id}]")

        # 4. Launch E-Commerce Pipelines
        if args.pipeline in ("ecommerce", "both"):
            logger.info("Initializing E-Commerce Clickstream & Dynamic Pricing Pipeline...")
            ecom_pipe = EcommerceStreamingPipeline(spark, config)

            # Query A: Raw clickstream and real-time cart tracking
            q_clickstream = ecom_pipe.start_clickstream_stream()
            active_queries.append(q_clickstream)
            logger.info(f"Clickstream query started: {q_clickstream.name} [ID: {q_clickstream.id}]")

            # Query B: Sliding window dynamic pricing demand surge
            q_pricing = ecom_pipe.start_dynamic_pricing_stream()
            active_queries.append(q_pricing)
            logger.info(f"Dynamic pricing query started: {q_pricing.name} [ID: {q_pricing.id}]")

        logger.info("\n>>> All streaming queries active. Awaiting new data microbatches... <<<")
        logger.info(">>> Press CTRL+C to terminate pipelines. <<<\n")

        # 5. Await termination
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Fatal error in streaming pipeline execution: {str(e)}", exc_info=True)
        for q in active_queries:
            if q.isActive:
                q.stop()
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
