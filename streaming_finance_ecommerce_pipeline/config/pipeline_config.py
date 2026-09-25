"""
Pipeline Configuration Manager

Handles configuration parameters for Kafka brokers, Snowflake credentials,
checkpoint paths, and stream processing windows. Supports JSON config
files as well as environment variable overrides.
"""

import os
import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "pipeline_config.json")


class PipelineConfig:
    """Manages streaming pipeline settings and credentials."""

    def __init__(self, config_file: str = DEFAULT_CONFIG_PATH):
        self.config_data: Dict[str, Any] = self._load_config(config_file)
        self._apply_env_overrides()

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Loads configuration from JSON file."""
        if os.path.exists(config_file):
            try:
                with open(config_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read {config_file}, using defaults: {e}")
        return self._get_fallback_defaults()

    def _get_fallback_defaults(self) -> Dict[str, Any]:
        """Provides default values if config file is absent."""
        return {
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "financial_topic": "financial_transactions",
                "ecommerce_topic": "ecommerce_events",
                "starting_offsets": "latest",
                "fail_on_data_loss": "false",
                "max_offsets_per_trigger": 10000,
            },
            "snowflake": {
                "sfURL": "your_account.snowflakecomputing.com",
                "sfUser": "your_username",
                "sfPassword": "your_password",
                "sfDatabase": "STREAMING_ANALYTICS_DB",
                "sfSchema": "PUBLIC",
                "sfWarehouse": "COMPUTE_WH",
                "sfRole": "ACCOUNTADMIN",
                "tables": {
                    "raw_financial_transactions": "FINANCIAL_TRANSACTIONS",
                    "fraud_alerts": "FRAUD_ALERTS",
                    "raw_ecommerce_events": "ECOMMERCE_CLICKSTREAM",
                    "dynamic_pricing_signals": "DYNAMIC_PRICING_SIGNALS",
                    "cart_activity_summary": "CART_ACTIVITY_SUMMARY",
                },
            },
            "pipeline": {
                "watermark_delay_finance": "2 minutes",
                "watermark_delay_ecommerce": "3 minutes",
                "finance_window_duration": "5 minutes",
                "finance_slide_duration": "1 minute",
                "ecommerce_window_duration": "5 minutes",
                "ecommerce_slide_duration": "1 minute",
                "checkpoint_base_dir": "/tmp/spark_checkpoints/streaming_pipeline",
                "trigger_interval": "10 seconds",
                "fraud_velocity_threshold_count": 5,
                "fraud_velocity_threshold_amount": 5000.0,
                "high_single_transaction_threshold": 2500.0,
                "dynamic_pricing_surge_threshold": 10,
            },
        }

    def _apply_env_overrides(self) -> None:
        """Allows environment variables to override sensitive or environment-specific values."""
        # Kafka Overrides
        if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
            self.config_data["kafka"]["bootstrap_servers"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        if "KAFKA_FINANCIAL_TOPIC" in os.environ:
            self.config_data["kafka"]["financial_topic"] = os.environ["KAFKA_FINANCIAL_TOPIC"]
        if "KAFKA_ECOMMERCE_TOPIC" in os.environ:
            self.config_data["kafka"]["ecommerce_topic"] = os.environ["KAFKA_ECOMMERCE_TOPIC"]

        # Snowflake Overrides
        sf_env_map = {
            "SNOWFLAKE_URL": "sfURL",
            "SNOWFLAKE_USER": "sfUser",
            "SNOWFLAKE_PASSWORD": "sfPassword",
            "SNOWFLAKE_DATABASE": "sfDatabase",
            "SNOWFLAKE_SCHEMA": "sfSchema",
            "SNOWFLAKE_WAREHOUSE": "sfWarehouse",
            "SNOWFLAKE_ROLE": "sfRole",
        }
        for env_var, sf_key in sf_env_map.items():
            if env_var in os.environ:
                self.config_data["snowflake"][sf_key] = os.environ[env_var]

        # Pipeline Overrides
        if "CHECKPOINT_BASE_DIR" in os.environ:
            self.config_data["pipeline"]["checkpoint_base_dir"] = os.environ["CHECKPOINT_BASE_DIR"]

    @property
    def kafka_options(self) -> Dict[str, Any]:
        """Returns Kafka configuration dictionary."""
        return self.config_data.get("kafka", {})

    @property
    def snowflake_options(self) -> Dict[str, Any]:
        """Returns base Snowflake connection options excluding internal table maps."""
        sf = self.config_data.get("snowflake", {}).copy()
        sf.pop("tables", None)
        return sf

    @property
    def snowflake_tables(self) -> Dict[str, str]:
        """Returns Snowflake target table mapping."""
        return self.config_data.get("snowflake", {}).get("tables", {})

    @property
    def pipeline_params(self) -> Dict[str, Any]:
        """Returns pipeline processing parameters."""
        return self.config_data.get("pipeline", {})

    def get_checkpoint_path(self, subquery_name: str) -> str:
        """Generates dedicated checkpoint directory for a streaming query."""
        base_dir = self.pipeline_params.get("checkpoint_base_dir", "/tmp/spark_checkpoints")
        return os.path.join(base_dir, subquery_name)
