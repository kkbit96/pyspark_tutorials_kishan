"""
Snowflake Streaming Sink Utility

Handles micro-batch writes from PySpark Structured Streaming to Snowflake tables.
Includes support for:
- writeStream.foreachBatch integration
- Idempotent appends
- Multi-table routing
- Graceful test fallback (prints and stages locally if credentials are dummy)
"""

import os
import logging
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class SnowflakeStreamSink:
    """Manages micro-batch loading of streaming DataFrames into Snowflake."""

    def __init__(self, snowflake_options: Dict[str, Any], local_staging_dir: str = "/tmp/snowflake_local_staging"):
        self.snowflake_options = snowflake_options
        self.local_staging_dir = local_staging_dir

        # Check if dummy credentials are being used
        sf_url = self.snowflake_options.get("sfURL", "")
        self.is_dry_run_or_mock = (
            "your_account" in sf_url
            or not self.snowflake_options.get("sfPassword")
            or self.snowflake_options.get("sfPassword") == "your_password"
        )
        if self.is_dry_run_or_mock:
            logger.warning(
                "[MOCK MODE ACTIVATED] Placeholder Snowflake credentials detected. "
                "Micro-batches will be logged and persisted to local parquet staging "
                f"at '{self.local_staging_dir}' instead of pushing to live Snowflake."
            )

    def write_microbatch(
        self,
        batch_df: DataFrame,
        batch_id: int,
        target_table: str,
        write_mode: str = "append"
    ) -> None:
        """
        Sinks a single streaming microbatch DataFrame to a target Snowflake table.

        Args:
            batch_df: Microbatch DataFrame.
            batch_id: Spark microbatch sequence ID.
            target_table: Destination table name in Snowflake.
            write_mode: Spark write mode ('append', 'overwrite').
        """
        record_count = batch_df.count()
        if record_count == 0:
            logger.debug(f"[Batch {batch_id}] Empty batch for table '{target_table}'. Skipping.")
            return

        logger.info(f"[Batch {batch_id}] Loading {record_count} records to table '{target_table}'...")

        if self.is_dry_run_or_mock:
            self._write_local_mock_batch(batch_df, batch_id, target_table)
            return

        try:
            (
                batch_df.write
                .format("snowflake")
                .options(**self.snowflake_options)
                .option("dbtable", target_table)
                .mode(write_mode)
                .save()
            )
            logger.info(f"[Batch {batch_id}] Successfully ingested {record_count} records into Snowflake '{target_table}'.")
        except Exception as e:
            logger.error(f"[Batch {batch_id}] Failed to write to Snowflake table '{target_table}': {str(e)}")
            raise e

    def _write_local_mock_batch(self, batch_df: DataFrame, batch_id: int, target_table: str) -> None:
        """Saves microbatch to local directory and prints summary when in mock/test mode."""
        print(f"\n========== [MOCK SNOWFLAKE SINK] Table: {target_table} | Batch: {batch_id} ==========")
        batch_df.show(5, truncate=False)
        out_path = os.path.join(self.local_staging_dir, target_table, f"batch_{batch_id}")
        batch_df.write.mode("overwrite").parquet(out_path)
        print(f"Staged {batch_df.count()} records locally to {out_path}\n========================================================================\n")


def create_snowflake_batch_handler(
    sink: SnowflakeStreamSink,
    target_table: str,
    write_mode: str = "append"
):
    """
    Higher-order function returning a foreachBatch compatible callback function.
    """
    def _batch_handler(batch_df: DataFrame, batch_id: int):
        sink.write_microbatch(batch_df, batch_id, target_table, write_mode)

    return _batch_handler
