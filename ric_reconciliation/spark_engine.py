"""
PySpark Reconciliation Engine.

Performs:
1. Distributed aggregation of 21M+ contract records by Business Case with Decimal precision.
2. Broadcast Join against the parsed RIC aggregated reference data.
3. Full outer reconciliation to capture exact matches, within-tolerance variances,
   breaches, and orphaned records (missing in RIC or missing in Plan).
4. Tolerance classification and executive tie-out KPI metrics.
"""

import logging
from typing import Dict, List, Any, Tuple
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    LongType,
    DoubleType
)

from ric_reconciliation.config import ReconciliationConfig, ToleranceConfig

logger = logging.getLogger(__name__)


class PySparkReconciliationEngine:
    """
    Production-grade reconciliation engine running on PySpark.
    """

    def __init__(self, spark: SparkSession, config: ReconciliationConfig):
        self.spark = spark
        self.config = config
        self._setup_spark_optimizations()

    def _setup_spark_optimizations(self):
        """Configure Spark runtime properties for high throughput and zero data loss."""
        # Enable Adaptive Query Execution (AQE) for dynamic partition coalescing & skew join handling
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(self.config.broadcast_threshold_bytes))
        self.spark.conf.set("spark.sql.shuffle.partitions", str(self.config.shuffle_partitions))

    def create_ric_dataframe(self, ric_records: List[Dict[str, Any]]) -> DataFrame:
        """
        Converts parsed Excel records into a strongly-typed PySpark DataFrame with Decimal precision.
        """
        schema = StructType([
            StructField("ric_business_case", StringType(), False),
            StructField("ric_premium", DecimalType(self.config.decimal_precision, self.config.decimal_scale), False),
            StructField("ric_claims", DecimalType(self.config.decimal_precision, self.config.decimal_scale), False),
            StructField("ric_commission", DecimalType(self.config.decimal_precision, self.config.decimal_scale), False),
        ])

        # Prepare tuples with Decimal conversion
        data = [
            (
                r["business_case"],
                Decimal(str(r["ric_premium"])),
                Decimal(str(r["ric_claims"])),
                Decimal(str(r["ric_commission"]))
            )
            for r in ric_records
        ]

        logger.info(f"Creating RIC Spark DataFrame with {len(data)} records.")
        return self.spark.createDataFrame(data, schema=schema)

    def aggregate_plan_rollforward(self, plan_df: DataFrame) -> DataFrame:
        """
        Aggregates 21M+ contract records up to the Business Case level.
        Enforces Decimal arithmetic to avoid floating-point drift across millions of rows.
        """
        logger.info("Executing distributed aggregation on Plan Rollforward dataset...")
        prec = self.config.decimal_precision
        scale = self.config.decimal_scale

        # Standardize column types to DecimalType
        standardized_plan = (
            plan_df
            .withColumn("business_case_clean", F.trim(F.upper(F.col("business_case"))))
            .withColumn("premium_dec", F.coalesce(F.col("premium").cast(DecimalType(prec, scale)), F.lit(Decimal("0.00"))))
            .withColumn("claims_dec", F.coalesce(F.col("claims").cast(DecimalType(prec, scale)), F.lit(Decimal("0.00"))))
            .withColumn("commission_dec", F.coalesce(F.col("commission").cast(DecimalType(prec, scale)), F.lit(Decimal("0.00"))))
        )

        aggregated = (
            standardized_plan
            .groupBy("business_case_clean")
            .agg(
                F.count(F.lit(1)).alias("plan_contract_count"),
                F.sum("premium_dec").cast(DecimalType(prec, scale)).alias("plan_premium"),
                F.sum("claims_dec").cast(DecimalType(prec, scale)).alias("plan_claims"),
                F.sum("commission_dec").cast(DecimalType(prec, scale)).alias("plan_commission")
            )
            .withColumnRenamed("business_case_clean", "plan_business_case")
        )

        return aggregated

    def reconcile(
        self,
        aggregated_plan_df: DataFrame,
        ric_df: DataFrame
    ) -> DataFrame:
        """
        Reconciles Aggregated Plan dataset with the RIC dataset.
        Uses Broadcast Join to eliminate shuffle for the small RIC dataset.
        Performs full outer join to detect missing business cases on either side.
        Computes absolute delta, relative variance (%), and tolerance status flags.
        """
        logger.info("Starting financial reconciliation with broadcast join...")
        tol: ToleranceConfig = self.config.tolerances
        prec = self.config.decimal_precision
        scale = self.config.decimal_scale

        # Full outer join on business case.
        # Note: Both aggregated_plan_df and ric_df are already at the business case grain
        # (typically a few dozen to a few thousand rows), making the join sub-second.
        joined_df = aggregated_plan_df.join(
            ric_df,
            aggregated_plan_df["plan_business_case"] == ric_df["ric_business_case"],
            how="full_outer"
        )

        # Unified Business Case column
        with_bc = joined_df.withColumn(
            "business_case",
            F.coalesce(F.col("plan_business_case"), F.col("ric_business_case"))
        )

        # Impute missing values with zero for delta calculation
        zero_val = F.lit(Decimal("0.00")).cast(DecimalType(prec, scale))

        clean_metrics = (
            with_bc
            .withColumn("plan_premium_safe", F.coalesce(F.col("plan_premium"), zero_val))
            .withColumn("ric_premium_safe", F.coalesce(F.col("ric_premium"), zero_val))
            .withColumn("plan_claims_safe", F.coalesce(F.col("plan_claims"), zero_val))
            .withColumn("ric_claims_safe", F.coalesce(F.col("ric_claims"), zero_val))
            .withColumn("plan_commission_safe", F.coalesce(F.col("plan_commission"), zero_val))
            .withColumn("ric_commission_safe", F.coalesce(F.col("ric_commission"), zero_val))
            .withColumn("plan_contract_count", F.coalesce(F.col("plan_contract_count"), F.lit(0).cast(LongType())))
        )

        # Compute Absolute Deltas (Plan - RIC)
        deltas_df = (
            clean_metrics
            .withColumn("delta_premium", (F.col("plan_premium_safe") - F.col("ric_premium_safe")).cast(DecimalType(prec, scale)))
            .withColumn("delta_claims", (F.col("plan_claims_safe") - F.col("ric_claims_safe")).cast(DecimalType(prec, scale)))
            .withColumn("delta_commission", (F.col("plan_commission_safe") - F.col("ric_commission_safe")).cast(DecimalType(prec, scale)))
            .withColumn("abs_delta_premium", F.abs(F.col("delta_premium")))
            .withColumn("abs_delta_claims", F.abs(F.col("delta_claims")))
            .withColumn("abs_delta_commission", F.abs(F.col("delta_commission")))
            .withColumn("total_abs_delta", (F.col("abs_delta_premium") + F.col("abs_delta_claims") + F.col("abs_delta_commission")).cast(DecimalType(prec, scale)))
        )

        # Percentage variance calculations (avoiding division by zero)
        pct_df = (
            deltas_df
            .withColumn(
                "delta_pct_premium",
                F.when(F.col("ric_premium_safe") != 0,
                       F.round((F.col("delta_premium") / F.col("ric_premium_safe")) * 100.0, 4))
                .otherwise(F.when(F.col("plan_premium_safe") != 0, F.lit(100.0)).otherwise(F.lit(0.0)))
            )
            .withColumn(
                "delta_pct_claims",
                F.when(F.col("ric_claims_safe") != 0,
                       F.round((F.col("delta_claims") / F.col("ric_claims_safe")) * 100.0, 4))
                .otherwise(F.when(F.col("plan_claims_safe") != 0, F.lit(100.0)).otherwise(F.lit(0.0)))
            )
            .withColumn(
                "delta_pct_commission",
                F.when(F.col("ric_commission_safe") != 0,
                       F.round((F.col("delta_commission") / F.col("ric_commission_safe")) * 100.0, 4))
                .otherwise(F.when(F.col("plan_commission_safe") != 0, F.lit(100.0)).otherwise(F.lit(0.0)))
            )
        )

        # Tolerance Conditions
        tol_prem_abs = F.lit(Decimal(str(tol.abs_tolerance_premium))).cast(DecimalType(prec, scale))
        tol_claims_abs = F.lit(Decimal(str(tol.abs_tolerance_claims))).cast(DecimalType(prec, scale))
        tol_comm_abs = F.lit(Decimal(str(tol.abs_tolerance_commission))).cast(DecimalType(prec, scale))

        is_exact_match = (
            (F.col("delta_premium") == 0) &
            (F.col("delta_claims") == 0) &
            (F.col("delta_commission") == 0)
        )

        is_within_tolerance = (
            (F.col("abs_delta_premium") <= tol_prem_abs) &
            (F.col("abs_delta_claims") <= tol_claims_abs) &
            (F.col("abs_delta_commission") <= tol_comm_abs)
        )

        # Status Tagging
        reconciled_df = (
            pct_df
            .withColumn(
                "recon_status",
                F.when(F.col("ric_business_case").isNull(), F.lit("ORPHAN_PLAN_ONLY"))
                .when(F.col("plan_business_case").isNull(), F.lit("ORPHAN_RIC_ONLY"))
                .when(is_exact_match, F.lit("MATCH_EXACT"))
                .when(is_within_tolerance, F.lit("MATCH_WITHIN_TOLERANCE"))
                .otherwise(F.lit("DISCREPANCY_BREACH"))
            )
            .withColumn(
                "breach_severity",
                F.when(F.col("recon_status") == "DISCREPANCY_BREACH",
                       F.when(F.col("total_abs_delta") > 10000.0, F.lit("CRITICAL"))
                       .when(F.col("total_abs_delta") > 100.0, F.lit("HIGH"))
                       .otherwise(F.lit("MEDIUM")))
                .when(F.col("recon_status").isin("ORPHAN_PLAN_ONLY", "ORPHAN_RIC_ONLY"), F.lit("CRITICAL"))
                .otherwise(F.lit("NONE"))
            )
            .select(
                "business_case",
                "recon_status",
                "breach_severity",
                "plan_contract_count",
                # Premiums
                F.col("plan_premium_safe").alias("plan_premium"),
                F.col("ric_premium_safe").alias("ric_premium"),
                "delta_premium",
                "delta_pct_premium",
                # Claims
                F.col("plan_claims_safe").alias("plan_claims"),
                F.col("ric_claims_safe").alias("ric_claims"),
                "delta_claims",
                "delta_pct_claims",
                # Commissions
                F.col("plan_commission_safe").alias("plan_commission"),
                F.col("ric_commission_safe").alias("ric_commission"),
                "delta_commission",
                "delta_pct_commission",
                # Summary Delta
                "total_abs_delta"
            )
            .orderBy(F.col("total_abs_delta").desc())
        )

        return reconciled_df

    def compute_executive_summary(self, reconciled_df: DataFrame) -> Dict[str, Any]:
        """
        Calculates executive KPIs and portfolio tie-out metrics across all business cases.
        """
        logger.info("Computing executive portfolio tie-out summary...")

        summary_row = (
            reconciled_df
            .agg(
                F.count(F.lit(1)).alias("total_business_cases"),
                F.sum(F.when(F.col("recon_status").isin("MATCH_EXACT", "MATCH_WITHIN_TOLERANCE"), 1).otherwise(0)).alias("cases_reconciled"),
                F.sum(F.when(F.col("recon_status") == "DISCREPANCY_BREACH", 1).otherwise(0)).alias("cases_breached"),
                F.sum(F.when(F.col("recon_status") == "ORPHAN_PLAN_ONLY", 1).otherwise(0)).alias("cases_plan_orphan"),
                F.sum(F.when(F.col("recon_status") == "ORPHAN_RIC_ONLY", 1).otherwise(0)).alias("cases_ric_orphan"),
                # Contract volume
                F.sum("plan_contract_count").alias("total_contracts"),
                # Financial sums
                F.sum("plan_premium").alias("total_plan_premium"),
                F.sum("ric_premium").alias("total_ric_premium"),
                F.sum("delta_premium").alias("net_premium_delta"),
                F.sum("plan_claims").alias("total_plan_claims"),
                F.sum("ric_claims").alias("total_ric_claims"),
                F.sum("delta_claims").alias("net_claims_delta"),
                F.sum("plan_commission").alias("total_plan_commission"),
                F.sum("ric_commission").alias("total_ric_commission"),
                F.sum("delta_commission").alias("net_commission_delta"),
                # Total Exposure
                F.sum("total_abs_delta").alias("gross_unreconciled_exposure")
            )
            .collect()[0]
        )

        total_cases = summary_row["total_business_cases"] or 0
        reconciled_cases = summary_row["cases_reconciled"] or 0
        match_rate_pct = round((reconciled_cases / total_cases * 100.0), 2) if total_cases > 0 else 0.0

        summary = {
            "total_business_cases": total_cases,
            "cases_reconciled": reconciled_cases,
            "cases_breached": summary_row["cases_breached"] or 0,
            "cases_plan_orphan": summary_row["cases_plan_orphan"] or 0,
            "cases_ric_orphan": summary_row["cases_ric_orphan"] or 0,
            "match_rate_pct": match_rate_pct,
            "total_contracts_processed": summary_row["total_contracts"] or 0,
            "total_plan_premium": summary_row["total_plan_premium"],
            "total_ric_premium": summary_row["total_ric_premium"],
            "net_premium_delta": summary_row["net_premium_delta"],
            "total_plan_claims": summary_row["total_plan_claims"],
            "total_ric_claims": summary_row["total_ric_claims"],
            "net_claims_delta": summary_row["net_claims_delta"],
            "total_plan_commission": summary_row["total_plan_commission"],
            "total_ric_commission": summary_row["total_ric_commission"],
            "net_commission_delta": summary_row["net_commission_delta"],
            "gross_unreconciled_exposure": summary_row["gross_unreconciled_exposure"]
        }

        return summary
