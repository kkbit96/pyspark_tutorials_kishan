"""
Main CLI & Pipeline Driver for RIC vs Plan Rollforward Reconciliation.

Executes end-to-end reconciliation:
1. Fast streaming extraction of aggregated RIC Excel figures.
2. Distributed aggregation of Plan Rollforward contracts in PySpark.
3. Broadcast Hash Join reconciliation with Decimal precision.
4. Variance calculation, threshold evaluation, and executive audit dashboard.
"""

import argparse
import logging
import os
import sys
from decimal import Decimal

from pyspark.sql import SparkSession

from ric_reconciliation.config import ReconciliationConfig, ToleranceConfig
from ric_reconciliation.excel_parser import extract_ric_data_as_dicts
from ric_reconciliation.spark_engine import PySparkReconciliationEngine
from ric_reconciliation.reporter import ReconciliationReporter
from ric_reconciliation.plan_data_generator import (
    generate_ric_excel,
    generate_plan_rollforward_parquet
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("RICReconciliationPipeline")


def configure_environment():
    """Ensures valid JAVA_HOME is configured on macOS Apple Silicon if needed."""
    current_java = os.environ.get("JAVA_HOME", "")
    # If not set or pointing to x86 temurin, look for Homebrew openjdk
    if not current_java or "temurin-8" in current_java:
        candidate_paths = [
            "/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home",
            "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home",
            "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
            "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
        ]
        for p in candidate_paths:
            if os.path.exists(p):
                os.environ["JAVA_HOME"] = p
                logger.info(f"Auto-configured JAVA_HOME to: {p}")
                break


def get_or_create_spark_session(config: ReconciliationConfig) -> SparkSession:
    """Creates an optimized SparkSession for reconciliation workloads."""
    configure_environment()
    return (
        SparkSession.builder
        .appName(config.spark_app_name)
        .master(config.spark_master)
        .config("spark.driver.host", config.spark_driver_host)
        .config("spark.driver.bindAddress", config.spark_driver_bind_address)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )


def run_pipeline(
    excel_path: str,
    plan_path: str,
    output_path: str,
    abs_tolerance: float = 1.0,
    spark: SparkSession = None
):
    """Executes the complete reconciliation pipeline."""
    config = ReconciliationConfig(
        excel_ric_path=excel_path,
        plan_rollforward_path=plan_path,
        output_report_path=output_path,
        tolerances=ToleranceConfig(
            abs_tolerance_premium=Decimal(str(abs_tolerance)),
            abs_tolerance_claims=Decimal(str(abs_tolerance)),
            abs_tolerance_commission=Decimal(str(abs_tolerance))
        )
    )

    should_stop_spark = False
    if spark is None:
        spark = get_or_create_spark_session(config)
        should_stop_spark = True

    try:
        print("\n" + "=" * 80)
        print("          STARTING RIC vs PLAN ROLLFORWARD RECONCILIATION")
        print("=" * 80)

        # Step 1: Parse Excel RIC Report
        logger.info(f"Step 1: Extracting RIC reference figures from Excel ({config.excel_ric_path})...")
        ric_records = extract_ric_data_as_dicts(config.excel_ric_path)
        logger.info(f"Extracted {len(ric_records)} business case targets from RIC Excel.")

        engine = PySparkReconciliationEngine(spark=spark, config=config)
        ric_df = engine.create_ric_dataframe(ric_records)

        # Step 2: Read and Aggregate Plan Rollforward (21M+ Contracts)
        logger.info(f"Step 2: Reading Plan Rollforward dataset from Parquet ({config.plan_rollforward_path})...")
        plan_df = spark.read.parquet(config.plan_rollforward_path)

        aggregated_plan_df = engine.aggregate_plan_rollforward(plan_df)

        # Step 3: Run Broadcast Reconciliation
        logger.info("Step 3: Executing Broadcast Join Reconciliation...")
        reconciled_df = engine.reconcile(aggregated_plan_df, ric_df)

        # Cache reconciled results for multiple downstream operations (dashboard + persistence)
        reconciled_df.cache()

        # Step 4: Executive KPI Summary
        logger.info("Step 4: Generating Executive Summary & Discrepancy Breakdown...")
        summary = engine.compute_executive_summary(reconciled_df)

        # Step 5: Reporting & Export
        ReconciliationReporter.print_executive_dashboard(summary)
        ReconciliationReporter.show_discrepancies(reconciled_df, max_rows=10)
        ReconciliationReporter.save_results(reconciled_df, summary, config.output_report_path)

        reconciled_df.unpersist()
        logger.info("Reconciliation Pipeline successfully completed.")
        return summary

    finally:
        if should_stop_spark:
            spark.stop()


def setup_sample_demo_data(spark: SparkSession, base_dir: str = "demo_data", sample_contracts: int = 100_000):
    """
    Sets up realistic sample data demonstrating:
    - Exact match
    - Rounding difference within $1.00 tolerance
    - Discrepancy breach (> $1000)
    - Orphan in Plan only
    - Orphan in RIC only
    """
    excel_path = os.path.join(base_dir, "ric_report.xlsx")
    parquet_path = os.path.join(base_dir, "plan_rollforward.parquet")

    business_cases_scenario = [
        # 1. Exact Match
        {
            "business_case": "BC_AUTO_PERSONAL",
            "ric_premium": Decimal("12500000.00"),
            "ric_claims": Decimal("7200000.00"),
            "ric_commission": Decimal("1250000.00"),
            "plan_premium": Decimal("12500000.00"),
            "plan_claims": Decimal("7200000.00"),
            "plan_commission": Decimal("1250000.00"),
            "is_ric_orphan": False
        },
        # 2. Minor Rounding ($0.45 difference - Within Tolerance)
        {
            "business_case": "BC_COMMERCIAL_FLEET",
            "ric_premium": Decimal("24600150.00"),
            "ric_claims": Decimal("15300020.00"),
            "ric_commission": Decimal("2460015.00"),
            "plan_premium": Decimal("24600150.45"),
            "plan_claims": Decimal("15300020.20"),
            "plan_commission": Decimal("2460015.10"),
            "is_ric_orphan": False
        },
        # 3. Discrepancy Breach ($12,500 difference - Action Required)
        {
            "business_case": "BC_PROPERTY_DIRECT",
            "ric_premium": Decimal("38000000.00"),
            "ric_claims": Decimal("21000000.00"),
            "ric_commission": Decimal("3800000.00"),
            "plan_premium": Decimal("38012500.00"),  # $12.5k delta
            "plan_claims": Decimal("21000000.00"),
            "plan_commission": Decimal("3800000.00"),
            "is_ric_orphan": False
        },
        # 4. Another Exact Match
        {
            "business_case": "BC_LIFE_TERM",
            "ric_premium": Decimal("45000000.00"),
            "ric_claims": Decimal("18000000.00"),
            "ric_commission": Decimal("4500000.00"),
            "plan_premium": Decimal("45000000.00"),
            "plan_claims": Decimal("18000000.00"),
            "plan_commission": Decimal("4500000.00"),
            "is_ric_orphan": False
        },
        # 5. Orphan in Plan (Missing from RIC legacy API)
        {
            "business_case": "BC_CYBER_ENTERPRISE",
            "ric_premium": Decimal("0.00"),
            "ric_claims": Decimal("0.00"),
            "ric_commission": Decimal("0.00"),
            "plan_premium": Decimal("5200000.00"),
            "plan_claims": Decimal("1100000.00"),
            "plan_commission": Decimal("520000.00"),
            "is_ric_orphan": False,
            "is_plan_orphan": True
        },
        # 6. Orphan in RIC (Missing from Plan Rollforward)
        {
            "business_case": "BC_DISCONTINUED_RUNOFF",
            "ric_premium": Decimal("1500000.00"),
            "ric_claims": Decimal("850000.00"),
            "ric_commission": Decimal("150000.00"),
            "plan_premium": Decimal("0.00"),
            "plan_claims": Decimal("0.00"),
            "plan_commission": Decimal("0.00"),
            "is_ric_orphan": True  # Won't be written to Plan Rollforward
        }
    ]

    # Generate Excel
    generate_ric_excel(excel_path, business_cases_scenario)

    # Generate Parquet
    generate_plan_rollforward_parquet(
        spark=spark,
        output_path=parquet_path,
        business_cases_data=business_cases_scenario,
        total_contracts=sample_contracts,
        num_partitions=4
    )

    return excel_path, parquet_path


def main():
    parser = argparse.ArgumentParser(description="RIC vs Plan Rollforward Reconciliation Engine")
    parser.add_argument("--excel-path", type=str, default=None, help="Path to RIC Excel file")
    parser.add_argument("--plan-path", type=str, default=None, help="Path to Plan Rollforward Parquet data")
    parser.add_argument("--output-dir", type=str, default="output/recon_run", help="Output directory for reports")
    parser.add_argument("--tolerance", type=float, default=1.0, help="Absolute tolerance in currency units (default: 1.0)")
    parser.add_argument("--demo", action="store_true", help="Generate synthetic demo data and run end-to-end")
    parser.add_argument("--demo-contracts", type=int, default=50_000, help="Number of contracts for demo run")

    args = parser.parse_args()

    config = ReconciliationConfig()
    spark = get_or_create_spark_session(config)

    try:
        excel_path = args.excel_path
        plan_path = args.plan_path

        if args.demo or (excel_path is None and plan_path is None):
            print("\nGenerating sample demonstration datasets...")
            excel_path, plan_path = setup_sample_demo_data(
                spark=spark,
                base_dir="demo_data",
                sample_contracts=args.demo_contracts
            )

        run_pipeline(
            excel_path=excel_path,
            plan_path=plan_path,
            output_path=args.output_dir,
            abs_tolerance=args.tolerance,
            spark=spark
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
