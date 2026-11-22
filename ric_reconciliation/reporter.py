"""
Audit & Reporting Module for Reconciliation Pipeline.
Generates human-readable console dashboards, JSON summaries, and parquet audit tables.
"""

import json
import logging
from typing import Dict, Any
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class ReconciliationReporter:
    """Formats and exports reconciliation results for executive and audit review."""

    @staticmethod
    def print_executive_dashboard(summary: Dict[str, Any]):
        """Prints a rich CLI dashboard summarizing portfolio tie-out status."""
        border = "=" * 80
        sub_border = "-" * 80

        print("\n" + border)
        print("          EXECUTIVE FINANCIAL RECONCILIATION AUDIT DASHBOARD")
        print("                 RIC REPORT vs PLAN ROLLFORWARD (21M+)")
        print(border)

        print(f"Total Business Cases Evaluated : {summary['total_business_cases']}")
        print(f"Total Contracts Processed       : {summary['total_contracts_processed']:,}")
        print(f"Reconciled Cases (Pass)        : {summary['cases_reconciled']} ({summary['match_rate_pct']}%)")
        print(f"Breached Cases (Fail)          : {summary['cases_breached']}")
        print(f"Orphaned Plan Cases            : {summary['cases_plan_orphan']}")
        print(f"Orphaned RIC Cases             : {summary['cases_ric_orphan']}")
        print(sub_border)

        status_text = "PASSED [WITHIN AUDIT TOLERANCE]" if summary['cases_breached'] == 0 else "ACTION REQUIRED [BREACHES DETECTED]"
        print(f"Overall Portfolio Tie-Out Status : {status_text}")
        print(sub_border)

        print("PORTFOLIO FINANCIAL TOTALS:")
        print(f"  Gross Written Premium:")
        print(f"    - Plan Rollforward Total : ${summary['total_plan_premium']:,.2f}")
        print(f"    - RIC Report Total       : ${summary['total_ric_premium']:,.2f}")
        print(f"    - Net Variance           : ${summary['net_premium_delta']:,.2f}")

        print(f"  Incurred Claims:")
        print(f"    - Plan Rollforward Total : ${summary['total_plan_claims']:,.2f}")
        print(f"    - RIC Report Total       : ${summary['total_ric_claims']:,.2f}")
        print(f"    - Net Variance           : ${summary['net_claims_delta']:,.2f}")

        print(f"  Commissions:")
        print(f"    - Plan Rollforward Total : ${summary['total_plan_commission']:,.2f}")
        print(f"    - RIC Report Total       : ${summary['total_ric_commission']:,.2f}")
        print(f"    - Net Variance           : ${summary['net_commission_delta']:,.2f}")

        print(sub_border)
        print(f"Gross Absolute Exposure Delta   : ${summary['gross_unreconciled_exposure']:,.2f}")
        print(border + "\n")

    @staticmethod
    def show_discrepancies(reconciled_df: DataFrame, max_rows: int = 20):
        """Displays detailed business case variance rows in PySpark tabular format."""
        print("\n" + "=" * 80)
        print(f"      TOP BUSINESS CASES BY RECONCILIATION EXPOSURE (Top {max_rows})")
        print("=" * 80)
        reconciled_df.select(
            "business_case",
            "recon_status",
            "breach_severity",
            "plan_premium",
            "ric_premium",
            "delta_premium",
            "delta_claims",
            "delta_commission",
            "total_abs_delta"
        ).show(max_rows, truncate=False)

    @staticmethod
    def save_results(
        reconciled_df: DataFrame,
        summary: Dict[str, Any],
        output_dir: str
    ):
        """Exports parquet audit trail and JSON summary report."""
        import os
        os.makedirs(output_dir, exist_ok=True)

        # 1. Parquet audit log (partitioned by recon_status for fast querying)
        parquet_path = f"{output_dir}/audit_table_parquet"
        reconciled_df.write.mode("overwrite").partitionBy("recon_status").parquet(parquet_path)
        logger.info(f"Exported partitioned reconciliation audit Parquet to: {parquet_path}")

        # 2. JSON Summary for alerting & monitoring systems (Datadog, Slack, Prometheus)
        json_path = f"{output_dir}/summary_kpis.json"
        # Convert Decimals to string/float for JSON serialization
        json_safe_summary = {
            k: float(v) if hasattr(v, '__float__') and not isinstance(v, (int, bool)) else v
            for k, v in summary.items()
        }
        with open(json_path, "w") as f:
            json.dump(json_safe_summary, f, indent=2)
        logger.info(f"Saved executive summary JSON to: {json_path}")
