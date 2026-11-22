"""
Configuration module for RIC vs Plan Rollforward Reconciliation Pipeline.
Defines data structures, schemas, tolerances, and execution settings.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional


@dataclass(frozen=True)
class ToleranceConfig:
    """Tolerances for financial reconciliation across metrics."""
    # Absolute tolerance in currency units (e.g. $1.00 for minor rounding)
    abs_tolerance_premium: Decimal = Decimal("1.00")
    abs_tolerance_claims: Decimal = Decimal("1.00")
    abs_tolerance_commission: Decimal = Decimal("1.00")

    # Relative percentage tolerance (e.g. 0.01% = 0.0001)
    rel_tolerance_pct: Decimal = Decimal("0.0001")


@dataclass
class ReconciliationConfig:
    """Master pipeline configuration."""
    # Paths
    excel_ric_path: str = "data/ric_report.xlsx"
    plan_rollforward_path: str = "data/plan_rollforward_parquet"
    output_report_path: str = "output/reconciliation_results"

    # Excel Parser Configuration
    ric_sheet_name: Optional[str] = None  # None = auto-detect or scan all relevant sheets
    ric_business_case_col: str = "Business_Case"
    ric_premium_col: str = "Premium"
    ric_claims_col: str = "Claims"
    ric_commission_col: str = "Commission"

    # Spark Optimizations
    spark_app_name: str = "RIC_Plan_Rollforward_Reconciliation"
    spark_master: str = "local[*]"
    spark_driver_host: str = "127.0.0.1"
    spark_driver_bind_address: str = "127.0.0.1"
    shuffle_partitions: int = 16  # Tailored for local; set to 200+ for large cluster
    broadcast_threshold_bytes: int = 10485760  # 10MB
    enable_aqe: bool = True

    # Financial Precision
    # In financial/actuarial accounting, DecimalType(18, 2) is used to prevent float rounding errors
    decimal_precision: int = 18
    decimal_scale: int = 2

    # Tolerances
    tolerances: ToleranceConfig = field(default_factory=ToleranceConfig)
