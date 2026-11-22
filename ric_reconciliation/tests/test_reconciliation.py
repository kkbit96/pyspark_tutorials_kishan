"""
Unit and Integration Tests for RIC vs Plan Rollforward Reconciliation Pipeline.
Tests:
1. Excel extraction accuracy and formula evaluation.
2. Spark aggregation arithmetic and Decimal precision.
3. Reconciliation logic: Exact Match, Within Tolerance, Breach, Orphans.
4. Executive summary metrics tie-out.
"""

import unittest
from decimal import Decimal
import os
import tempfile
import openpyxl

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)

from ric_reconciliation.config import ReconciliationConfig, ToleranceConfig
from ric_reconciliation.excel_parser import RICExcelParser
from ric_reconciliation.spark_engine import PySparkReconciliationEngine
from ric_reconciliation.main import configure_environment


class TestRICReconciliation(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        configure_environment()
        cls.spark = (
            SparkSession.builder
            .appName("UnitTest_Recon")
            .master("local[2]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.config = ReconciliationConfig(
            tolerances=ToleranceConfig(
                abs_tolerance_premium=Decimal("1.00"),
                abs_tolerance_claims=Decimal("1.00"),
                abs_tolerance_commission=Decimal("1.00")
            )
        )
        cls.engine = PySparkReconciliationEngine(spark=cls.spark, config=cls.config)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_excel_parser_streaming(self):
        """Verify Excel parser extracts clean records and strips currency characters."""
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            temp_path = f.name

        try:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Summary_Data"
            ws.append(["Business Case", "Gross Premium", "Claims Incurred", "Broker Commission"])
            ws.append(["BC_TEST_01", "$ 1,000,500.50", "500,200.25", "(10,000.00)"])
            ws.append(["BC_TEST_02", 250000.00, 120000.00, 25000.00])
            wb.save(temp_path)

            parser = RICExcelParser(temp_path)
            records = parser.parse_sheet_stream()

            self.assertEqual(len(records), 2)
            self.assertEqual(records[0]["business_case"], "BC_TEST_01")
            self.assertEqual(records[0]["ric_premium"], Decimal("1000500.50"))
            self.assertEqual(records[0]["ric_claims"], Decimal("500200.25"))
            self.assertEqual(records[0]["ric_commission"], Decimal("-10000.00"))  # Negative parentheses handled

            self.assertEqual(records[1]["business_case"], "BC_TEST_02")
            self.assertEqual(records[1]["ric_premium"], Decimal("250000.00"))
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_reconciliation_logic(self):
        """Verify full outer join reconciliation tags matches, tolerances, and breaches accurately."""
        # 1. RIC records
        ric_records = [
            {"business_case": "CASE_EXACT", "ric_premium": Decimal("1000.00"), "ric_claims": Decimal("500.00"), "ric_commission": Decimal("100.00")},
            {"business_case": "CASE_TOLERANCE", "ric_premium": Decimal("2000.00"), "ric_claims": Decimal("1000.00"), "ric_commission": Decimal("200.00")},
            {"business_case": "CASE_BREACH", "ric_premium": Decimal("3000.00"), "ric_claims": Decimal("1500.00"), "ric_commission": Decimal("300.00")},
            {"business_case": "CASE_ORPHAN_RIC", "ric_premium": Decimal("500.00"), "ric_claims": Decimal("250.00"), "ric_commission": Decimal("50.00")}
        ]
        ric_df = self.engine.create_ric_dataframe(ric_records)

        # 2. Plan contract records
        plan_schema = StructType([
            StructField("contract_id", StringType(), False),
            StructField("business_case", StringType(), False),
            StructField("premium", DecimalType(18, 2), False),
            StructField("claims", DecimalType(18, 2), False),
            StructField("commission", DecimalType(18, 2), False),
        ])

        plan_data = [
            # CASE_EXACT: exactly 1000, 500, 100
            ("C1", "CASE_EXACT", Decimal("500.00"), Decimal("250.00"), Decimal("50.00")),
            ("C2", "CASE_EXACT", Decimal("500.00"), Decimal("250.00"), Decimal("50.00")),

            # CASE_TOLERANCE: delta is 0.50, 0.20, 0.10 (within 1.00 tolerance)
            ("C3", "CASE_TOLERANCE", Decimal("2000.50"), Decimal("1000.20"), Decimal("200.10")),

            # CASE_BREACH: delta is 50.00 (> 1.00 tolerance)
            ("C4", "CASE_BREACH", Decimal("3050.00"), Decimal("1500.00"), Decimal("300.00")),

            # CASE_ORPHAN_PLAN: not in RIC
            ("C5", "CASE_ORPHAN_PLAN", Decimal("750.00"), Decimal("300.00"), Decimal("75.00"))
        ]
        plan_raw_df = self.spark.createDataFrame(plan_data, schema=plan_schema)

        # 3. Aggregate Plan
        agg_plan_df = self.engine.aggregate_plan_rollforward(plan_raw_df)

        # 4. Reconcile
        recon_df = self.engine.reconcile(agg_plan_df, ric_df)
        results = {r["business_case"]: r["recon_status"] for r in recon_df.collect()}

        self.assertEqual(results["CASE_EXACT"], "MATCH_EXACT")
        self.assertEqual(results["CASE_TOLERANCE"], "MATCH_WITHIN_TOLERANCE")
        self.assertEqual(results["CASE_BREACH"], "DISCREPANCY_BREACH")
        self.assertEqual(results["CASE_ORPHAN_PLAN"], "ORPHAN_PLAN_ONLY")
        self.assertEqual(results["CASE_ORPHAN_RIC"], "ORPHAN_RIC_ONLY")

    def test_executive_summary_kpis(self):
        """Ensure executive KPI calculations tie out."""
        ric_records = [
            {"business_case": "BC_A", "ric_premium": Decimal("100.00"), "ric_claims": Decimal("50.00"), "ric_commission": Decimal("10.00")},
        ]
        ric_df = self.engine.create_ric_dataframe(ric_records)

        plan_schema = StructType([
            StructField("contract_id", StringType(), False),
            StructField("business_case", StringType(), False),
            StructField("premium", DecimalType(18, 2), False),
            StructField("claims", DecimalType(18, 2), False),
            StructField("commission", DecimalType(18, 2), False),
        ])
        plan_data = [("C1", "BC_A", Decimal("100.00"), Decimal("50.00"), Decimal("10.00"))]
        plan_df = self.spark.createDataFrame(plan_data, schema=plan_schema)

        agg_plan_df = self.engine.aggregate_plan_rollforward(plan_df)
        recon_df = self.engine.reconcile(agg_plan_df, ric_df)
        summary = self.engine.compute_executive_summary(recon_df)

        self.assertEqual(summary["total_business_cases"], 1)
        self.assertEqual(summary["cases_reconciled"], 1)
        self.assertEqual(summary["cases_breached"], 0)
        self.assertEqual(summary["match_rate_pct"], 100.0)
        self.assertEqual(summary["net_premium_delta"], Decimal("0.00"))


if __name__ == "__main__":
    unittest.main()
