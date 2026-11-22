"""
Synthetic Data Generator for RIC Report (Excel) and Plan Rollforward Dataset (Parquet).

Simulates:
- A realistic legacy RIC Excel report with dropdown filter / aggregated reference table.
- A Plan Rollforward dataset with contracts aggregated by Business Case.
- Controlled test cases: Exact Matches, Within-Tolerance roundings, Discrepancies/Breaches,
  and Orphaned records.
"""

import logging
import os
import random
from decimal import Decimal
from typing import List, Dict, Any

import openpyxl
from openpyxl.worksheet.datavalidation import DataValidation
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    LongType
)

logger = logging.getLogger(__name__)


def generate_ric_excel(
    file_path: str,
    business_cases_data: List[Dict[str, Any]]
) -> str:
    """
    Generates a legacy RIC Excel workbook simulating a dropdown filter layout
    and an underlying aggregated reference table.
    """
    os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    wb = openpyxl.Workbook()

    # Sheet 1: Master Aggregated Reference Table (Where legacy API puts the figures)
    ws_data = wb.active
    ws_data.title = "RIC_Aggregated_Data"

    # Exclude cases designated as plan-only orphans from Excel
    excel_cases = [b for b in business_cases_data if not b.get("is_plan_orphan", False)]

    headers = ["Business_Case", "Premium", "Claims", "Commission"]
    ws_data.append(headers)

    for row in excel_cases:
        ws_data.append([
            row["business_case"],
            float(row["ric_premium"]),
            float(row["ric_claims"]),
            float(row["ric_commission"])
        ])

    # Sheet 2: User Dropdown / Filter Dashboard (Simulating Excel interactive filter)
    ws_dash = wb.create_sheet(title="RIC_Dropdown_View")
    ws_dash["A1"] = "Select Business Case:"
    ws_dash["B1"] = excel_cases[0]["business_case"] if excel_cases else ""

    ws_dash["A3"] = "Metric"
    ws_dash["B3"] = "Aggregated Value"
    ws_dash["A4"] = "Gross Premium"
    ws_dash["B4"] = "=VLOOKUP(B1, RIC_Aggregated_Data!A:D, 2, FALSE)"
    ws_dash["A5"] = "Incurred Claims"
    ws_dash["B5"] = "=VLOOKUP(B1, RIC_Aggregated_Data!A:D, 3, FALSE)"
    ws_dash["A6"] = "Broker Commission"
    ws_dash["B6"] = "=VLOOKUP(B1, RIC_Aggregated_Data!A:D, 4, FALSE)"

    # Add Data Validation Dropdown pointing to the Business Case list
    dv = DataValidation(
        type="list",
        formula1=f"=RIC_Aggregated_Data!$A$2:$A${len(excel_cases) + 1}",
        allow_blank=True
    )
    ws_dash.add_data_validation(dv)
    dv.add(ws_dash["B1"])

    wb.save(file_path)
    logger.info(f"Generated synthetic RIC Excel report at: {file_path}")
    return file_path


def generate_plan_rollforward_parquet(
    spark: SparkSession,
    output_path: str,
    business_cases_data: List[Dict[str, Any]],
    total_contracts: int = 100_000,
    num_partitions: int = 8
) -> str:
    """
    Generates synthetic Plan Rollforward contract-level dataset and saves to Parquet.
    Contracts are distributed across business cases such that the sums tie out
    (or deviate predictably) according to the test scenario.
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    logger.info(f"Generating {total_contracts:,} contracts for Plan Rollforward dataset...")

    # Active business cases for plan rollforward (exclude cases designated as RIC-only orphans)
    plan_cases = [b for b in business_cases_data if not b.get("is_ric_orphan", False)]
    if not plan_cases:
        raise ValueError("No active business cases available for Plan Rollforward generation.")

    contracts_per_case = total_contracts // len(plan_cases)
    remainder = total_contracts % len(plan_cases)

    records = []
    contract_seq = 1000000000

    for idx, bc in enumerate(plan_cases):
        count = contracts_per_case + (remainder if idx == 0 else 0)
        target_prem = Decimal(str(bc["plan_premium"]))
        target_claims = Decimal(str(bc["plan_claims"]))
        target_comm = Decimal(str(bc["plan_commission"]))

        # Base amounts per contract with balanced remainder on the last row
        base_prem = (target_prem / Decimal(count)).quantize(Decimal("0.01"))
        base_claims = (target_claims / Decimal(count)).quantize(Decimal("0.01"))
        base_comm = (target_comm / Decimal(count)).quantize(Decimal("0.01"))

        prem_accum = Decimal("0.00")
        claims_accum = Decimal("0.00")
        comm_accum = Decimal("0.00")

        bc_name = bc["business_case"]

        for i in range(count):
            contract_seq += 1
            cid = f"POL-{contract_seq}"

            if i == count - 1:
                # Last contract absorbs remainder to match target_prem exactly
                p = (target_prem - prem_accum).quantize(Decimal("0.01"))
                c = (target_claims - claims_accum).quantize(Decimal("0.01"))
                m = (target_comm - comm_accum).quantize(Decimal("0.01"))
            else:
                # Add tiny random jitter around base amount
                jitter = Decimal(str(random.choice([-0.05, 0.0, 0.05])))
                p = (base_prem + jitter).quantize(Decimal("0.01"))
                c = (base_claims + jitter).quantize(Decimal("0.01"))
                m = (base_comm + jitter).quantize(Decimal("0.01"))

                prem_accum += p
                claims_accum += c
                comm_accum += m

            records.append((cid, bc_name, p, c, m))

    schema = StructType([
        StructField("contract_id", StringType(), False),
        StructField("business_case", StringType(), False),
        StructField("premium", DecimalType(18, 2), False),
        StructField("claims", DecimalType(18, 2), False),
        StructField("commission", DecimalType(18, 2), False),
    ])

    df = spark.createDataFrame(records, schema=schema)
    df.repartition(num_partitions).write.mode("overwrite").parquet(output_path)
    logger.info(f"Saved Plan Rollforward Parquet data ({len(records):,} contracts) to: {output_path}")

    return output_path
