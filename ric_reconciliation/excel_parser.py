"""
High-Performance Excel Parser for Legacy RIC Reports.

Optimized for:
1. Memory efficiency: Uses openpyxl streaming mode (read_only=True) to handle large multi-sheet workbooks.
2. Formula evaluation: Uses data_only=True to extract pre-calculated cached values instead of raw formula strings.
3. Layout flexibility: Handles:
   - Tabular master/reference sheets behind dropdowns.
   - Multi-tab business case layouts.
   - Varied column aliases (e.g. 'Prem', 'Gross Premium', 'Claims Incurred', 'Broker Commission').
"""

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple, Any
import openpyxl

logger = logging.getLogger(__name__)


class RICExcelParser:
    """
    Parser for extracting aggregated business case metrics (Premiums, Claims, Commissions)
    from legacy Excel RIC reports.
    """

    # Aliases for robust fuzzy column header matching
    COLUMN_ALIASES = {
        "business_case": [
            "business_case", "business case", "businesscase", "case", "case_id",
            "bc", "segment", "portfolio", "lob", "line_of_business", "plan_case"
        ],
        "premium": [
            "premium", "premiums", "gross_premium", "written_premium", "total_premium",
            "gwp", "earned_premium", "prem"
        ],
        "claims": [
            "claims", "claim", "claims_paid", "incurred_claims", "total_claims",
            "claims_amount", "loss", "losses"
        ],
        "commission": [
            "commission", "commissions", "broker_commission", "total_commission",
            "comm", "acquisition_cost", "expenses"
        ]
    }

    def __init__(self, file_path: str):
        self.file_path = file_path

    @staticmethod
    def _clean_header(val: Any) -> str:
        """Normalize header string for matching."""
        if val is None:
            return ""
        return re.sub(r"[^a-z0-9]", "_", str(val).strip().lower()).strip("_")

    @staticmethod
    def _parse_financial_value(val: Any) -> Decimal:
        """
        Converts Excel financial values (strings, floats, currency formatting)
        into exact Decimals. Handles currency signs, commas, and parentheses for negatives.
        """
        if val is None or val == "":
            return Decimal("0.00")
        if isinstance(val, (int, float)):
            # Round to 2 decimal places to prevent IEEE 754 float inaccuracies
            return Decimal(str(round(float(val), 2)))
        if isinstance(val, Decimal):
            return val

        # Clean string formatted currency: e.g. "$ (1,250.50)" -> "-1250.50"
        s = str(val).strip()
        is_negative = False
        if s.startswith("(") and s.endswith(")"):
            is_negative = True
            s = s[1:-1]
        elif "-" in s:
            is_negative = True

        # Remove currency symbols, commas, spaces
        cleaned = re.sub(r"[^\d.]", "", s)
        if not cleaned:
            return Decimal("0.00")

        try:
            d = Decimal(cleaned)
            return -d if is_negative else d
        except InvalidOperation:
            logger.warning(f"Failed to parse value '{val}' as Decimal. Defaulting to 0.00")
            return Decimal("0.00")

    def _match_column_index(self, header_row: List[str]) -> Dict[str, int]:
        """Maps canonical metric names to column indices in the header row."""
        col_mapping: Dict[str, int] = {}
        cleaned_headers = [self._clean_header(c) for c in header_row]

        # Pass 1: Exact alias match
        for idx, cleaned_col in enumerate(cleaned_headers):
            if not cleaned_col:
                continue
            for canonical_name, aliases in self.COLUMN_ALIASES.items():
                if canonical_name not in col_mapping and cleaned_col in aliases:
                    col_mapping[canonical_name] = idx

        # Pass 2: Token / Substring fallback for unmapped canonical fields
        for canonical_name, aliases in self.COLUMN_ALIASES.items():
            if canonical_name in col_mapping:
                continue
            for idx, cleaned_col in enumerate(cleaned_headers):
                if idx in col_mapping.values() or not cleaned_col:
                    continue
                tokens = cleaned_col.split("_")
                if any(alias in tokens or alias in cleaned_col for alias in aliases):
                    col_mapping[canonical_name] = idx
                    break

        return col_mapping

    def parse_sheet_stream(
        self,
        sheet_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Streams records from the specified sheet (or best matching sheet) using openpyxl read_only=True.
        Returns a list of dictionaries with canonical keys:
        - business_case: str
        - ric_premium: Decimal
        - ric_claims: Decimal
        - ric_commission: Decimal
        """
        logger.info(f"Opening Excel file in streaming read_only mode: {self.file_path}")
        wb = openpyxl.load_workbook(
            filename=self.file_path,
            read_only=True,
            data_only=True,  # Crucial: Evaluates cached formula results
            keep_links=False
        )

        try:
            target_sheet = None
            if sheet_name and sheet_name in wb.sheetnames:
                target_sheet = wb[sheet_name]
            else:
                # Auto-detect the best sheet containing aggregated business case data
                # Typically named 'Aggregated', 'Data', 'RIC_Summary', 'Reference', or sheet 0
                for s_name in wb.sheetnames:
                    s_lower = s_name.lower()
                    if any(k in s_lower for k in ["ric", "agg", "summary", "data", "business", "lookup"]):
                        target_sheet = wb[s_name]
                        break
                if target_sheet is None:
                    target_sheet = wb.active or wb[wb.sheetnames[0]]

            logger.info(f"Extracting aggregated records from worksheet: {target_sheet.title}")

            records: List[Dict[str, Any]] = []
            header_map: Optional[Dict[str, int]] = None
            header_row_idx: Optional[int] = None

            # Stream rows
            for row_idx, row in enumerate(target_sheet.iter_rows(values_only=True), start=1):
                if not any(row):
                    continue

                row_vals = [c for c in row]

                # Look for header row if not yet identified
                if header_map is None:
                    mapping = self._match_column_index([str(c or "") for c in row_vals])
                    if "business_case" in mapping and len(mapping) >= 2:
                        header_map = mapping
                        header_row_idx = row_idx
                        logger.info(f"Found valid header on row {row_idx}: {header_map}")
                    continue

                # Once header is established, extract data rows
                bc_idx = header_map.get("business_case")
                if bc_idx is None or bc_idx >= len(row_vals):
                    continue

                bc_raw = row_vals[bc_idx]
                if bc_raw is None or str(bc_raw).strip() == "" or str(bc_raw).lower() in ["total", "grand total", "sum"]:
                    continue

                bc_val = str(bc_raw).strip().upper()

                # Extract metrics
                prem_idx = header_map.get("premium")
                claims_idx = header_map.get("claims")
                comm_idx = header_map.get("commission")

                prem_val = self._parse_financial_value(row_vals[prem_idx]) if (prem_idx is not None and prem_idx < len(row_vals)) else Decimal("0.00")
                claims_val = self._parse_financial_value(row_vals[claims_idx]) if (claims_idx is not None and claims_idx < len(row_vals)) else Decimal("0.00")
                comm_val = self._parse_financial_value(row_vals[comm_idx]) if (comm_idx is not None and comm_idx < len(row_vals)) else Decimal("0.00")

                records.append({
                    "business_case": bc_val,
                    "ric_premium": prem_val,
                    "ric_claims": claims_val,
                    "ric_commission": comm_val
                })

            logger.info(f"Successfully extracted {len(records)} business case records from RIC Excel.")
            return records

        finally:
            wb.close()


def extract_ric_data_as_dicts(file_path: str, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Convenience helper function to parse Excel into clean dictionary records."""
    parser = RICExcelParser(file_path=file_path)
    return parser.parse_sheet_stream(sheet_name=sheet_name)
