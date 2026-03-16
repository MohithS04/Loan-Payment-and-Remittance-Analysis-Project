"""
clean.py — Reusable, Config-Driven Data Cleaning Pipeline

Standardises dates, casts amounts, strips strings, removes duplicates,
validates IDs/statuses, and produces a cleaning report dict.
"""

import os
import re
import sys
import time
import logging

import pandas as pd
import numpy as np
from dateutil import parser as date_parser

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

logger = logging.getLogger(__name__)


# =========================================================================
#  Cleaning Functions
# =========================================================================

def standardize_dates(df: pd.DataFrame, date_cols: list | None = None) -> tuple[pd.DataFrame, int]:
    """Convert all date columns to YYYY-MM-DD string format."""
    date_cols = date_cols or [c for c in config.DATE_COLUMNS if c in df.columns]
    fixes = 0
    for col in date_cols:
        original = df[col].copy()
        df[col] = df[col].apply(lambda x: _safe_parse_date(x) if pd.notna(x) else x)
        fixes += (original != df[col]).sum()
    return df, int(fixes)


def _safe_parse_date(val) -> str:
    """Parse a date value to YYYY-MM-DD; return as-is on failure."""
    try:
        return date_parser.parse(str(val)).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return val


def title_case_strings(df: pd.DataFrame, str_cols: list | None = None) -> tuple[pd.DataFrame, int]:
    """Strip whitespace and title-case all string columns."""
    str_cols = str_cols or [c for c in config.STRING_COLUMNS if c in df.columns]
    fixes = 0
    for col in str_cols:
        original = df[col].copy()
        df[col] = df[col].astype(str).str.strip().str.title()
        fixes += (original != df[col]).sum()
    return df, int(fixes)


def cast_amounts(df: pd.DataFrame, amount_cols: list | None = None) -> tuple[pd.DataFrame, int]:
    """Cast amount columns to float; coerce errors to NaN."""
    amount_cols = amount_cols or [c for c in config.AMOUNT_COLUMNS if c in df.columns]
    fixes = 0
    for col in amount_cols:
        before_nan = df[col].isna().sum()
        df[col] = pd.to_numeric(df[col], errors="coerce")
        after_nan = df[col].isna().sum()
        fixes += int(after_nan - before_nan)
    return df, int(fixes)


def flag_nan_amounts(
    df: pd.DataFrame,
    amount_cols: list | None = None,
    exceptions_path: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Flag rows with NaN in any amount column. Write exceptions to file."""
    amount_cols = amount_cols or [c for c in config.AMOUNT_COLUMNS if c in df.columns]
    mask = df[amount_cols].isna().any(axis=1)
    exceptions_df = df[mask].copy()
    if exceptions_path and len(exceptions_df) > 0:
        os.makedirs(os.path.dirname(exceptions_path), exist_ok=True)
        exceptions_df.to_csv(exceptions_path, index=False)
        logger.info(f"Wrote {len(exceptions_df)} exception rows to {exceptions_path}")
    return df, exceptions_df, int(len(exceptions_df))


def remove_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Remove exact duplicate rows."""
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed > 0:
        logger.info(f"Removed {removed} exact duplicate rows")
    return df, removed


def validate_loan_ids(df: pd.DataFrame, col: str = "loan_id") -> tuple[pd.DataFrame, int]:
    """Validate loan_id matches the expected pattern. Flag invalids."""
    if col not in df.columns:
        return df, 0
    pattern = re.compile(config.LOAN_ID_PATTERN)
    invalid_mask = ~df[col].astype(str).apply(lambda x: bool(pattern.match(x)))
    count = int(invalid_mask.sum())
    if count > 0:
        df.loc[invalid_mask, "_invalid_loan_id"] = True
        logger.warning(f"Found {count} rows with invalid loan_id format")
    return df, count


def validate_payment_status(df: pd.DataFrame, col: str = "payment_status") -> tuple[pd.DataFrame, int]:
    """Validate payment_status is in allowed set."""
    if col not in df.columns:
        return df, 0
    invalid_mask = ~df[col].isin(config.ALLOWED_PAYMENT_STATUSES)
    count = int(invalid_mask.sum())
    if count > 0:
        df.loc[invalid_mask, "_invalid_status"] = True
        logger.warning(f"Found {count} rows with invalid payment_status")
    return df, count


# =========================================================================
#  Main Pipeline
# =========================================================================

def clean_dataframe(
    df: pd.DataFrame,
    dataset_name: str = "dataset",
    exceptions_dir: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Run the full cleaning pipeline on a DataFrame.

    Returns:
        (cleaned_df, cleaning_report)
    """
    start_time = time.time()
    report = {
        "dataset": dataset_name,
        "rows_before": len(df),
        "date_fixes": 0,
        "string_fixes": 0,
        "amount_coercions": 0,
        "nan_exceptions": 0,
        "duplicates_removed": 0,
        "invalid_loan_ids": 0,
        "invalid_statuses": 0,
    }

    # 1. Standardize dates
    df, n = standardize_dates(df)
    report["date_fixes"] = n

    # 2. Title-case strings
    df, n = title_case_strings(df)
    report["string_fixes"] = n

    # 3. Cast amounts
    df, n = cast_amounts(df)
    report["amount_coercions"] = n

    # 4. Flag NaN amounts
    exc_path = None
    if exceptions_dir:
        exc_path = os.path.join(exceptions_dir, f"{dataset_name}_exceptions.csv")
    df, exceptions_df, n = flag_nan_amounts(df, exceptions_path=exc_path)
    report["nan_exceptions"] = n

    # 5. Remove exact duplicates
    df, n = remove_duplicates(df)
    report["duplicates_removed"] = n

    # 6. Validate loan_id
    df, n = validate_loan_ids(df)
    report["invalid_loan_ids"] = n

    # 7. Validate payment_status
    df, n = validate_payment_status(df)
    report["invalid_statuses"] = n

    df = df.reset_index(drop=True)
    elapsed = time.time() - start_time
    report["rows_after"] = len(df)
    report["cleaning_time_seconds"] = round(elapsed, 4)

    return df, report


def simulate_manual_baseline(df: pd.DataFrame) -> float:
    """
    Simulate a manual cleaning time baseline.
    Assumes ~0.5 seconds per 100 rows for a human analyst.
    """
    return len(df) * 0.005


def compute_time_saved(cleaning_report: dict, manual_time: float) -> float:
    """Compute percentage time saved vs manual baseline."""
    auto_time = cleaning_report.get("cleaning_time_seconds", 0)
    if manual_time <= 0:
        return 0.0
    return round((1 - auto_time / manual_time) * 100, 2)
