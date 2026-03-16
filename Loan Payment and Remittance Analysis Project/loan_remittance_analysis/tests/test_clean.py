"""
test_clean.py — Unit tests for the data cleaning pipeline.
"""

import sys
import os
import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.clean import (
    standardize_dates,
    title_case_strings,
    cast_amounts,
    flag_nan_amounts,
    remove_duplicates,
    validate_loan_ids,
    validate_payment_status,
    clean_dataframe,
)


# ─── Test 1: Date Normalization ──────────────────────────────────────

def test_standardize_dates_various_formats():
    """Dates in various formats should all become YYYY-MM-DD."""
    df = pd.DataFrame({
        "payment_date": ["2025-01-15", "01/15/2025", "Jan 15, 2025", "15-01-2025"],
    })
    result, fixes = standardize_dates(df, date_cols=["payment_date"])
    assert all(result["payment_date"].str.match(r"^\d{4}-\d{2}-\d{2}$"))
    assert fixes >= 2  # At least some were reformatted


def test_standardize_dates_handles_nan():
    """NaN dates should pass through without error."""
    df = pd.DataFrame({"payment_date": ["2025-01-15", None, "2025-03-20"]})
    result, _ = standardize_dates(df, date_cols=["payment_date"])
    assert pd.isna(result["payment_date"].iloc[1])


# ─── Test 2: Duplicate Removal ──────────────────────────────────────

def test_remove_duplicates_removes_exact():
    """Exact duplicate rows should be removed."""
    df = pd.DataFrame({
        "loan_id": ["LN-000001", "LN-000001", "LN-000002"],
        "amount": [1000, 1000, 2000],
    })
    result, removed = remove_duplicates(df)
    assert removed == 1
    assert len(result) == 2


def test_remove_duplicates_keeps_unique():
    """All-unique rows should keep the same count."""
    df = pd.DataFrame({
        "loan_id": ["LN-000001", "LN-000002", "LN-000003"],
        "amount": [100, 200, 300],
    })
    result, removed = remove_duplicates(df)
    assert removed == 0
    assert len(result) == 3


# ─── Test 3: NaN Flagging ───────────────────────────────────────────

def test_flag_nan_amounts_detects_missing(tmp_path):
    """Rows with NaN amounts should be flagged and optionally written."""
    df = pd.DataFrame({
        "actual_payment_amount": [100.0, np.nan, 200.0],
        "scheduled_payment_amount": [100.0, 150.0, np.nan],
    })
    exc_path = str(tmp_path / "exceptions.csv")
    _, exceptions, count = flag_nan_amounts(
        df,
        amount_cols=["actual_payment_amount", "scheduled_payment_amount"],
        exceptions_path=exc_path,
    )
    assert count == 2
    assert len(exceptions) == 2
    assert os.path.exists(exc_path)


# ─── Test 4: Loan ID Validation ─────────────────────────────────────

def test_validate_loan_ids_valid():
    """Valid loan IDs should pass without flags."""
    df = pd.DataFrame({"loan_id": ["LN-000001", "LN-123456", "LN-000099"]})
    result, invalid_count = validate_loan_ids(df)
    assert invalid_count == 0
    assert "_invalid_loan_id" not in result.columns


def test_validate_loan_ids_invalid():
    """Invalid loan IDs should be flagged."""
    df = pd.DataFrame({"loan_id": ["LN-000001", "INVALID", "LN-12", "LN-0000001"]})
    result, invalid_count = validate_loan_ids(df)
    assert invalid_count == 3  # INVALID, LN-12, LN-0000001


# ─── Test 5: Payment Status Validation ──────────────────────────────

def test_validate_payment_status_valid():
    """Valid statuses should not be flagged."""
    df = pd.DataFrame({"payment_status": ["Posted", "Pending", "Reversed"]})
    result, invalid_count = validate_payment_status(df)
    assert invalid_count == 0


def test_validate_payment_status_invalid():
    """Invalid statuses should be flagged."""
    df = pd.DataFrame({"payment_status": ["Posted", "Unknown", "Cancelled"]})
    result, invalid_count = validate_payment_status(df)
    assert invalid_count == 2


# ─── Test 6: Full Pipeline ──────────────────────────────────────────

def test_clean_dataframe_returns_report():
    """The full pipeline should return a dict with expected keys."""
    df = pd.DataFrame({
        "loan_id": ["LN-000001", "LN-000002"],
        "payment_date": ["2025-01-15", "2025-02-20"],
        "actual_payment_amount": [1000.0, 2000.0],
        "payment_status": ["Posted", "Pending"],
        "borrower_name": ["  john DOE  ", "Jane Smith"],
    })
    result, report = clean_dataframe(df, "test")
    assert "rows_before" in report
    assert "rows_after" in report
    assert "cleaning_time_seconds" in report
    assert report["rows_before"] == 2


# ─── Test 7: Amount Casting ─────────────────────────────────────────

def test_cast_amounts_coerces_strings():
    """Non-numeric strings should become NaN."""
    df = pd.DataFrame({"actual_payment_amount": ["100.50", "abc", "200"]})
    result, coerced = cast_amounts(df, amount_cols=["actual_payment_amount"])
    assert coerced == 1
    assert pd.isna(result["actual_payment_amount"].iloc[1])
    assert result["actual_payment_amount"].iloc[0] == 100.50


# ─── Test 8: String Title Case ──────────────────────────────────────

def test_title_case_strings():
    """Strings should be stripped and title-cased."""
    df = pd.DataFrame({"borrower_name": ["  john DOE  ", "JANE SMITH", "bob jones "]})
    result, _ = title_case_strings(df, str_cols=["borrower_name"])
    assert result["borrower_name"].tolist() == ["John Doe", "Jane Smith", "Bob Jones"]
