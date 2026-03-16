"""
test_collateral.py — Unit tests for collateral payment calculations.
"""

import sys
import os
import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.collateral import CollateralCalculator


@pytest.fixture
def calculator():
    return CollateralCalculator()


def _make_collateral(rows):
    """Helper to build a collateral DataFrame."""
    return pd.DataFrame(rows, columns=[
        "security_id", "loan_id", "collateral_type", "face_value",
        "coupon_rate", "payment_frequency", "next_payment_date",
        "calculated_payment", "trust_requirement_flag",
    ])


# ─── Test 1: Monthly Frequency ──────────────────────────────────────

def test_monthly_coupon_calculation(calculator):
    """Monthly: expected = face_value * (coupon_rate / 12)."""
    df = _make_collateral([
        ["SEC-0001", "LN-000001", "MBS", 100000, 0.06, "Monthly", "2026-06-01", 500.0, False],
    ])
    result = calculator.calculate_expected_payments(df)
    # 100000 * 0.06 / 12 = 500.00
    assert abs(result["expected_payment"].iloc[0] - 500.0) < 0.01


# ─── Test 2: Quarterly Frequency ────────────────────────────────────

def test_quarterly_coupon_calculation(calculator):
    """Quarterly: expected = face_value * (coupon_rate / 4)."""
    df = _make_collateral([
        ["SEC-0002", "LN-000002", "ABS", 200000, 0.04, "Quarterly", "2026-06-01", 2000.0, False],
    ])
    result = calculator.calculate_expected_payments(df)
    # 200000 * 0.04 / 4 = 2000.00
    assert abs(result["expected_payment"].iloc[0] - 2000.0) < 0.01


# ─── Test 3: Semi-Annual Frequency ──────────────────────────────────

def test_semiannual_coupon_calculation(calculator):
    """Semi-Annual: expected = face_value * (coupon_rate / 2)."""
    df = _make_collateral([
        ["SEC-0003", "LN-000003", "CMO", 150000, 0.05, "Semi-Annual", "2026-06-01", 3750.0, False],
    ])
    result = calculator.calculate_expected_payments(df)
    # 150000 * 0.05 / 2 = 3750.00
    assert abs(result["expected_payment"].iloc[0] - 3750.0) < 0.01


# ─── Test 4: Annual Frequency ───────────────────────────────────────

def test_annual_coupon_calculation(calculator):
    """Annual: expected = face_value * coupon_rate."""
    calc = CollateralCalculator(frequency_map={"Annual": 1, "Monthly": 12, "Quarterly": 4, "Semi-Annual": 2})
    df = _make_collateral([
        ["SEC-0004", "LN-000004", "CLO", 500000, 0.03, "Annual", "2026-12-01", 15000.0, False],
    ])
    result = calc.calculate_expected_payments(df)
    # 500000 * 0.03 / 1 = 15000.00
    assert abs(result["expected_payment"].iloc[0] - 15000.0) < 0.01


# ─── Test 5: Trust Requirement Flag — 0% Tolerance ──────────────────

def test_trust_flag_exact_match_required(calculator):
    """Trust-required securities must have 0% tolerance (exact match)."""
    df = _make_collateral([
        # Calculated matches expected exactly → COMPLIANT
        ["SEC-0005", "LN-000005", "MBS", 100000, 0.06, "Monthly", "2026-06-01", 500.0, True],
    ])
    df = calculator.calculate_expected_payments(df)
    result = calculator.evaluate_compliance(df)
    assert result["compliance_status"].iloc[0] == "COMPLIANT"


def test_trust_flag_tiny_deviation_flagged(calculator):
    """Even a tiny deviation for trust-required should be REQUIRES_REVIEW."""
    df = _make_collateral([
        # Expected = 500.0, actual = 500.01 → 0.002% deviation
        ["SEC-0006", "LN-000006", "MBS", 100000, 0.06, "Monthly", "2026-06-01", 500.50, True],
    ])
    df = calculator.calculate_expected_payments(df)
    result = calculator.evaluate_compliance(df)
    assert result["compliance_status"].iloc[0] in ["REQUIRES_REVIEW", "NON-COMPLIANT"]


# ─── Test 6: Standard 2% Tolerance — Compliant ──────────────────────

def test_standard_within_tolerance(calculator):
    """Within 2% should be COMPLIANT for non-trust securities."""
    df = _make_collateral([
        # Expected = 500.0, actual = 505.0 → 1.0% deviation
        ["SEC-0007", "LN-000007", "ABS", 100000, 0.06, "Monthly", "2026-06-01", 505.0, False],
    ])
    df = calculator.calculate_expected_payments(df)
    result = calculator.evaluate_compliance(df)
    assert result["compliance_status"].iloc[0] == "COMPLIANT"


# ─── Test 7: Standard 2% Tolerance — Non-Compliant ──────────────────

def test_standard_exceeds_tolerance(calculator):
    """Beyond 2% should be NON-COMPLIANT for non-trust securities."""
    df = _make_collateral([
        # Expected = 500.0, actual = 550.0 → 10% deviation
        ["SEC-0008", "LN-000008", "CLO", 100000, 0.06, "Monthly", "2026-06-01", 550.0, False],
    ])
    df = calculator.calculate_expected_payments(df)
    result = calculator.evaluate_compliance(df)
    assert result["compliance_status"].iloc[0] == "NON-COMPLIANT"


# ─── Test 8: Compliance Summary ──────────────────────────────────────

def test_compliance_summary_structure(calculator):
    """Summary should have by_collateral_type and totals."""
    df = _make_collateral([
        ["SEC-0001", "LN-000001", "MBS", 100000, 0.06, "Monthly", "2026-06-01", 500.0, False],
        ["SEC-0002", "LN-000002", "ABS", 100000, 0.06, "Monthly", "2026-06-01", 550.0, False],
    ])
    df = calculator.calculate_expected_payments(df)
    df = calculator.evaluate_compliance(df)
    # Rename for summary
    report = df.rename(columns={"calculated_payment": "actual_payment"})
    summary = calculator.get_compliance_summary(report)
    assert "by_collateral_type" in summary
    assert "totals" in summary
