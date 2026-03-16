"""
test_reconcile.py — Unit tests for the reconciliation engine.
"""

import sys
import os
import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.reconcile import ReconciliationEngine


@pytest.fixture
def engine():
    return ReconciliationEngine(tolerance=0.01, large_variance=500.00)


def _make_payments(rows):
    """Helper to create a payments DataFrame."""
    return pd.DataFrame(rows, columns=[
        "loan_id", "borrower_name", "payment_date", "scheduled_payment_amount",
        "actual_payment_amount", "payment_type", "payment_status",
        "bank_reference_id", "pool_id",
    ])


def _make_remittances(rows):
    """Helper to create a remittance DataFrame."""
    return pd.DataFrame(rows, columns=[
        "remittance_id", "loan_id", "remittance_date", "remittance_amount",
        "servicer_name", "wire_reference", "trust_account_id", "remittance_status",
    ])


# ─── Test 1: Exact Match ────────────────────────────────────────────

def test_exact_match(engine):
    """Payments with matching remittance on same date should be MATCHED."""
    payments = _make_payments([
        ["LN-000001", "John", "2025-06-15", 1000, 1000, "Principal", "Posted", "REF1", "POOL-A"],
    ])
    remittances = _make_remittances([
        ["REM-000001", "LN-000001", "2025-06-15", 1000, "Svc", "WIRE1", "TRU-001", "Matched"],
    ])
    result = engine.match_payments_to_remittances(payments, remittances)
    assert result["reconciliation_status"].iloc[0] == "MATCHED"
    assert abs(result["variance"].iloc[0]) <= 0.01


# ─── Test 2: Unmatched ──────────────────────────────────────────────

def test_unmatched_no_remittance(engine):
    """Payments with no matching remittance should be UNMATCHED."""
    payments = _make_payments([
        ["LN-000001", "John", "2025-06-15", 1000, 1000, "Principal", "Posted", "REF1", "POOL-A"],
    ])
    remittances = _make_remittances([
        ["REM-000001", "LN-000099", "2025-06-15", 1000, "Svc", "WIRE1", "TRU-001", "Matched"],
    ])
    result = engine.match_payments_to_remittances(payments, remittances)
    assert result["reconciliation_status"].iloc[0] in ["UNMATCHED", "PARTIAL"]


# ─── Test 3: Partial Match (Variance) ───────────────────────────────

def test_partial_match_variance(engine):
    """Matched but with a variance should be PARTIAL."""
    payments = _make_payments([
        ["LN-000001", "John", "2025-06-15", 1000, 1000, "Principal", "Posted", "REF1", "POOL-A"],
    ])
    remittances = _make_remittances([
        ["REM-000001", "LN-000001", "2025-06-15", 900, "Svc", "WIRE1", "TRU-001", "Matched"],
    ])
    result = engine.match_payments_to_remittances(payments, remittances)
    assert result["reconciliation_status"].iloc[0] == "PARTIAL"
    assert abs(result["variance"].iloc[0] - 100.0) < 0.01


# ─── Test 4: Reversed Status ────────────────────────────────────────

def test_reversed_payment(engine):
    """Reversed payments should be classified as REVERSED."""
    payments = _make_payments([
        ["LN-000001", "John", "2025-06-15", 1000, 1000, "Principal", "Reversed", "REF1", "POOL-A"],
    ])
    remittances = _make_remittances([
        ["REM-000001", "LN-000001", "2025-06-15", 1000, "Svc", "WIRE1", "TRU-001", "Matched"],
    ])
    result = engine.match_payments_to_remittances(payments, remittances)
    assert result["reconciliation_status"].iloc[0] == "REVERSED"


# ─── Test 5: Discrepancy Detection ──────────────────────────────────

def test_detect_discrepancies_short_pay(engine):
    """Short pay should be categorized correctly."""
    recon = pd.DataFrame({
        "loan_id": ["LN-000001"],
        "payment_date": ["2025-06-15"],
        "actual_payment_amount": [800],
        "remittance_amount": [1000],
        "variance": [-200],
        "reconciliation_status": ["PARTIAL"],
    })
    disc = engine.detect_discrepancies(recon)
    assert len(disc) > 0
    assert disc["discrepancy_category"].iloc[0] == "SHORT_PAY"


def test_detect_discrepancies_over_pay(engine):
    """Over pay should be categorized correctly."""
    recon = pd.DataFrame({
        "loan_id": ["LN-000001"],
        "payment_date": ["2025-06-15"],
        "actual_payment_amount": [1200],
        "remittance_amount": [1000],
        "variance": [200],
        "reconciliation_status": ["PARTIAL"],
    })
    disc = engine.detect_discrepancies(recon)
    assert len(disc) > 0
    assert disc["discrepancy_category"].iloc[0] == "OVER_PAY"


# ─── Test 6: Severity Classification ────────────────────────────────

def test_severity_high_for_large_variance(engine):
    """Large variance should be classified as High severity."""
    recon = pd.DataFrame({
        "loan_id": ["LN-000001"],
        "payment_date": ["2025-06-15"],
        "actual_payment_amount": [1000],
        "remittance_amount": [200],
        "variance": [800],
        "reconciliation_status": ["PARTIAL"],
    })
    disc = engine.detect_discrepancies(recon)
    assert disc["severity"].iloc[0] == "High"


def test_severity_medium_for_small_variance(engine):
    """Small-but-nonzero variance should be Medium."""
    recon = pd.DataFrame({
        "loan_id": ["LN-000001"],
        "payment_date": ["2025-06-15"],
        "actual_payment_amount": [1000],
        "remittance_amount": [900],
        "variance": [100],
        "reconciliation_status": ["PARTIAL"],
    })
    disc = engine.detect_discrepancies(recon)
    assert disc["severity"].iloc[0] == "Medium"


# ─── Test 7: Reconciliation Summary ─────────────────────────────────

def test_generate_reconciliation_summary(engine):
    """Summary should contain expected keys."""
    recon = pd.DataFrame({
        "loan_id": ["LN-000001", "LN-000002", "LN-000003"],
        "variance": [0, 100, 0],
        "reconciliation_status": ["MATCHED", "PARTIAL", "UNMATCHED"],
        "pool_id": ["POOL-A", "POOL-A", "POOL-B"],
        "trust_account_id": ["TRU-001", "TRU-001", "TRU-002"],
    })
    summary = engine.generate_reconciliation_summary(recon)
    assert summary["total_payments_processed"] == 3
    assert summary["matched"] == 1
    assert summary["unmatched"] == 1
    assert summary["partial"] == 1
    assert "by_pool" in summary
    assert "POOL-A" in summary["by_pool"]


# ─── Test 8: Empty DataFrames ───────────────────────────────────────

def test_empty_payments(engine):
    """Empty payments should produce empty reconciliation."""
    payments = _make_payments([])
    remittances = _make_remittances([
        ["REM-000001", "LN-000001", "2025-06-15", 1000, "Svc", "WIRE1", "TRU-001", "Matched"],
    ])
    result = engine.match_payments_to_remittances(payments, remittances)
    assert len(result) == 0
