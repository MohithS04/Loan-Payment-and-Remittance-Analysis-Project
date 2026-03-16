"""
ingest.py — Synthetic Data Generator & Loader

Generates three realistic CSV datasets (loan_payments, remittance_records,
collateral_schedule) with 150-170 deliberately injected errors.
Also produces ground_truth_errors.csv for validation scoring.
"""

import os
import sys
import random
import hashlib
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ── Setup path so config is importable ──────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

fake = Faker()
Faker.seed(config.RANDOM_SEED)
np.random.seed(config.RANDOM_SEED)
random.seed(config.RANDOM_SEED)


# =========================================================================
#  Helpers
# =========================================================================

def _random_dates(start: str, end: str, n: int) -> list:
    """Generate n random dates between start and end (YYYY-MM-DD)."""
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    delta = (end_dt - start_dt).days
    return [(start_dt + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d") for _ in range(n)]


def _is_weekend(date_str: str) -> bool:
    return datetime.strptime(date_str, "%Y-%m-%d").weekday() >= 5


def _is_holiday(date_str: str) -> bool:
    return date_str in config.US_FEDERAL_HOLIDAYS


def _bank_ref() -> str:
    return hashlib.md5(fake.uuid4().encode()).hexdigest()[:12].upper()


# =========================================================================
#  Dataset 1 — Loan Payments
# =========================================================================

def generate_loan_payments(n: int = config.NUM_LOAN_PAYMENTS) -> pd.DataFrame:
    """Generate loan_payments.csv with n rows."""
    loan_ids = [f"LN-{i:06d}" for i in range(1, n + 1)]
    payment_dates = _random_dates("2025-03-15", "2026-03-14", n)
    scheduled = np.round(np.random.uniform(500, 50000, n), 2)

    # 85% match scheduled, 15% discrepancy
    actual = scheduled.copy()
    disc_mask = np.random.rand(n) < 0.15
    for idx in np.where(disc_mask)[0]:
        kind = random.choice(["short", "over", "zero"])
        if kind == "short":
            actual[idx] = round(scheduled[idx] * random.uniform(0.5, 0.95), 2)
        elif kind == "over":
            actual[idx] = round(scheduled[idx] * random.uniform(1.05, 1.30), 2)
        else:
            actual[idx] = 0.0

    df = pd.DataFrame({
        "loan_id": loan_ids,
        "borrower_name": [fake.name() for _ in range(n)],
        "payment_date": payment_dates,
        "scheduled_payment_amount": scheduled,
        "actual_payment_amount": actual,
        "payment_type": np.random.choice(["Principal", "Interest", "Escrow", "Fee"], n),
        "payment_status": np.random.choice(["Posted", "Pending", "Reversed"], n, p=[0.75, 0.15, 0.10]),
        "bank_reference_id": [_bank_ref() for _ in range(n)],
        "pool_id": np.random.choice([f"POOL-{c}" for c in "ABCDEF"], n),
    })
    return df


# =========================================================================
#  Dataset 2 — Remittance Records
# =========================================================================

def generate_remittance_records(
    payments_df: pd.DataFrame,
    n: int = config.NUM_REMITTANCE_RECORDS,
) -> pd.DataFrame:
    """Generate remittance_records.csv linked to loan_payments."""
    # Sample loan_ids from payments (some payments may have no remittance)
    sampled = payments_df.sample(n=n, replace=True, random_state=config.RANDOM_SEED)

    remittance_dates = []
    for d in sampled["payment_date"]:
        offset = random.randint(0, 5)
        rd = (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=offset)).strftime("%Y-%m-%d")
        remittance_dates.append(rd)

    df = pd.DataFrame({
        "remittance_id": [f"REM-{i:06d}" for i in range(1, n + 1)],
        "loan_id": sampled["loan_id"].values,
        "remittance_date": remittance_dates,
        "remittance_amount": sampled["actual_payment_amount"].values.copy(),
        "servicer_name": [fake.company() for _ in range(n)],
        "wire_reference": [f"WIRE-{_bank_ref()}" for _ in range(n)],
        "trust_account_id": np.random.choice([f"TRU-{i:03d}" for i in range(1, 11)], n),
        "remittance_status": np.random.choice(["Matched", "Unmatched", "In-Review"], n, p=[0.70, 0.20, 0.10]),
    })
    return df


# =========================================================================
#  Dataset 3 — Collateral Schedule
# =========================================================================

def generate_collateral_schedule(
    payments_df: pd.DataFrame,
    n: int = config.NUM_COLLATERAL_RECORDS,
) -> pd.DataFrame:
    """Generate collateral_schedule.csv."""
    security_ids = [f"SEC-{i:04d}" for i in range(1, 101)] * (n // 100 + 1)
    security_ids = security_ids[:n]

    loan_ids = payments_df["loan_id"].sample(n=n, replace=True, random_state=config.RANDOM_SEED).values
    frequencies = np.random.choice(["Monthly", "Quarterly", "Semi-Annual"], n, p=[0.5, 0.3, 0.2])
    face_values = np.round(np.random.uniform(10000, 500000, n), 2)
    coupon_rates = np.round(np.random.uniform(0.025, 0.075, n), 4)

    periods_map = config.PAYMENT_FREQUENCY_MAP
    periods = np.array([periods_map[f] for f in frequencies], dtype=float)
    calculated = np.round(face_values * (coupon_rates / periods), 2)

    next_dates = _random_dates("2026-03-15", "2026-12-31", n)

    df = pd.DataFrame({
        "security_id": security_ids,
        "loan_id": loan_ids,
        "collateral_type": np.random.choice(["MBS", "ABS", "CMO", "CLO"], n),
        "face_value": face_values,
        "coupon_rate": coupon_rates,
        "payment_frequency": frequencies,
        "next_payment_date": next_dates,
        "calculated_payment": calculated,
        "trust_requirement_flag": np.random.choice([True, False], n, p=[0.30, 0.70]),
    })
    return df


# =========================================================================
#  Error Injection
# =========================================================================

def inject_errors(
    payments_df: pd.DataFrame,
    remittance_df: pd.DataFrame,
    collateral_df: pd.DataFrame,
    target_errors: int = 160,
) -> tuple:
    """
    Inject 150-170 deliberate errors across all three datasets.
    Returns (modified_payments, modified_remittance, modified_collateral, ground_truth_df).
    """
    errors = []
    error_id = 0

    payments_df = payments_df.copy()
    remittance_df = remittance_df.copy()
    collateral_df = collateral_df.copy()

    # ── Error Type 1: Duplicate payment entries (same loan_id + date + amount) ──
    n_dup = 25
    dup_indices = payments_df.sample(n=n_dup, random_state=1).index
    dup_rows = payments_df.loc[dup_indices].copy()
    # Give them new bank references so they look like separate entries
    dup_rows["bank_reference_id"] = [_bank_ref() for _ in range(n_dup)]
    payments_df = pd.concat([payments_df, dup_rows], ignore_index=True)
    for idx in dup_indices:
        error_id += 1
        errors.append({
            "error_id": error_id,
            "error_type": "DUPLICATE_PAYMENT",
            "dataset": "loan_payments",
            "record_id": payments_df.loc[idx, "loan_id"],
            "description": f"Duplicate entry for {payments_df.loc[idx, 'loan_id']} on {payments_df.loc[idx, 'payment_date']}",
        })

    # ── Error Type 2: Amount mismatches between payments and remittances ──
    n_mismatch = 30
    mismatch_idx = remittance_df.sample(n=n_mismatch, random_state=2).index
    for idx in mismatch_idx:
        original = remittance_df.loc[idx, "remittance_amount"]
        # Perturb by 5-15%
        factor = random.choice([random.uniform(0.85, 0.95), random.uniform(1.05, 1.15)])
        remittance_df.loc[idx, "remittance_amount"] = round(original * factor, 2)
        error_id += 1
        errors.append({
            "error_id": error_id,
            "error_type": "AMOUNT_MISMATCH",
            "dataset": "remittance_records",
            "record_id": remittance_df.loc[idx, "remittance_id"],
            "description": f"Remittance amount differs from payment for {remittance_df.loc[idx, 'loan_id']}",
        })

    # ── Error Type 3: Missing remittance for posted payments ──
    n_missing = 25
    posted_payments = payments_df[payments_df["payment_status"] == "Posted"]
    posted_loan_ids = posted_payments["loan_id"].unique()
    # Find loan_ids that have remittance records and remove some
    with_remittance = remittance_df[remittance_df["loan_id"].isin(posted_loan_ids)]
    if len(with_remittance) >= n_missing:
        drop_idx = with_remittance.sample(n=n_missing, random_state=3).index
        for idx in drop_idx:
            error_id += 1
            errors.append({
                "error_id": error_id,
                "error_type": "MISSING_REMITTANCE",
                "dataset": "remittance_records",
                "record_id": remittance_df.loc[idx, "loan_id"],
                "description": f"Posted payment {remittance_df.loc[idx, 'loan_id']} has no remittance after removal",
            })
        remittance_df = remittance_df.drop(drop_idx).reset_index(drop=True)

    # ── Error Type 4: Reversed payments still showing as active remittance ──
    n_reversed = 20
    reversed_payments = payments_df[payments_df["payment_status"] == "Reversed"]
    if len(reversed_payments) >= n_reversed:
        rev_sample = reversed_payments.sample(n=n_reversed, random_state=4)
        for _, row in rev_sample.iterrows():
            # Ensure there is an active remittance for this reversed payment
            new_rem = {
                "remittance_id": f"REM-{len(remittance_df) + 1:06d}",
                "loan_id": row["loan_id"],
                "remittance_date": row["payment_date"],
                "remittance_amount": row["actual_payment_amount"],
                "servicer_name": fake.company(),
                "wire_reference": f"WIRE-{_bank_ref()}",
                "trust_account_id": f"TRU-{random.randint(1,10):03d}",
                "remittance_status": "Matched",
            }
            remittance_df = pd.concat([remittance_df, pd.DataFrame([new_rem])], ignore_index=True)
            error_id += 1
            errors.append({
                "error_id": error_id,
                "error_type": "REVERSED_STILL_ACTIVE",
                "dataset": "both",
                "record_id": row["loan_id"],
                "description": f"Reversed payment {row['loan_id']} still has active remittance",
            })

    # ── Error Type 5: Coupon payments outside ±2% of calculated_payment ──
    n_coupon = 30
    coupon_idx = collateral_df.sample(n=n_coupon, random_state=5).index
    for idx in coupon_idx:
        original = collateral_df.loc[idx, "calculated_payment"]
        # Perturb by 3-10%
        factor = random.choice([random.uniform(0.87, 0.97), random.uniform(1.03, 1.10)])
        collateral_df.loc[idx, "calculated_payment"] = round(original * factor, 2)
        error_id += 1
        errors.append({
            "error_id": error_id,
            "error_type": "COUPON_OUTLIER",
            "dataset": "collateral_schedule",
            "record_id": collateral_df.loc[idx, "security_id"],
            "description": f"Coupon payment for {collateral_df.loc[idx, 'security_id']} deviates >2% from expected",
        })

    # ── Error Type 6: Payments posted on non-business days ──
    n_nonbiz = target_errors - error_id  # fill remainder
    n_nonbiz = max(n_nonbiz, 15)
    # Find rows NOT already on weekends/holidays and move them
    biz_idx = []
    for idx in payments_df.index:
        d = payments_df.loc[idx, "payment_date"]
        if not _is_weekend(d) and not _is_holiday(d):
            biz_idx.append(idx)
        if len(biz_idx) >= n_nonbiz * 3:
            break

    chosen = random.sample(biz_idx, min(n_nonbiz, len(biz_idx)))
    for idx in chosen:
        d = datetime.strptime(payments_df.loc[idx, "payment_date"], "%Y-%m-%d")
        # Move to nearest Saturday or Sunday
        days_to_sat = (5 - d.weekday()) % 7
        if days_to_sat == 0:
            days_to_sat = 5  # move forward to Saturday
        new_date = (d + timedelta(days=days_to_sat)).strftime("%Y-%m-%d")
        payments_df.loc[idx, "payment_date"] = new_date
        error_id += 1
        errors.append({
            "error_id": error_id,
            "error_type": "NON_BUSINESS_DAY",
            "dataset": "loan_payments",
            "record_id": payments_df.loc[idx, "loan_id"],
            "description": f"Payment {payments_df.loc[idx, 'loan_id']} posted on non-business day {new_date}",
        })

    ground_truth_df = pd.DataFrame(errors)
    return payments_df, remittance_df, collateral_df, ground_truth_df


# =========================================================================
#  Public API
# =========================================================================

def load_raw_data() -> dict:
    """Load pre-generated CSVs from data/raw/. Returns dict of DataFrames."""
    return {
        "loan_payments": pd.read_csv(os.path.join(config.DATA_RAW, "loan_payments.csv")),
        "remittance_records": pd.read_csv(os.path.join(config.DATA_RAW, "remittance_records.csv")),
        "collateral_schedule": pd.read_csv(os.path.join(config.DATA_RAW, "collateral_schedule.csv")),
        "ground_truth_errors": pd.read_csv(os.path.join(config.DATA_RAW, "ground_truth_errors.csv")),
    }


def generate_and_save() -> dict:
    """Generate all synthetic datasets, inject errors, save to disk, and return DataFrames."""
    os.makedirs(config.DATA_RAW, exist_ok=True)

    print("  ▸ Generating loan payments …")
    payments = generate_loan_payments()

    print("  ▸ Generating remittance records …")
    remittance = generate_remittance_records(payments)

    print("  ▸ Generating collateral schedule …")
    collateral = generate_collateral_schedule(payments)

    print(f"  ▸ Injecting 150-170 deliberate errors …")
    payments, remittance, collateral, ground_truth = inject_errors(payments, remittance, collateral)

    payments.to_csv(os.path.join(config.DATA_RAW, "loan_payments.csv"), index=False)
    remittance.to_csv(os.path.join(config.DATA_RAW, "remittance_records.csv"), index=False)
    collateral.to_csv(os.path.join(config.DATA_RAW, "collateral_schedule.csv"), index=False)
    ground_truth.to_csv(os.path.join(config.DATA_RAW, "ground_truth_errors.csv"), index=False)

    print(f"  ✓ Saved {len(payments)} loan payments")
    print(f"  ✓ Saved {len(remittance)} remittance records")
    print(f"  ✓ Saved {len(collateral)} collateral records")
    print(f"  ✓ Saved {len(ground_truth)} ground truth errors")

    return {
        "loan_payments": payments,
        "remittance_records": remittance,
        "collateral_schedule": collateral,
        "ground_truth_errors": ground_truth,
    }


if __name__ == "__main__":
    generate_and_save()
