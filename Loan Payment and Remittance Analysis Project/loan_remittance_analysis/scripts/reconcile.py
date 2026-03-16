"""
reconcile.py — Core Reconciliation Engine

Matches loan payments to remittance records, detects discrepancies,
and generates reconciliation summaries.
"""

import os
import sys
from datetime import timedelta

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


class ReconciliationEngine:
    """Reconcile loan payments against remittance records."""

    def __init__(
        self,
        tolerance: float = config.TOLERANCE_AMOUNT,
        large_variance: float = config.LARGE_VARIANCE_THRESHOLD,
    ):
        self.tolerance = tolerance
        self.large_variance = large_variance

    # ──────────────────────────────────────────────────────────────────
    #  Match Payments to Remittances
    # ──────────────────────────────────────────────────────────────────
    def match_payments_to_remittances(
        self,
        payments_df: pd.DataFrame,
        remittance_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Merge on loan_id + payment_date (exact), then fuzzy ±2 business days.
        Classify each record: MATCHED, UNMATCHED, PARTIAL, REVERSED.
        """
        pay = payments_df.copy()
        rem = remittance_df.copy()

        # Ensure date columns are datetime
        pay["payment_date"] = pd.to_datetime(pay["payment_date"], errors="coerce")
        rem["remittance_date"] = pd.to_datetime(rem["remittance_date"], errors="coerce")

        # ── Step 1: Exact match on loan_id + date ──
        exact = pay.merge(
            rem,
            left_on=["loan_id", "payment_date"],
            right_on=["loan_id", "remittance_date"],
            how="left",
            suffixes=("", "_rem"),
        )

        matched_mask = exact["remittance_id"].notna()
        exact["match_type"] = "UNMATCHED"
        exact.loc[matched_mask, "match_type"] = "EXACT"

        # ── Step 2: Fuzzy match for unmatched (±2 business days) ──
        unmatched = exact[exact["match_type"] == "UNMATCHED"].copy()
        if len(unmatched) > 0 and len(rem) > 0:
            fuzzy_matches = self._fuzzy_date_match(unmatched, rem)
            if len(fuzzy_matches) > 0:
                # Update exact df with fuzzy matches
                for idx, row in fuzzy_matches.iterrows():
                    mask = exact["loan_id"] == row["loan_id"]
                    mask &= exact["payment_date"] == row["payment_date"]
                    mask &= exact["match_type"] == "UNMATCHED"
                    match_indices = exact[mask].index
                    if len(match_indices) > 0:
                        fidx = match_indices[0]
                        exact.loc[fidx, "remittance_id"] = row.get("remittance_id_fuzzy", None)
                        exact.loc[fidx, "remittance_amount"] = row.get("remittance_amount_fuzzy", None)
                        exact.loc[fidx, "remittance_date"] = row.get("remittance_date_fuzzy", None)
                        exact.loc[fidx, "match_type"] = "FUZZY"

        # ── Step 3: Classify final status ──
        recon = exact.copy()
        recon["variance"] = np.where(
            recon["remittance_amount"].notna(),
            recon["actual_payment_amount"] - recon["remittance_amount"],
            recon["actual_payment_amount"],
        )

        recon["reconciliation_status"] = "UNMATCHED"
        # Matched (exact or fuzzy)
        matched = recon["match_type"].isin(["EXACT", "FUZZY"])
        small_var = recon["variance"].abs() <= self.tolerance
        recon.loc[matched & small_var, "reconciliation_status"] = "MATCHED"
        recon.loc[matched & ~small_var, "reconciliation_status"] = "PARTIAL"

        # Reversed
        if "payment_status" in recon.columns:
            reversed_mask = recon["payment_status"].str.lower() == "reversed"
            recon.loc[reversed_mask, "reconciliation_status"] = "REVERSED"

        # Clean up columns
        keep_cols = [
            "loan_id", "borrower_name", "payment_date", "scheduled_payment_amount",
            "actual_payment_amount", "payment_type", "payment_status",
            "bank_reference_id", "pool_id", "remittance_id", "remittance_date",
            "remittance_amount", "trust_account_id", "match_type",
            "variance", "reconciliation_status",
        ]
        keep_cols = [c for c in keep_cols if c in recon.columns]
        recon = recon[keep_cols].copy()
        return recon

    def _fuzzy_date_match(
        self,
        unmatched_df: pd.DataFrame,
        rem_df: pd.DataFrame,
        biz_days: int = 2,
    ) -> pd.DataFrame:
        """Attempt to match unmatched payments to remittances within ±2 business days."""
        results = []
        for _, pay_row in unmatched_df.iterrows():
            loan = pay_row["loan_id"]
            pdate = pay_row["payment_date"]
            if pd.isna(pdate):
                continue

            # Filter remittances for same loan
            rem_for_loan = rem_df[rem_df["loan_id"] == loan]
            if len(rem_for_loan) == 0:
                continue

            # Check if any remittance date is within ±2 business days
            for _, rem_row in rem_for_loan.iterrows():
                rdate = rem_row["remittance_date"]
                if pd.isna(rdate):
                    continue
                day_diff = abs((pdate - rdate).days)
                if day_diff <= (biz_days + 2):  # ±2 biz days ≈ ±4 calendar days
                    results.append({
                        "loan_id": loan,
                        "payment_date": pdate,
                        "remittance_id_fuzzy": rem_row["remittance_id"],
                        "remittance_amount_fuzzy": rem_row["remittance_amount"],
                        "remittance_date_fuzzy": rdate,
                    })
                    break  # take first match

        return pd.DataFrame(results) if results else pd.DataFrame()

    # ──────────────────────────────────────────────────────────────────
    #  Detect Discrepancies
    # ──────────────────────────────────────────────────────────────────
    def detect_discrepancies(self, reconciliation_df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag and categorize discrepancies in the reconciliation output.
        """
        df = reconciliation_df.copy()

        # Only examine rows with a variance or unmatched status
        disc_mask = (
            (df["variance"].abs() > self.tolerance) |
            (df["reconciliation_status"].isin(["UNMATCHED", "PARTIAL", "REVERSED"]))
        )
        disc = df[disc_mask].copy()

        if len(disc) == 0:
            return pd.DataFrame(columns=[
                "loan_id", "variance", "reconciliation_status",
                "discrepancy_category", "severity", "recommended_action",
            ])

        # Categorize
        disc["discrepancy_category"] = "OTHER"
        disc.loc[disc["variance"] < -self.tolerance, "discrepancy_category"] = "SHORT_PAY"
        disc.loc[disc["variance"] > self.tolerance, "discrepancy_category"] = "OVER_PAY"
        disc.loc[
            disc["reconciliation_status"] == "UNMATCHED",
            "discrepancy_category",
        ] = "MISSING_REMITTANCE"

        # Check for duplicates (same loan + date + amount appears >1 time)
        dup_cols = ["loan_id", "payment_date", "actual_payment_amount"]
        dup_cols = [c for c in dup_cols if c in disc.columns]
        if dup_cols:
            dup_mask = disc.duplicated(subset=dup_cols, keep=False)
            disc.loc[dup_mask, "discrepancy_category"] = "DUPLICATE"

        # Severity
        disc["severity"] = "Low"
        disc.loc[disc["variance"].abs() > self.large_variance, "severity"] = "High"
        disc.loc[
            (disc["variance"].abs() > self.tolerance) &
            (disc["variance"].abs() <= self.large_variance),
            "severity",
        ] = "Medium"
        disc.loc[disc["reconciliation_status"] == "UNMATCHED", "severity"] = "High"

        # Recommended action
        action_map = {
            "SHORT_PAY": "Contact borrower for remaining balance",
            "OVER_PAY": "Verify overpayment; apply to principal or refund",
            "MISSING_REMITTANCE": "Escalate to servicer for missing remittance",
            "DUPLICATE": "Review for potential double-posting; reverse if confirmed",
            "OTHER": "Manual review required",
        }
        disc["recommended_action"] = disc["discrepancy_category"].map(action_map)
        return disc

    # ──────────────────────────────────────────────────────────────────
    #  Reconciliation Summary
    # ──────────────────────────────────────────────────────────────────
    def generate_reconciliation_summary(self, reconciliation_df: pd.DataFrame) -> dict:
        """Generate high-level reconciliation summary statistics."""
        total = len(reconciliation_df)
        status_counts = reconciliation_df["reconciliation_status"].value_counts().to_dict()
        matched = status_counts.get("MATCHED", 0)
        unmatched = status_counts.get("UNMATCHED", 0)
        partial = status_counts.get("PARTIAL", 0)
        reversed_count = status_counts.get("REVERSED", 0)

        total_variance = reconciliation_df["variance"].abs().sum()
        disc_rate = ((unmatched + partial) / total * 100) if total > 0 else 0

        # Breakdown by pool_id
        pool_summary = {}
        if "pool_id" in reconciliation_df.columns:
            pool_groups = reconciliation_df.groupby("pool_id")
            for pool, grp in pool_groups:
                pool_summary[pool] = {
                    "total": len(grp),
                    "matched": int((grp["reconciliation_status"] == "MATCHED").sum()),
                    "unmatched": int((grp["reconciliation_status"] == "UNMATCHED").sum()),
                    "variance": round(grp["variance"].abs().sum(), 2),
                }

        # Breakdown by trust_account_id
        trust_summary = {}
        if "trust_account_id" in reconciliation_df.columns:
            trust_groups = reconciliation_df.groupby("trust_account_id")
            for trust, grp in trust_groups:
                trust_summary[trust] = {
                    "total": len(grp),
                    "matched": int((grp["reconciliation_status"] == "MATCHED").sum()),
                    "unmatched": int((grp["reconciliation_status"] == "UNMATCHED").sum()),
                    "variance": round(grp["variance"].abs().sum(), 2),
                }

        return {
            "total_payments_processed": total,
            "matched": matched,
            "unmatched": unmatched,
            "partial": partial,
            "reversed": reversed_count,
            "total_dollar_variance": round(total_variance, 2),
            "discrepancy_rate_pct": round(disc_rate, 2),
            "by_pool": pool_summary,
            "by_trust_account": trust_summary,
        }
