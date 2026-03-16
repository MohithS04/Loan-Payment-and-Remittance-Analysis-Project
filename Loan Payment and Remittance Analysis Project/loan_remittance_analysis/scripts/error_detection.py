"""
error_detection.py — Transaction Error Scanner

Runs 6 validation checks across all datasets and compiles a
master error log. Compares detected errors against ground truth
to compute precision, recall, and F1 score.
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


class ErrorScanner:
    """Full-suite transaction validation engine."""

    def __init__(self):
        self.errors = []
        self.holidays = set(config.US_FEDERAL_HOLIDAYS)

    # ──────────────────────────────────────────────────────────────────
    #  Check 1 — Duplicate Transactions
    # ──────────────────────────────────────────────────────────────────
    def check_duplicates(self, payments_df: pd.DataFrame) -> dict:
        """Detect rows where (loan_id, payment_date, actual_payment_amount) are identical."""
        df = payments_df.copy()
        df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")
        dup_cols = ["loan_id", "payment_date", "actual_payment_amount"]
        dup_mask = df.duplicated(subset=dup_cols, keep=False)
        dup_df = df[dup_mask]

        affected_ids = dup_df["loan_id"].unique().tolist()
        result = {
            "error_type": "DUPLICATE_PAYMENT",
            "affected_record_ids": affected_ids,
            "error_count": len(affected_ids),
            "severity": "High",
            "detail_df": dup_df,
        }
        self.errors.append(result)
        return result

    # ──────────────────────────────────────────────────────────────────
    #  Check 2 — Orphaned Remittances
    # ──────────────────────────────────────────────────────────────────
    def check_orphaned_remittances(
        self,
        remittance_df: pd.DataFrame,
        payments_df: pd.DataFrame,
    ) -> dict:
        """Find remittance records not linked to any loan payment."""
        valid_loans = set(payments_df["loan_id"].unique())
        orphan_mask = ~remittance_df["loan_id"].isin(valid_loans)
        orphan_df = remittance_df[orphan_mask]

        affected_ids = orphan_df["remittance_id"].tolist()
        result = {
            "error_type": "ORPHANED_REMITTANCE",
            "affected_record_ids": affected_ids,
            "error_count": len(affected_ids),
            "severity": "Medium",
            "detail_df": orphan_df,
        }
        self.errors.append(result)
        return result

    # ──────────────────────────────────────────────────────────────────
    #  Check 3 — Reversed Payment Leakage
    # ──────────────────────────────────────────────────────────────────
    def check_reversed_leakage(
        self,
        payments_df: pd.DataFrame,
        remittance_df: pd.DataFrame,
    ) -> dict:
        """Find reversed payments that still have active remittance records."""
        reversed_loans = payments_df[
            payments_df["payment_status"].str.lower() == "reversed"
        ]["loan_id"].unique()

        active_rem = remittance_df[
            remittance_df["remittance_status"].str.lower().isin(["matched", "in-review"])
        ]

        leaked = active_rem[active_rem["loan_id"].isin(reversed_loans)]
        affected_ids = leaked["loan_id"].unique().tolist()

        result = {
            "error_type": "REVERSED_STILL_ACTIVE",
            "affected_record_ids": affected_ids,
            "error_count": len(affected_ids),
            "severity": "High",
            "detail_df": leaked,
        }
        self.errors.append(result)
        return result

    # ──────────────────────────────────────────────────────────────────
    #  Check 4 — Non-Business Day Payments
    # ──────────────────────────────────────────────────────────────────
    def check_non_business_day(self, payments_df: pd.DataFrame) -> dict:
        """Flag payments posted on weekends or US federal holidays."""
        df = payments_df.copy()
        df["payment_date_dt"] = pd.to_datetime(df["payment_date"], errors="coerce")

        weekend_mask = df["payment_date_dt"].dt.weekday >= 5
        holiday_mask = df["payment_date_dt"].dt.strftime("%Y-%m-%d").isin(self.holidays)
        non_biz_mask = weekend_mask | holiday_mask

        non_biz_df = df[non_biz_mask]
        affected_ids = non_biz_df["loan_id"].tolist()

        result = {
            "error_type": "NON_BUSINESS_DAY",
            "affected_record_ids": affected_ids,
            "error_count": len(affected_ids),
            "severity": "Low",
            "detail_df": non_biz_df,
        }
        self.errors.append(result)
        return result

    # ──────────────────────────────────────────────────────────────────
    #  Check 5 — Collateral Coupon Outliers
    # ──────────────────────────────────────────────────────────────────
    def check_collateral_outliers(self, collateral_df: pd.DataFrame) -> dict:
        """Flag collateral payments deviating >2% from expected calculation."""
        df = collateral_df.copy()
        freq_map = config.PAYMENT_FREQUENCY_MAP
        df["payment_periods"] = df["payment_frequency"].map(freq_map)
        df["expected_payment"] = np.round(
            df["face_value"] * (df["coupon_rate"] / df["payment_periods"]),
            2,
        )
        df["variance_pct"] = np.where(
            df["expected_payment"] != 0,
            np.abs(df["calculated_payment"] - df["expected_payment"]) / df["expected_payment"],
            0.0,
        )

        outlier_mask = df["variance_pct"] > config.COLLATERAL_TOLERANCE_PCT
        outlier_df = df[outlier_mask]
        affected_ids = outlier_df["security_id"].tolist()

        result = {
            "error_type": "COUPON_OUTLIER",
            "affected_record_ids": affected_ids,
            "error_count": len(affected_ids),
            "severity": "Medium",
            "detail_df": outlier_df,
        }
        self.errors.append(result)
        return result

    # ──────────────────────────────────────────────────────────────────
    #  Check 6 — Stale Pending Payments
    # ──────────────────────────────────────────────────────────────────
    def check_stale_pending(self, payments_df: pd.DataFrame) -> dict:
        """Flag payments with status='Pending' older than STALE_PENDING_DAYS."""
        df = payments_df.copy()
        df["payment_date_dt"] = pd.to_datetime(df["payment_date"], errors="coerce")
        cutoff = pd.Timestamp.now() - timedelta(days=config.STALE_PENDING_DAYS)

        pending_mask = df["payment_status"].str.lower() == "pending"
        stale_mask = df["payment_date_dt"] < cutoff
        combined = pending_mask & stale_mask

        stale_df = df[combined]
        affected_ids = stale_df["loan_id"].tolist()

        result = {
            "error_type": "STALE_PENDING",
            "affected_record_ids": affected_ids,
            "error_count": len(affected_ids),
            "severity": "Medium",
            "detail_df": stale_df,
        }
        self.errors.append(result)
        return result

    # ──────────────────────────────────────────────────────────────────
    #  Run All Checks
    # ──────────────────────────────────────────────────────────────────
    def run_all_checks(
        self,
        payments_df: pd.DataFrame,
        remittance_df: pd.DataFrame,
        collateral_df: pd.DataFrame,
    ) -> list[dict]:
        """Execute all 6 validation checks and return results."""
        self.errors = []  # reset

        print("    Check 1/6 — Duplicate transactions …")
        self.check_duplicates(payments_df)

        print("    Check 2/6 — Orphaned remittances …")
        self.check_orphaned_remittances(remittance_df, payments_df)

        print("    Check 3/6 — Reversed payment leakage …")
        self.check_reversed_leakage(payments_df, remittance_df)

        print("    Check 4/6 — Non-business day payments …")
        self.check_non_business_day(payments_df)

        print("    Check 5/6 — Collateral coupon outliers …")
        self.check_collateral_outliers(collateral_df)

        print("    Check 6/6 — Stale pending payments …")
        self.check_stale_pending(payments_df)

        return self.errors

    # ──────────────────────────────────────────────────────────────────
    #  Master Error Log
    # ──────────────────────────────────────────────────────────────────
    def compile_master_error_log(self, output_path: str | None = None) -> pd.DataFrame:
        """Compile all detected errors into a master CSV."""
        rows = []
        for err in self.errors:
            for rid in err["affected_record_ids"]:
                rows.append({
                    "error_type": err["error_type"],
                    "record_id": rid,
                    "severity": err["severity"],
                })

        master_df = pd.DataFrame(rows)

        if output_path is None:
            output_path = os.path.join(config.DATA_PROCESSED, "master_error_log.csv")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        master_df.to_csv(output_path, index=False)
        return master_df

    # ──────────────────────────────────────────────────────────────────
    #  Detection Accuracy vs Ground Truth
    # ──────────────────────────────────────────────────────────────────
    def evaluate_accuracy(
        self,
        ground_truth_df: pd.DataFrame,
        master_error_df: pd.DataFrame | None = None,
    ) -> dict:
        """Compute precision, recall, F1 against ground truth."""
        if master_error_df is None:
            master_error_df = self.compile_master_error_log()

        # Build sets of (error_type, record_id)
        gt_set = set()
        for _, row in ground_truth_df.iterrows():
            gt_set.add((row["error_type"], str(row["record_id"])))

        detected_set = set()
        for _, row in master_error_df.iterrows():
            detected_set.add((row["error_type"], str(row["record_id"])))

        true_positives = len(gt_set & detected_set)
        false_positives = len(detected_set - gt_set)
        false_negatives = len(gt_set - detected_set)

        precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
        recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        return {
            "true_positives": true_positives,
            "false_positives": false_positives,
            "false_negatives": false_negatives,
            "precision": round(precision * 100, 2),
            "recall": round(recall * 100, 2),
            "f1_score": round(f1 * 100, 2),
            "total_ground_truth": len(gt_set),
            "total_detected": len(detected_set),
        }

    def print_accuracy_report(self, accuracy: dict) -> None:
        """Print a formatted detection accuracy report."""
        print("\n  ┌─────────────────────────────────────────┐")
        print("  │     ERROR DETECTION ACCURACY REPORT     │")
        print("  ├─────────────────────────────────────────┤")
        print(f"  │ Ground Truth Errors : {accuracy['total_ground_truth']:>6}            │")
        print(f"  │ Detected Errors     : {accuracy['total_detected']:>6}            │")
        print(f"  │ True Positives      : {accuracy['true_positives']:>6}            │")
        print(f"  │ False Positives     : {accuracy['false_positives']:>6}            │")
        print(f"  │ False Negatives     : {accuracy['false_negatives']:>6}            │")
        print(f"  │ Precision           : {accuracy['precision']:>6.1f}%           │")
        print(f"  │ Recall              : {accuracy['recall']:>6.1f}%           │")
        print(f"  │ F1 Score            : {accuracy['f1_score']:>6.1f}%           │")
        print("  └─────────────────────────────────────────┘")
