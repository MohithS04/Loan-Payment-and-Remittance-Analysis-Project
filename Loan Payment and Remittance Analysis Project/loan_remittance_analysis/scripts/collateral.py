"""
collateral.py — Collateral Payment Calculation Logic

Recalculates expected coupon payments, compares against reported values,
applies tolerance rules (2% standard, 0% for trust-required), and
outputs a compliance variance report.
"""

import os
import sys

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


class CollateralCalculator:
    """Calculate and validate collateral coupon payments."""

    def __init__(
        self,
        frequency_map: dict = None,
        tolerance_pct: float = config.COLLATERAL_TOLERANCE_PCT,
        trust_tolerance_pct: float = config.TRUST_REQUIREMENT_TOLERANCE_PCT,
    ):
        self.frequency_map = frequency_map or config.PAYMENT_FREQUENCY_MAP
        self.tolerance_pct = tolerance_pct
        self.trust_tolerance_pct = trust_tolerance_pct

    def load_collateral(self, filepath: str | None = None) -> pd.DataFrame:
        """Load collateral schedule CSV."""
        if filepath is None:
            filepath = os.path.join(config.DATA_RAW, "collateral_schedule.csv")
        df = pd.read_csv(filepath)
        df["face_value"] = pd.to_numeric(df["face_value"], errors="coerce")
        df["coupon_rate"] = pd.to_numeric(df["coupon_rate"], errors="coerce")
        df["calculated_payment"] = pd.to_numeric(df["calculated_payment"], errors="coerce")
        return df

    def calculate_expected_payments(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Recalculate expected payment per security:
            expected = face_value * (coupon_rate / payment_periods_per_year)
        """
        df = df.copy()
        df["payment_periods"] = df["payment_frequency"].map(self.frequency_map)
        df["expected_payment"] = np.round(
            df["face_value"] * (df["coupon_rate"] / df["payment_periods"]),
            2,
        )
        return df

    def evaluate_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compare calculated_payment vs expected_payment.
        Apply tolerance rules:
          - Standard: ±2% tolerance
          - Trust-required: 0% tolerance (exact match)
        """
        df = df.copy()

        # Variance percentage
        df["variance_amt"] = df["calculated_payment"] - df["expected_payment"]
        df["variance_pct"] = np.where(
            df["expected_payment"] != 0,
            np.abs(df["variance_amt"]) / df["expected_payment"],
            0.0,
        )
        df["variance_pct"] = np.round(df["variance_pct"], 6)

        # Compliance status
        df["compliance_status"] = "COMPLIANT"

        # Standard tolerance check
        non_trust = ~df["trust_requirement_flag"].astype(bool)
        df.loc[
            non_trust & (df["variance_pct"] > self.tolerance_pct),
            "compliance_status",
        ] = "NON-COMPLIANT"

        # Trust requirement: exact match (0% tolerance)
        trust = df["trust_requirement_flag"].astype(bool)
        df.loc[
            trust & (df["variance_pct"] > self.trust_tolerance_pct),
            "compliance_status",
        ] = "REQUIRES_REVIEW"

        # Also mark trust rows with large variance as NON-COMPLIANT
        df.loc[
            trust & (df["variance_pct"] > self.tolerance_pct),
            "compliance_status",
        ] = "NON-COMPLIANT"

        return df

    def generate_variance_report(
        self,
        df: pd.DataFrame | None = None,
        filepath: str | None = None,
        output_path: str | None = None,
    ) -> pd.DataFrame:
        """
        Full pipeline: load → calculate → evaluate → save report.
        Returns the variance report DataFrame.
        """
        if df is None:
            df = self.load_collateral(filepath)

        df = self.calculate_expected_payments(df)
        df = self.evaluate_compliance(df)

        # Build report
        report_cols = [
            "security_id", "loan_id", "collateral_type", "face_value",
            "coupon_rate", "payment_frequency", "expected_payment",
            "calculated_payment", "variance_amt", "variance_pct",
            "trust_requirement_flag", "compliance_status",
        ]
        report = df[[c for c in report_cols if c in df.columns]].copy()
        report = report.rename(columns={"calculated_payment": "actual_payment"})

        # Save
        if output_path is None:
            output_path = os.path.join(config.DATA_PROCESSED, "collateral_variance_report.csv")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        report.to_csv(output_path, index=False)

        return report

    def get_compliance_summary(self, report: pd.DataFrame) -> dict:
        """Summary counts of compliance status by collateral type."""
        summary = (
            report
            .groupby(["collateral_type", "compliance_status"])
            .size()
            .unstack(fill_value=0)
            .to_dict(orient="index")
        )
        total = report["compliance_status"].value_counts().to_dict()
        return {"by_collateral_type": summary, "totals": total}
