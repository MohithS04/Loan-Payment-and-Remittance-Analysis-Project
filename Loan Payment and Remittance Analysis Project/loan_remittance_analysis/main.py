#!/usr/bin/env python3
"""
main.py — Pipeline Orchestrator

Runs the full Loan Payment & Remittance Analysis pipeline end-to-end:
  1. Ingest / generate synthetic data
  2. Clean all datasets
  3. Reconcile payments ↔ remittances
  4. Calculate collateral compliance
  5. Scan for transaction errors
  6. Generate Excel reports
  7. Print pipeline summary
"""

import os
import sys
import time

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from scripts.ingest import generate_and_save, load_raw_data
from scripts.clean import clean_dataframe, simulate_manual_baseline, compute_time_saved
from scripts.reconcile import ReconciliationEngine
from scripts.collateral import CollateralCalculator
from scripts.error_detection import ErrorScanner
from scripts.reporting import (
    generate_reconciliation_report,
    generate_collateral_report,
    generate_error_log_report,
    generate_pivot_template,
)


def run_pipeline():
    """Execute the full analysis pipeline."""

    print("=" * 60)
    print("  LOAN PAYMENT & REMITTANCE ANALYSIS SYSTEM")
    print("=" * 60)
    pipeline_start = time.time()

    # ══════════════════════════════════════════════════════════════
    # STEP 1 — Ingest / Generate Data
    # ══════════════════════════════════════════════════════════════
    print("\n[1/6] Generating synthetic datasets …")
    data = generate_and_save()
    payments_raw = data["loan_payments"]
    remittance_raw = data["remittance_records"]
    collateral_raw = data["collateral_schedule"]
    ground_truth = data["ground_truth_errors"]

    # ══════════════════════════════════════════════════════════════
    # STEP 2 — Clean All Datasets
    # ══════════════════════════════════════════════════════════════
    print("\n[2/6] Running data cleaning pipeline …")
    exceptions_dir = config.DATA_PROCESSED

    # Simulate manual baseline
    manual_time = (
        simulate_manual_baseline(payments_raw) +
        simulate_manual_baseline(remittance_raw) +
        simulate_manual_baseline(collateral_raw)
    )

    clean_start = time.time()
    payments_clean, pay_report = clean_dataframe(
        payments_raw, "loan_payments", exceptions_dir
    )
    remittance_clean, rem_report = clean_dataframe(
        remittance_raw, "remittance_records", exceptions_dir
    )
    collateral_clean, col_report = clean_dataframe(
        collateral_raw, "collateral_schedule", exceptions_dir
    )
    clean_elapsed = time.time() - clean_start

    total_clean_report = {
        "cleaning_time_seconds": clean_elapsed,
    }
    time_saved_pct = compute_time_saved(total_clean_report, manual_time)

    print(f"  ✓ Cleaned {pay_report['dataset']}: {pay_report['rows_before']} → {pay_report['rows_after']} rows")
    print(f"  ✓ Cleaned {rem_report['dataset']}: {rem_report['rows_before']} → {rem_report['rows_after']} rows")
    print(f"  ✓ Cleaned {col_report['dataset']}: {col_report['rows_before']} → {col_report['rows_after']} rows")
    print(f"  ✓ Time saved vs manual: {time_saved_pct:.1f}%")

    # Save cleaned data
    payments_clean.to_csv(os.path.join(config.DATA_PROCESSED, "loan_payments_clean.csv"), index=False)
    remittance_clean.to_csv(os.path.join(config.DATA_PROCESSED, "remittance_records_clean.csv"), index=False)
    collateral_clean.to_csv(os.path.join(config.DATA_PROCESSED, "collateral_schedule_clean.csv"), index=False)

    # ══════════════════════════════════════════════════════════════
    # STEP 3 — Reconciliation
    # ══════════════════════════════════════════════════════════════
    print("\n[3/6] Running reconciliation engine …")
    engine = ReconciliationEngine()
    recon_df = engine.match_payments_to_remittances(payments_clean, remittance_clean)
    disc_df = engine.detect_discrepancies(recon_df)
    recon_summary = engine.generate_reconciliation_summary(recon_df)

    print(f"  ✓ Matched: {recon_summary['matched']}  |  Unmatched: {recon_summary['unmatched']}  |  Partial: {recon_summary['partial']}")
    print(f"  ✓ Total variance: ${recon_summary['total_dollar_variance']:,.2f}")
    print(f"  ✓ Discrepancy rate: {recon_summary['discrepancy_rate_pct']:.1f}%")

    # Save reconciliation output
    recon_df.to_csv(os.path.join(config.DATA_PROCESSED, "reconciliation_output.csv"), index=False)
    disc_df.to_csv(os.path.join(config.DATA_PROCESSED, "discrepancies.csv"), index=False)

    # ══════════════════════════════════════════════════════════════
    # STEP 4 — Collateral Compliance
    # ══════════════════════════════════════════════════════════════
    print("\n[4/6] Running collateral calculator …")
    calc = CollateralCalculator()
    variance_report = calc.generate_variance_report(df=collateral_clean)
    compliance_summary = calc.get_compliance_summary(variance_report)

    totals = compliance_summary.get("totals", {})
    compliant = totals.get("COMPLIANT", 0)
    non_compliant = totals.get("NON-COMPLIANT", 0)
    review = totals.get("REQUIRES_REVIEW", 0)
    print(f"  ✓ Compliant: {compliant}  |  Non-Compliant: {non_compliant}  |  Requires Review: {review}")

    # ══════════════════════════════════════════════════════════════
    # STEP 5 — Error Detection
    # ══════════════════════════════════════════════════════════════
    print("\n[5/6] Running error scanner …")
    scanner = ErrorScanner()
    scan_results = scanner.run_all_checks(payments_clean, remittance_clean, collateral_clean)
    master_errors = scanner.compile_master_error_log()

    total_errors_detected = len(master_errors)
    print(f"  ✓ Total errors detected: {total_errors_detected}")

    for result in scan_results:
        print(f"    • {result['error_type']}: {result['error_count']} ({result['severity']})")

    # Accuracy evaluation
    accuracy = scanner.evaluate_accuracy(ground_truth, master_errors)
    scanner.print_accuracy_report(accuracy)

    # ══════════════════════════════════════════════════════════════
    # STEP 6 — Generate Reports
    # ══════════════════════════════════════════════════════════════
    print("\n[6/6] Generating Excel reports …")

    report1 = generate_reconciliation_report(recon_df, disc_df, recon_summary)
    print(f"  ✓ {os.path.basename(report1)}")

    report2 = generate_collateral_report(variance_report, compliance_summary)
    print(f"  ✓ {os.path.basename(report2)}")

    report3 = generate_error_log_report(master_errors, accuracy)
    print(f"  ✓ {os.path.basename(report3)}")

    pivot_path = generate_pivot_template()
    print(f"  ✓ {os.path.basename(pivot_path)}")

    # ══════════════════════════════════════════════════════════════
    # PIPELINE SUMMARY
    # ══════════════════════════════════════════════════════════════
    pipeline_elapsed = time.time() - pipeline_start
    total_records = len(payments_raw) + len(remittance_raw) + len(collateral_raw)

    matched_pct = (recon_summary["matched"] / max(recon_summary["total_payments_processed"], 1)) * 100

    # Pre-compute display values
    matched_val = recon_summary["matched"]
    variance_val = recon_summary["total_dollar_variance"]
    precision_val = accuracy["precision"]
    gt_count = len(ground_truth)

    s_records = str(total_records)
    s_time = f"{time_saved_pct:.1f}%"
    s_matched = f"{matched_val}  ({matched_pct:.1f}%)"
    s_variance = f"${variance_val:,.2f}"
    s_errors = f"{total_errors_detected} / {gt_count} injected"
    s_precision = f"{precision_val:.1f}%"
    s_duration = f"{pipeline_elapsed:.1f}s"

    print("\n")
    print("  ┌──────────────────────────────────────────────┐")
    print("  │        PIPELINE EXECUTION SUMMARY            │")
    print("  ├──────────────────────────────────────────────┤")
    print(f"  │ Records Processed     : {s_records:<21}│")
    print(f"  │ Cleaning Time Saved   : {s_time:<21}│")
    print(f"  │ Matched Payments      : {s_matched:<21}│")
    print(f"  │ Total Variance $      : {s_variance:<21}│")
    print(f"  │ Errors Detected       : {s_errors:<21}│")
    print(f"  │ Detection Precision   : {s_precision:<21}│")
    print(f"  │ Reports Generated     : {'3 + pivot template':<21}│")
    print(f"  │ Pipeline Duration     : {s_duration:<21}│")
    print("  └──────────────────────────────────────────────┘")
    print()


if __name__ == "__main__":
    run_pipeline()
