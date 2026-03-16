# Loan Payment & Remittance Analysis System

A production-ready Python system that automates **reconciliation**, **discrepancy detection**, and **compliance reporting** for loan and security payments in a trust administration context.

---

## Features

| Capability | Description |
|---|---|
| **Synthetic Data Generation** | 3 datasets (2,000+ rows each) with 150-170 injected errors |
| **Data Cleaning Pipeline** | Config-driven: date normalization, type casting, dedup, validation |
| **Reconciliation Engine** | Exact + fuzzy date matching, variance analysis, discrepancy categorization |
| **Collateral Compliance** | Coupon recalculation with dual tolerance (2% standard / 0% trust) |
| **Error Detection** | 6 automated checks with precision/recall/F1 scoring vs ground truth |
| **Excel Reporting** | 3 multi-sheet reports with conditional formatting, charts, auto-filters |

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the full pipeline
python main.py
```

That's it. The pipeline will generate synthetic data, clean it, reconcile, analyze collateral, scan for errors, and produce all reports.

---

## Project Structure

```
loan_remittance_analysis/
├── data/
│   ├── raw/                  # Generated CSV datasets
│   ├── processed/            # Cleaned data + variance reports
│   └── reference/            # Bond docs, amortization schedules
├── scripts/
│   ├── ingest.py             # Data generation & loading
│   ├── clean.py              # Reusable cleaning pipeline
│   ├── reconcile.py          # Reconciliation engine
│   ├── collateral.py         # Collateral payment calculator
│   ├── error_detection.py    # Transaction error scanner
│   └── reporting.py          # Excel report builder
├── templates/
│   └── pivot_template.xlsx   # Reusable pivot template
├── tests/
│   ├── test_clean.py
│   ├── test_reconcile.py
│   └── test_collateral.py
├── output/reports/            # Generated Excel reports
├── config.py                  # All constants & thresholds
├── main.py                    # Pipeline orchestrator
└── requirements.txt
```

---

## Generating Synthetic Data

Data is generated automatically when you run `python main.py`. To generate data standalone:

```bash
python -m scripts.ingest
```

This creates:
- `data/raw/loan_payments.csv` — 2,500 loan payment records
- `data/raw/remittance_records.csv` — 2,200 remittance records
- `data/raw/collateral_schedule.csv` — 2,000 collateral records
- `data/raw/ground_truth_errors.csv` — 150-170 injected errors for validation

---

## Running the Full Pipeline

```bash
python main.py
```

The pipeline executes 6 stages:
1. **Ingest** — Generate/load synthetic datasets
2. **Clean** — Standardize dates, cast types, remove duplicates, validate
3. **Reconcile** — Match payments to remittances (exact + fuzzy ±2 business days)
4. **Collateral** — Recalculate expected coupons, evaluate compliance
5. **Error Scan** — Run 6 validation checks, compute accuracy metrics
6. **Report** — Generate 3 Excel reports + pivot template

---

## Running Unit Tests

```bash
pytest tests/ -v
```

Tests cover:
- `test_clean.py` — Date normalization, duplicate removal, NaN flagging, ID/status validation
- `test_reconcile.py` — MATCHED/UNMATCHED/PARTIAL/REVERSED logic with mock DataFrames
- `test_collateral.py` — Coupon calculation for 4 frequencies, trust tolerance enforcement

---

## Output Reports

| Report | File | Description |
|---|---|---|
| Reconciliation | `output/reports/reconciliation_report.xlsx` | 4 sheets: Summary, Discrepancies, By Pool, By Trust Account |
| Collateral | `output/reports/collateral_report.xlsx` | 3 sheets: Compliance Summary, Detail, Chart (top 20 outliers) |
| Error Log | `output/reports/error_log_report.xlsx` | 2 sheets: Master Error Log, Error Summary with accuracy metrics |
| Pivot Template | `templates/pivot_template.xlsx` | Pre-configured for pool-level monthly analysis |

---

## Configuration

All thresholds and constants are in `config.py`:

| Parameter | Default | Description |
|---|---|---|
| `TOLERANCE_AMOUNT` | $0.01 | Variance tolerance for reconciliation matching |
| `LARGE_VARIANCE_THRESHOLD` | $500 | Threshold for High severity classification |
| `COLLATERAL_TOLERANCE_PCT` | 2% | Standard coupon deviation tolerance |
| `TRUST_REQUIREMENT_TOLERANCE_PCT` | 0% | Strict tolerance for trust-required securities |
| `STALE_PENDING_DAYS` | 30 | Days before a Pending payment is flagged stale |
