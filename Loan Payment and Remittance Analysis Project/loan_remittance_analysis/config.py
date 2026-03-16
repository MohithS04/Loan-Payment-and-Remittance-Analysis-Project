"""
Configuration constants for the Loan Payment & Remittance Analysis System.
"""
import os

# ── Paths ───────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_RAW = os.path.join(BASE_DIR, "data", "raw")
DATA_PROCESSED = os.path.join(BASE_DIR, "data", "processed")
DATA_REFERENCE = os.path.join(BASE_DIR, "data", "reference")
OUTPUT_REPORTS = os.path.join(BASE_DIR, "output", "reports")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

# ── Reconciliation Thresholds ───────────────────────────────────────
TOLERANCE_AMOUNT = 0.01
LARGE_VARIANCE_THRESHOLD = 500.00

# ── Validation Rules ────────────────────────────────────────────────
LOAN_ID_PATTERN = r'^LN-\d{6}$'
ALLOWED_PAYMENT_STATUSES = ['Posted', 'Pending', 'Reversed']
STALE_PENDING_DAYS = 30

# ── Collateral / Coupon ────────────────────────────────────────────
PAYMENT_FREQUENCY_MAP = {
    'Monthly': 12,
    'Quarterly': 4,
    'Semi-Annual': 2,
    'Annual': 1,
}
COLLATERAL_TOLERANCE_PCT = 0.02   # 2%
TRUST_REQUIREMENT_TOLERANCE_PCT = 0.0  # exact match for bond docs

# ── Column Mappings ─────────────────────────────────────────────────
AMOUNT_COLUMNS = [
    'scheduled_payment_amount',
    'actual_payment_amount',
    'remittance_amount',
    'face_value',
    'coupon_rate',
    'calculated_payment',
]

DATE_COLUMNS = [
    'payment_date',
    'remittance_date',
    'next_payment_date',
]

STRING_COLUMNS = [
    'borrower_name',
    'payment_type',
    'payment_status',
    'servicer_name',
    'remittance_status',
    'collateral_type',
    'payment_frequency',
]

# ── US Federal Holidays (2024-2025) ─────────────────────────────────
US_FEDERAL_HOLIDAYS = [
    # 2024
    "2024-01-01", "2024-01-15", "2024-02-19", "2024-05-27",
    "2024-06-19", "2024-07-04", "2024-09-02", "2024-10-14",
    "2024-11-11", "2024-11-28", "2024-12-25",
    # 2025
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26",
    "2025-06-19", "2025-07-04", "2025-09-01", "2025-10-13",
    "2025-11-11", "2025-11-27", "2025-12-25",
    # 2026
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25",
    "2026-06-19", "2026-07-03", "2026-09-07", "2026-10-12",
    "2026-11-11", "2026-11-26", "2026-12-25",
]

# ── Data Generation ─────────────────────────────────────────────────
NUM_LOAN_PAYMENTS = 2500
NUM_REMITTANCE_RECORDS = 2200
NUM_COLLATERAL_RECORDS = 2000
NUM_INJECTED_ERRORS_RANGE = (150, 170)
RANDOM_SEED = 42
