"""Generate a SQLite source database that simulates client Oracle query results."""

from __future__ import annotations

import logging
import random
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from faker import Faker
except ModuleNotFoundError:
    class Faker:  # type: ignore[no-redef]
        """Small deterministic fallback used only when Faker is not installed locally."""

        first_names = ["Ada", "Grace", "Katherine", "Alan", "Mary", "Dorothy", "Joan", "Claude"]
        last_names = ["Lovelace", "Hopper", "Johnson", "Turing", "Jackson", "Vaughan", "Clarke", "Shannon"]
        words = ["reviewed", "customer", "activity", "matched", "policy", "threshold", "case", "closed"]

        def __init__(self, locale: str = "en_GB") -> None:
            self.locale = locale

        @staticmethod
        def seed(value: int) -> None:
            random.seed(value)

        def name(self) -> str:
            return f"{random.choice(self.first_names)} {random.choice(self.last_names)}"

        def date_between(self, start_date: str | date, end_date: str | date) -> date:
            if isinstance(start_date, str):
                days_back = 365
                if start_date.endswith("y"):
                    days_back = int(start_date.strip("-y")) * 365
                elif start_date.endswith("M"):
                    days_back = int(start_date.strip("-M")) * 31
                start = date.today() - timedelta(days=days_back)
            else:
                start = start_date
            end = date.today() if end_date == "today" else end_date
            assert isinstance(end, date)
            return start + timedelta(days=random.randint(0, max((end - start).days, 0)))

        def bothify(self, text: str) -> str:
            output = []
            for char in text:
                if char == "?":
                    output.append(chr(random.randint(65, 90)))
                elif char == "#":
                    output.append(str(random.randint(0, 9)))
                else:
                    output.append(char)
            return "".join(output)

        def sentence(self, nb_words: int = 8) -> str:
            return " ".join(random.choice(self.words) for _ in range(nb_words)).capitalize() + "."

LOGGER = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent
SQLITE_PATH = ROOT / "financial_data.sqlite"

ACCOUNT_TYPES = ["credit_card", "current", "savings", "loan"]
CURRENCIES = ["GBP", "USD", "EUR"]
TRANSACTION_TYPES = ["purchase", "refund", "cash_withdrawal", "payment", "transfer"]
MERCHANT_CATEGORIES = [
    "groceries",
    "travel",
    "utilities",
    "entertainment",
    "restaurants",
    "healthcare",
    "fuel",
    "retail",
]
FLAG_TYPES = ["aml_review", "late_payment", "fraud_watch", "kyc_refresh", "credit_review"]
SEVERITIES = ["low", "medium", "high", "critical"]


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(path)


def _drop_tables(conn: sqlite3.Connection) -> None:
    for table in ("customer_accounts", "transactions", "risk_flags", "monthly_summaries"):
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE customer_accounts (
            account_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            account_type TEXT NOT NULL,
            open_date TEXT NOT NULL,
            status TEXT NOT NULL,
            credit_limit REAL NOT NULL,
            balance REAL NOT NULL,
            currency TEXT NOT NULL,
            branch_code TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE transactions (
            transaction_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            transaction_date TEXT NOT NULL,
            amount REAL NOT NULL,
            transaction_type TEXT NOT NULL,
            merchant_category TEXT NOT NULL,
            status TEXT NOT NULL,
            reference_code TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE risk_flags (
            flag_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            flag_type TEXT NOT NULL,
            flag_date TEXT NOT NULL,
            severity TEXT NOT NULL,
            resolved INTEGER NOT NULL,
            resolved_date TEXT,
            notes TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE monthly_summaries (
            summary_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL,
            month_year TEXT NOT NULL,
            total_credits REAL NOT NULL,
            total_debits REAL NOT NULL,
            avg_balance REAL NOT NULL,
            transaction_count INTEGER NOT NULL,
            flagged_count INTEGER NOT NULL
        )
        """
    )


def _insert_many(conn: sqlite3.Connection, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ", ".join("?" for _ in columns)
    column_sql = ", ".join(columns)
    values = [tuple(row[column] for column in columns) for row in rows]
    conn.executemany(f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})", values)


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def generate(path: Path = SQLITE_PATH) -> None:
    """Create a complete fake source database at ``path``."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    faker = Faker("en_GB")
    Faker.seed(42)
    random.seed(42)
    run_marker = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    customers = [
        {
            "customer_id": f"CUST{i:05d}",
            "customer_name": faker.name(),
        }
        for i in range(1, 351)
    ]

    account_rows: list[dict[str, Any]] = []
    for i in range(1, 501):
        customer = random.choice(customers)
        account_type = random.choice(ACCOUNT_TYPES)
        credit_limit = round(random.uniform(500, 25000), 2)
        balance = round(random.uniform(-1000, credit_limit * 0.95), 2)
        if i <= 15:
            balance = round(balance + random.uniform(10, 250), 2)
        account_rows.append(
            {
                "account_id": f"ACC{i:06d}",
                "customer_id": customer["customer_id"],
                "customer_name": customer["customer_name"],
                "account_type": account_type,
                "open_date": faker.date_between(start_date="-6y", end_date="today").isoformat(),
                "status": random.choices(
                    ["active", "closed", "suspended"], weights=[0.86, 0.09, 0.05], k=1
                )[0],
                "credit_limit": credit_limit,
                "balance": balance,
                "currency": random.choices(CURRENCIES, weights=[0.83, 0.1, 0.07], k=1)[0],
                "branch_code": f"BR{random.randint(1, 75):03d}",
            }
        )

    transaction_rows: list[dict[str, Any]] = []
    for i in range(1, 5001):
        account = random.choice(account_rows)
        amount = round(random.uniform(3, 5000), 2)
        if random.random() < 0.65:
            amount = -amount
        reference = faker.bothify(text="REF-????-########").upper()
        if i <= 20:
            reference = f"{reference}-{run_marker}"
        transaction_rows.append(
            {
                "transaction_id": f"TXN{i:08d}",
                "account_id": account["account_id"],
                "transaction_date": faker.date_between(start_date="-18M", end_date="today").isoformat(),
                "amount": amount,
                "transaction_type": random.choice(TRANSACTION_TYPES),
                "merchant_category": random.choice(MERCHANT_CATEGORIES),
                "status": random.choices(["posted", "pending", "reversed"], weights=[0.91, 0.06, 0.03], k=1)[0],
                "reference_code": reference,
            }
        )

    risk_rows: list[dict[str, Any]] = []
    for i in range(1, 301):
        customer = random.choice(customers)
        resolved = random.random() < 0.68
        flag_date = faker.date_between(start_date="-2y", end_date="today")
        resolved_date = (
            faker.date_between(start_date=flag_date, end_date="today").isoformat() if resolved else None
        )
        note_suffix = f" run {run_marker}" if i <= 10 else ""
        risk_rows.append(
            {
                "flag_id": f"FLAG{i:06d}",
                "customer_id": customer["customer_id"],
                "flag_type": random.choice(FLAG_TYPES),
                "flag_date": flag_date.isoformat(),
                "severity": random.choices(SEVERITIES, weights=[0.45, 0.32, 0.18, 0.05], k=1)[0],
                "resolved": int(resolved),
                "resolved_date": resolved_date,
                "notes": f"{faker.sentence(nb_words=8)}{note_suffix}",
            }
        )

    summary_rows: list[dict[str, Any]] = []
    today = date.today()
    candidate_months = [_month_start(today - timedelta(days=31 * offset)) for offset in range(0, 18)]
    selected_pairs: set[tuple[str, str]] = set()
    while len(summary_rows) < 200:
        account = random.choice(account_rows)
        month = random.choice(candidate_months)
        pair = (account["account_id"], month.strftime("%Y-%m"))
        if pair in selected_pairs:
            continue
        selected_pairs.add(pair)
        total_credits = round(random.uniform(200, 12000), 2)
        total_debits = round(random.uniform(200, 15000), 2)
        if len(summary_rows) < 5:
            total_debits = round(total_debits + random.uniform(5, 500), 2)
        summary_rows.append(
            {
                "summary_id": f"SUM{len(summary_rows) + 1:07d}",
                "account_id": account["account_id"],
                "month_year": month.strftime("%Y-%m"),
                "total_credits": total_credits,
                "total_debits": total_debits,
                "avg_balance": round(random.uniform(-500, account["credit_limit"]), 2),
                "transaction_count": random.randint(1, 120),
                "flagged_count": random.randint(0, 5),
            }
        )

    with _connect(path) as conn:
        _drop_tables(conn)
        _create_tables(conn)
        _insert_many(conn, "customer_accounts", account_rows)
        _insert_many(conn, "transactions", transaction_rows)
        _insert_many(conn, "risk_flags", risk_rows)
        _insert_many(conn, "monthly_summaries", summary_rows)
        conn.commit()

    LOGGER.info("Generated SQLite source database at %s", path)


if __name__ == "__main__":
    generate()
