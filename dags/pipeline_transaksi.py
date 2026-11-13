from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

BASE_DIR = Path("/opt/airflow/dags")
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_FILE = DATA_DIR / "transaksi.csv"
CLEAN_FILE = OUTPUT_DIR / "transaksi_clean.csv"
VALID_FILE = OUTPUT_DIR / "transaksi_valid.csv"
REJECT_FILE = OUTPUT_DIR / "transaksi_reject.csv"
DB_FILE = OUTPUT_DIR / "warehouse.db"
REPORT_FILE = OUTPUT_DIR / "ringkasan.txt"


@dag(
    dag_id="pipeline_transaksi_harian",
    description="Pipeline otomatis data transaksi CSV",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    default_args={
        "owner": "praktikum",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["praktikum", "pipeline", "transaksi"],
)
def pipeline_transaksi_harian():

    @task
    def extract():
        if not INPUT_FILE.exists():
            raise FileNotFoundError(f"File tidak ditemukan: {INPUT_FILE}")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with INPUT_FILE.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        if not rows:
            raise ValueError("File input tidak memiliki data.")

        print(f"Extract selesai: {len(rows)} record")
        return rows

    @task
    def transform(rows: list[dict]):
        cleaned = []

        for row in rows:
            item = {k: (v or "").strip() for k, v in row.items()}
            item["transaction_id"] = item["transaction_id"].upper()
            item["customer_id"] = item["customer_id"].upper()
            item["category"] = item["category"].title()
            item["quantity"] = int(item["quantity"])
            item["amount"] = float(item["amount"])
            cleaned.append(item)

        with CLEAN_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cleaned[0].keys())
            writer.writeheader()
            writer.writerows(cleaned)

        print(f"Transform selesai: {len(cleaned)} record")
        return cleaned

    @task
    def validate(rows: list[dict]):
        valid, rejected = [], []

        for row in rows:
            reasons = []

            if not row["transaction_id"]:
                reasons.append("transaction_id kosong")
            if not row["customer_id"]:
                reasons.append("customer_id kosong")
            if row["quantity"] <= 0:
                reasons.append("quantity harus lebih dari 0")
            if row["amount"] < 0:
                reasons.append("amount tidak boleh negatif")

            try:
                pendulum.parse(row["transaction_date"])
            except Exception:
                reasons.append("transaction_date tidak valid")

            if reasons:
                rejected.append({**row, "reject_reason": "; ".join(reasons)})
            else:
                valid.append(row)

        if not valid:
            raise ValueError("Tidak ada record valid untuk dimuat.")

        with VALID_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=valid[0].keys())
            writer.writeheader()
            writer.writerows(valid)

        reject_fields = (
            list(rejected[0].keys())
            if rejected else list(rows[0].keys()) + ["reject_reason"]
        )
        with REJECT_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=reject_fields)
            writer.writeheader()
            writer.writerows(rejected)

        print(f"Valid: {len(valid)}; ditolak: {len(rejected)}")
        return {
            "valid_file": str(VALID_FILE),
            "valid_count": len(valid),
            "rejected_count": len(rejected),
        }

    @task
    def load(validation_result: dict):
        valid_path = Path(validation_result["valid_file"])
        if not valid_path.exists():
            raise FileNotFoundError(valid_path)

        with valid_path.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transaksi (
                    transaction_id TEXT PRIMARY KEY,
                    transaction_date TEXT NOT NULL,
                    customer_id TEXT NOT NULL,
                    product TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    category TEXT NOT NULL
                )
            """)
            conn.executemany("""
                INSERT OR REPLACE INTO transaksi
                (transaction_id, transaction_date, customer_id,
                 product, quantity, amount, category)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    r["transaction_id"], r["transaction_date"],
                    r["customer_id"], r["product"], int(r["quantity"]),
                    float(r["amount"]), r["category"]
                )
                for r in rows
            ])
            conn.commit()

        print(f"Load selesai: {len(rows)} record")
        return len(rows)

    @task
    def report(load_count: int, validation_result: dict):
        with sqlite3.connect(DB_FILE) as conn:
            total_rows = conn.execute(
                "SELECT COUNT(*) FROM transaksi"
            ).fetchone()[0]
            total_amount = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM transaksi"
            ).fetchone()[0]
            categories = conn.execute("""
                SELECT category, COUNT(*), SUM(amount)
                FROM transaksi GROUP BY category ORDER BY category
            """).fetchall()

        lines = [
            "LAPORAN PIPELINE TRANSAKSI",
            "==========================",
            f"Record pada task load: {load_count}",
            f"Record valid batch: {validation_result['valid_count']}",
            f"Record ditolak batch: {validation_result['rejected_count']}",
            f"Total record database: {total_rows}",
            f"Total nilai transaksi: {total_amount:,.2f}",
            "",
            "RINGKASAN PER KATEGORI",
        ]
        for category, count, amount in categories:
            lines.append(f"{category}: {count} transaksi, nilai {amount:,.2f}")

        REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")
        print("\n".join(lines))
        return str(REPORT_FILE)

    extracted = extract()
    transformed = transform(extracted)
    validation_result = validate(transformed)
    loaded = load(validation_result)
    report(loaded, validation_result)


pipeline_transaksi_harian()
