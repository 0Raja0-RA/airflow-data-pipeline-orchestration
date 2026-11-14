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
VALID_FILE = OUTPUT_DIR / "transaksi_valid_mandiri.csv"
REJECT_FILE = OUTPUT_DIR / "transaksi_reject_mandiri.csv"
DB_FILE = OUTPUT_DIR / "warehouse.db"
REPORT_FILE = OUTPUT_DIR / "laporan_penjualan_eksekutif.txt"


@dag(
    dag_id="pipeline_pengolahan_penjualan",
    description="Pipeline Otomatis Pengolahan dan Analisis Data Penjualan (Tugas Mandiri)",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    default_args={
        "owner": "raja_rahman",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["praktikum_mandiri", "penjualan", "etl", "analisis_paralel"],
)
def pipeline_pengolahan_penjualan():

    @task
    def ekstrak_data():
        if not INPUT_FILE.exists():
            raise FileNotFoundError(f"File data tidak ditemukan: {INPUT_FILE}")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with INPUT_FILE.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        if not rows:
            raise ValueError("File transaksi kosong.")

        print(f"Ekstraksi selesai: {len(rows)} data transaksi terbaca.")
        return rows

    @task
    def transformasi_data(rows: list[dict]):
        cleaned = []
        for r in rows:
            item = {k: (v or "").strip() for k, v in r.items()}
            item["transaction_id"] = item["transaction_id"].upper()
            item["customer_id"] = item["customer_id"].upper()
            item["category"] = item["category"].title()
            item["product"] = item["product"].strip()
            item["quantity"] = int(item["quantity"])
            item["amount"] = float(item["amount"])
            cleaned.append(item)

        print(f"Transformasi selesai: {len(cleaned)} data dinormalisasi.")
        return cleaned

    @task
    def validasi_aturan(rows: list[dict]):
        valid, rejected = [], []

        for row in rows:
            issues = []
            if not row["transaction_id"]:
                issues.append("transaction_id kosong")
            if not row["customer_id"]:
                issues.append("customer_id kosong")
            if row["quantity"] <= 0:
                issues.append("quantity harus > 0")
            if row["amount"] < 0:
                issues.append("amount tidak boleh bernilai negatif")

            try:
                pendulum.parse(row["transaction_date"])
            except Exception:
                issues.append("format tanggal tidak valid")

            if issues:
                rejected.append({**row, "alasan_reject": "; ".join(issues)})
            else:
                valid.append(row)

        with VALID_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=valid[0].keys())
            writer.writeheader()
            writer.writerows(valid)

        reject_fields = list(rejected[0].keys()) if rejected else list(rows[0].keys()) + ["alasan_reject"]
        with REJECT_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=reject_fields)
            writer.writeheader()
            writer.writerows(rejected)

        print(f"Validasi data: {len(valid)} valid, {len(rejected)} ditolak.")
        return {
            "valid_rows": valid,
            "total_valid": len(valid),
            "total_reject": len(rejected),
        }

    @task
    def analisis_kategori(validation_res: dict):
        valid_rows = validation_res["valid_rows"]
        kategori_summary = {}
        for r in valid_rows:
            cat = r["category"]
            if cat not in kategori_summary:
                kategori_summary[cat] = {"total_omset": 0.0, "total_qty": 0}
            kategori_summary[cat]["total_omset"] += r["amount"]
            kategori_summary[cat]["total_qty"] += r["quantity"]

        print("Analisis Kategori selesai:", kategori_summary)
        return kategori_summary

    @task
    def analisis_pelanggan(validation_res: dict):
        valid_rows = validation_res["valid_rows"]
        pelanggan_summary = {}
        for r in valid_rows:
            cust = r["customer_id"]
            pelanggan_summary[cust] = pelanggan_summary.get(cust, 0.0) + r["amount"]

        top_cust = max(pelanggan_summary.items(), key=lambda x: x[1])
        print(f"Analisis Pelanggan selesai. Top Customer: {top_cust[0]} (Rp{top_cust[1]:,.2f})")
        return {"summary": pelanggan_summary, "top_customer": top_cust[0], "top_spent": top_cust[1]}

    @task
    def load_sqlite_dan_lapor(validation_res: dict, cat_res: dict, cust_res: dict):
        valid_rows = validation_res["valid_rows"]

        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS penjualan_mandiri (
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
                INSERT OR REPLACE INTO penjualan_mandiri
                (transaction_id, transaction_date, customer_id, product, quantity, amount, category)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                (r["transaction_id"], r["transaction_date"], r["customer_id"],
                 r["product"], r["quantity"], r["amount"], r["category"])
                for r in valid_rows
            ])
            conn.commit()

        total_omset = sum(r["amount"] for r in valid_rows)
        lines = [
            "============================================================",
            "         LAPORAN EKSEKUTIF PIPELINE DATA PENJUALAN         ",
            "             (TUGAS PRAKTIKUM MANDIRI M12)                 ",
            "============================================================",
            f"Waktu Proses Pipeline     : {pendulum.now('Asia/Jakarta').to_datetime_string()}",
            f"Jumlah Transaksi Valid    : {validation_res['total_valid']} transaksi",
            f"Jumlah Transaksi Ditolak  : {validation_res['total_reject']} transaksi",
            f"Total Omset Penjualan     : Rp{total_omset:,.2f}",
            f"Top Customer Belanja      : {cust_res['top_customer']} (Total: Rp{cust_res['top_spent']:,.2f})",
            "",
            "--- RINGKASAN PENJUALAN PER KATEGORI PRODUK ---",
        ]
        for cat, val in cat_res.items():
            lines.append(f"• Kategori {cat:<12} | Qty: {val['total_qty']:>2} item | Omset: Rp{val['total_omset']:>12,.2f}")
        lines.append("============================================================")

        REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")
        print("\n".join(lines))
        return str(REPORT_FILE)

    raw_data = ekstrak_data()
    cleaned_data = transformasi_data(raw_data)
    val_result = validasi_aturan(cleaned_data)

    cat_metrics = analisis_kategori(val_result)
    cust_metrics = analisis_pelanggan(val_result)

    load_sqlite_dan_lapor(val_result, cat_metrics, cust_metrics)


pipeline_pengolahan_penjualan()
