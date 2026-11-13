from __future__ import annotations

import csv
from pathlib import Path
import pendulum
from airflow.sdk import dag, task

DATA_FILE = Path("/opt/airflow/dags/data/transaksi.csv")


@dag(
    dag_id="dag_paralel_demo",
    description="Demo alur kerja percabangan paralel pada Airflow",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    tags=["praktikum", "paralel"],
)
def dag_paralel_demo():

    @task
    def extract_and_clean():
        with DATA_FILE.open("r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        # Saring baris yang valid (amount > 0 dan transaction_id terisi)
        valid_rows = [
            r for r in rows
            if r["transaction_id"] and float(r["amount"]) > 0
        ]
        print(f"Data siap diproses paralel: {len(valid_rows)} baris")
        return valid_rows

    @task
    def hitung_total_transaksi(rows: list[dict]):
        total_nominal = sum(float(r["amount"]) for r in rows)
        total_qty = sum(int(r["quantity"]) for r in rows)
        print(f"Total Nominal: Rp{total_nominal:,.2f}, Total Qty: {total_qty}")
        return {"total_nominal": total_nominal, "total_qty": total_qty}

    @task
    def hitung_per_kategori(rows: list[dict]):
        kategori_map = {}
        for r in rows:
            cat = r["category"].title()
            kategori_map[cat] = kategori_map.get(cat, 0) + float(r["amount"])
        print("Ringkasan per kategori:", kategori_map)
        return kategori_map

    @task
    def buat_laporan(total_metric: dict, category_metric: dict):
        print("\n=== LAPORAN GABUNGAN ANALISIS PARALEL ===")
        print(f"Total Nilai Transaksi : Rp{total_metric['total_nominal']:,.2f}")
        print(f"Total Kuantitas       : {total_metric['total_qty']} item")
        print("\nRincian Penjualan per Kategori:")
        for cat, val in category_metric.items():
            print(f"- {cat}: Rp{val:,.2f}")
        print("=========================================\n")

    # Alur paralel: extract_and_clean bercabang dua, lalu menyatu ke buat_laporan
    data = extract_and_clean()
    metrik_total = hitung_total_transaksi(data)
    metrik_kategori = hitung_per_kategori(data)
    buat_laporan(metrik_total, metrik_kategori)


dag_paralel_demo()
