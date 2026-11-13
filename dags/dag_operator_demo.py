import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_operator_demo",
    description="Contoh operator dan dependency",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    tags=["praktikum", "operator"],
) as dag:

    start = EmptyOperator(task_id="start")

    tampilkan_pesan = BashOperator(
        task_id="tampilkan_pesan",
        bash_command="echo 'Praktikum Apache Airflow dimulai'",
    )

    cek_waktu = BashOperator(
        task_id="cek_waktu",
        bash_command="date",
    )

    selesai = EmptyOperator(task_id="selesai")

    start >> tampilkan_pesan >> cek_waktu >> selesai
