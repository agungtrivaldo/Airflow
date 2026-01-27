from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pendulum
from pathlib import Path

SQL_DIR = Path("/opt/airflow/sql")
local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def load_sql(filename):
    return (SQL_DIR / filename).read_text()

with DAG(
    dag_id="data_ingest",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule="0 7 * * *",
    catchup=False,
    default_args=default_args,
    tags=["dwh", "incremental"]
):

    # ======================
    # DIM DATE
    # ======================
    @task
    def upsert_dim_customer():
        oltp = PostgresHook("data_ops")
        dwh = PostgresHook("data_warehouse")

        # Ambil data customer dari OLTP
        customers = oltp.get_records("SELECT id, name FROM customers")

        sql = load_sql("dim_customer_upsert.sql")

        # Jalankan UPSERT satu-satu
        for row in customers:
            dwh.run(sql, parameters=row)

    # ======================
    # DIM CUSTOMER
    # ======================
    @task
    def upsert_dim_customer():
        oltp = PostgresHook("data_ops")
        dwh = PostgresHook("data_warehouse")

        customers = oltp.get_records(
            "SELECT id, name FROM customers"
        )

        sql = load_sql("dim_customer_upsert.sql")
        dwh.run(sql, parameters=customers)

    # ======================
    # FACT ACCUMULATING
    # ======================
    @task
    def merge_fact_order():
        oltp = PostgresHook("data_ops")
        dwh = PostgresHook("data_warehouse")

        records = oltp.get_records("""
            SELECT
                order.order_number,
                order.customer_id,
                order.date AS order_date,
                invoice.invoice_number,
                invoice.date AS invoice_date,
                payment.payment_number,
                payment.date AS payment_date,
                SUM(order_line.quantity),
                SUM(order_line.usd_amount)
            FROM orders order
            JOIN order_lines order_line ON order.order_number = order_line.order_number
            LEFT JOIN invoices invoice ON order.order_number = invoice.order_number
            LEFT JOIN payments payment ON invoice.invoice_number = payment.invoice_number
            GROUP BY
                order.order_number, order.customer_id, order.date,
                invoice.invoice_number, invoice.date,
                payment.payment_number, payment.date
        """)

        sql = load_sql("fact_order_accumulating_merge.sql")

        rows = []
        for r in records:
            (
                order_number, customer_id, order_date,
                invoice_number, invoice_date,
                payment_number, payment_date,
                qty, usd
            ) = r

            order_id = int(order_date.strftime("%s"))
            invoice_id = int(invoice_date.strftime("%s")) if invoice_date else None
            payment_id = int(payment_date.strftime("%s")) if payment_date else None

            rows.append((
                order_id,
                invoice_id,
                payment_id,
                customer_id,
                order_number,
                invoice_number,
                payment_number,
                qty,
                usd,
                (invoice_date - order_date).days if invoice_date else None,
                (payment_date - invoice_date).days if payment_date else None
            ))

        dwh.run(sql, parameters=rows)

    upsert_dim_date() >> upsert_dim_customer() >> merge_fact_order()
