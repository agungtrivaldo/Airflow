from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pendulum
from psycopg2.extras import execute_values

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

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
    def upsert_dim_date():
        dwh = PostgresHook("data_warehouse")
        sql = """
        INSERT INTO dim_date (id, date, month, quater_of_year, year, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET
            date = EXCLUDED.date,
            month = EXCLUDED.month,
            quater_of_year = EXCLUDED.quater_of_year,
            year = EXCLUDED.year,
            is_weekend = EXCLUDED.is_weekend;
        """
        # Here you would generate the list of dates to upsert
        from datetime import date, timedelta
        start = date(2024, 1, 1)
        end = date.today()
        rows = []
        current = start
        while current <= end:
            rows.append((
                int(current.strftime("%Y%m%d")),  # id as YYYYMMDD
                current,
                current.month,
                (current.month - 1) // 3 + 1,
                current.year,
                current.weekday() >= 5
            ))
            current += timedelta(days=1)

        conn = dwh.get_conn()
        cur = conn.cursor()
        execute_values(cur, sql, rows)
        conn.commit()
        cur.close()
        conn.close()

    # ======================
    # DIM CUSTOMER
    # ======================
    @task
    def upsert_dim_customer():
        oltp = PostgresHook("data_ops")
        dwh = PostgresHook("data_warehouse")

        customers = oltp.get_records("SELECT id, name FROM customers")

        sql = """
        INSERT INTO dim_customer (id, name)
        VALUES %s
        ON CONFLICT (id)
        DO UPDATE SET
            name = EXCLUDED.name;
        """
        conn = dwh.get_conn()
        cur = conn.cursor()
        execute_values(cur, sql, customers)
        conn.commit()
        cur.close()
        conn.close()

    # ======================
    # FACT ACCUMULATING
    # ======================
    @task
    def merge_fact_order():
        oltp = PostgresHook("data_ops")
        dwh = PostgresHook("data_warehouse")

        records = oltp.get_records("""
            SELECT
                o.order_number,
                o.customer_id,
                o.date AS order_date,
                i.invoice_number,
                i.date AS invoice_date,
                p.payment_number,
                p.date AS payment_date,
                SUM(ol.quantity),
                SUM(ol.usd_amount)
            FROM orders o
            JOIN order_lines ol ON o.order_number = ol.order_number
            LEFT JOIN invoices i ON o.order_number = i.order_number
            LEFT JOIN payments p ON i.invoice_number = p.invoice_number
            GROUP BY
                o.order_number, o.customer_id, o.date,
                i.invoice_number, i.date,
                p.payment_number, p.date
        """)

        sql = """
        INSERT INTO fact_order_accumulating (
            order_date_id, invoice_date_id, payment_date_id, customer_id,
            order_number, invoice_number, payment_number,
            total_order_quantity, total_order_usd_amount,
            order_to_invoice_lag_days, invoice_to_payment_lag_days
        )
        VALUES %s
        ON CONFLICT (order_number)
        DO UPDATE SET
            invoice_number = EXCLUDED.invoice_number,
            payment_number = EXCLUDED.payment_number,
            total_order_quantity = EXCLUDED.total_order_quantity,
            total_order_usd_amount = EXCLUDED.total_order_usd_amount,
            order_to_invoice_lag_days = EXCLUDED.order_to_invoice_lag_days,
            invoice_to_payment_lag_days = EXCLUDED.invoice_to_payment_lag_days;
        """

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

        conn = dwh.get_conn()
        cur = conn.cursor()
        execute_values(cur, sql, rows)
        conn.commit()
        cur.close()
        conn.close()

    # DAG dependencies
    upsert_dim_customer() >> upsert_dim_date() >> merge_fact_order()
