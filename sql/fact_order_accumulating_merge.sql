INSERT INTO fact_order_accumulating (
  order_date_id,
  invoice_date_id,
  payment_date_id,
  customer_id,
  order_number,
  invoice_number,
  payment_number,
  total_order_quantity,
  total_order_usd_amount,
  order_to_invoice_lag_days,
  invoice_to_payment_lag_days
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (order_number)
DO UPDATE SET
  invoice_date_id = EXCLUDED.invoice_date_id,
  payment_date_id = EXCLUDED.payment_date_id,
  invoice_number = EXCLUDED.invoice_number,
  payment_number = EXCLUDED.payment_number,
  order_to_invoice_lag_days = EXCLUDED.order_to_invoice_lag_days,
  invoice_to_payment_lag_days = EXCLUDED.invoice_to_payment_lag_days,
  total_order_quantity = EXCLUDED.total_order_quantity,
  total_order_usd_amount = EXCLUDED.total_order_usd_amount;
