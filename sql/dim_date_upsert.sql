INSERT INTO dim_customer (id, name)
VALUES (%s, %s)
ON CONFLICT (id)
DO UPDATE SET
  name = EXCLUDED.name;
