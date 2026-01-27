INSERT INTO dim_customer (id, name)
SELECT DISTINCT
    c.id,
    c.name
FROM customers c
ON CONFLICT (id)
DO UPDATE SET
    name = EXCLUDED.name;
