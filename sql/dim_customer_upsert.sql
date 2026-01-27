INSERT INTO dim_date (id, date, month, quater_of_year, year, is_weekend)
SELECT
    EXTRACT(EPOCH FROM d)::int AS id,
    d::date,
    EXTRACT(MONTH FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(YEAR FROM d),
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END
FROM generate_series(
    '2020-01-01'::date,
    CURRENT_DATE,
    interval '1 day'
) d
ON CONFLICT (id) DO NOTHING;
