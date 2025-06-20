-- 1. Create the table with UUID surrogate key
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk UUID PRIMARY KEY DEFAULT uuid(),
    full_date TIMESTAMP UNIQUE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    weekday TEXT NOT NULL
);

-- 2. Insert new unique dates with generated UUID surrogate key
INSERT INTO dim_date (full_date, year, month, day, weekday)
SELECT
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(DAY FROM full_date) AS day,
    STRFTIME(full_date, '%w') AS weekday
FROM (
    SELECT DISTINCT STRPTIME(InvoiceDate, '%m/%d/%Y %H:%M') AS full_date
    FROM order_items
    WHERE InvoiceDate IS NOT NULL
) AS base
WHERE full_date IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM dim_date d WHERE d.full_date = base.full_date
);
-- LAST WHERE NOT EXISTS SERVES THE PURPOSE OF DEDUPLICATION