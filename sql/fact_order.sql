-- 1. Create the table with UUID surrogate and foreign keys
CREATE TABLE IF NOT EXISTS fact_order (
    fact_order_sk UUID PRIMARY KEY DEFAULT uuid(),
    invoice_number TEXT,
    customer_sk UUID,
    location_sk UUID,
    min_date_sk UUID,
    max_date_sk UUID,
    total_amount DOUBLE
);

-- 2. Insert new unique fact_order records with generated UUID
INSERT INTO fact_order (
    invoice_number,
    customer_sk,
    location_sk,
    min_date_sk,
    max_date_sk,
    total_amount
)
SELECT
    invoice_number,
    customer_sk,
    location_sk,
    MIN(date_sk) AS min_date_sk,
    MAX(date_sk) AS max_date_sk,
    SUM(amount) AS total_amount
FROM fact_order_item
-- GROUPING BY THAT WAY TO GATHER LINE ITEMS TO AN ORDER
GROUP BY invoice_number, customer_sk, location_sk
HAVING NOT EXISTS (
    SELECT 1
    FROM fact_order f
    WHERE f.invoice_number = fact_order_item.invoice_number
      AND f.customer_sk = fact_order_item.customer_sk
      AND f.location_sk = fact_order_item.location_sk
);
-- HAVING NOT EXISTS HERE SERVES THE PURPOSE OF AVOIDING POSSIBLE DUPLICATE ENTRIES OF LINE ITEMS