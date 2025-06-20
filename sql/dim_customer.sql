-- 1. Create the table with UUID surrogate key
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk UUID PRIMARY KEY DEFAULT uuid(),
    customer_id TEXT UNIQUE NOT NULL
);

-- 2. Insert new unique records with generated UUID surrogate key
INSERT INTO dim_customer (customer_id)
SELECT DISTINCT "Customer ID" AS customer_id
FROM order_items
WHERE "Customer ID" IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM dim_customer dc WHERE dc.customer_id = order_items."Customer ID"
);
-- LAST WHERE NOT EXISTS SERVES THE PURPOSE OF DEDUPLICATION