-- 1. Create the dimension table with UUID surrogate key
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk UUID PRIMARY KEY DEFAULT uuid(),
    stock_code TEXT NOT NULL,
    description TEXT NOT NULL,
    UNIQUE(stock_code)
);

-- 2. Insert new products with generated UUID surrogate key
INSERT INTO dim_product (stock_code, description)
SELECT stock_code, description
FROM (
    SELECT 
        "StockCode" AS stock_code,
        description,
        ROW_NUMBER() OVER (
            PARTITION BY "StockCode" 
            ORDER BY "InvoiceDate" DESC
        ) AS row_num
    FROM order_items
    WHERE "StockCode" IS NOT NULL 
      AND description IS NOT NULL 
      AND "InvoiceDate" IS NOT NULL
) AS deduped
WHERE row_num = 1
  AND NOT EXISTS (
      SELECT 1 
      FROM dim_product dp 
      WHERE dp.stock_code = deduped.stock_code
  );

-- LAST WHERE NOT EXISTS SERVES THE PURPOSE OF DEDUPLICATION MAKING USE OF THE COMPOSITE UNIQUE KEY
