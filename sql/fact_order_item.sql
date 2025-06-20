-- 1. Create the table with UUID foreign keys and UUID surrogate key
CREATE TABLE IF NOT EXISTS fact_order_item (
    fact_order_item_sk UUID PRIMARY KEY DEFAULT uuid(),
    invoice_number TEXT,
    product_sk UUID,
    customer_sk UUID,
    date_sk UUID,
    location_sk UUID,
    quantity INTEGER,
    price DOUBLE,
    amount DOUBLE
);

-- 2. Insert new unique fact records with generated UUID surrogate key
INSERT INTO fact_order_item (
    invoice_number,
    product_sk,
    customer_sk,
    date_sk,
    location_sk,
    quantity,
    price,
    amount
)
SELECT
    o."Invoice" AS invoice_number,
    p.product_sk,
    c.customer_sk,
    d.date_sk,
    l.location_sk,
    o."Quantity" AS quantity,
    o."Price",
    o."Quantity" * o."Price" AS amount
FROM order_items o
JOIN dim_product p
    ON o."StockCode" = p.stock_code
left JOIN dim_customer c
    ON o."Customer ID" = c.customer_id
JOIN dim_date d
    ON STRPTIME(o."InvoiceDate", '%m/%d/%Y %H:%M') = d.full_date
left JOIN dim_location l
    ON o."Country" = l.country;

-- LEFT JOINS HERE ARE FOR THE PURPOSE OF TAKING IN ACCOUNT DEFECTED ROWS LIKE CUSTOMERS & COUNTRIES WITH NULL IDs
-- BUT DO NOT RESTRICT US FROM CONSIDERING THEM AS IMPACTFUL ON THE REVENUE
-- ANYONE QUERYING FOR AGGREGATIONS BASED ON CUSTOMER OR COUTNRY DISTRIBUTION FOR EXAMPLE SHOULD HIGHLY CONSIDER THE FACT THAT 
-- THESE DATA WILL CAUSE FALSY RESULTS