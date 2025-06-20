-- 1. Create the table with UUID surrogate key
CREATE TABLE IF NOT EXISTS dim_location (
    location_sk UUID PRIMARY KEY DEFAULT uuid(),
    country TEXT UNIQUE NOT NULL
);

-- 2. Insert unique countries with generated UUID surrogate key
INSERT INTO dim_location (country)
SELECT country
FROM (
    SELECT DISTINCT country
    FROM order_items
    WHERE country IS NOT NULL
) AS unique_countries
WHERE NOT EXISTS (
    SELECT 1 FROM dim_location dl WHERE dl.country = unique_countries.country
);

-- LAST WHERE NOT EXISTS SERVES THE PURPOSE OF DEDUPLICATION