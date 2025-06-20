import duckdb
import pandas as pd

def generate_reports(con):
    print("Generating reports from `fact_order`...\n")

    # 1. Top 5 customers by total amount
    top_customers_df = con.execute("""
        SELECT 
            dc.customer_id, 
            ROUND(SUM(fo.total_amount), 2) AS total_spent
        FROM fact_order fo
        JOIN dim_customer dc ON dc.customer_sk = fo.customer_sk
        WHERE dc.customer_id IS NOT NULL 
        GROUP BY dc.customer_id
        ORDER BY total_spent DESC
        LIMIT 5;
    """).fetchdf()

    print("Top 5 Customers by Total Amount:")
    print(top_customers_df, end="\n\n")

    # 2. Top selling Products for the first 4 months of 2010
    top_products_by_month = con.execute("""
        SELECT
            year,
            month,
            stock_code,
            description,
            ROUND(total_revenue, 2) AS total_revenue
        FROM (
            SELECT
                d.year,
                d.month,
                p.stock_code,
                p.description,
                SUM(foi.amount) AS total_revenue,
                ROW_NUMBER() OVER (
                    PARTITION BY d.year, d.month
                    ORDER BY SUM(foi.amount) DESC
                ) AS rn
            FROM fact_order_item foi
            JOIN dim_product p ON foi.product_sk = p.product_sk
            JOIN dim_date d ON foi.date_sk = d.date_sk
            WHERE d.year = 2010 AND d.month <= 4
            GROUP BY d.year, d.month, p.stock_code, p.description
        ) ranked
        WHERE rn <= 5
        ORDER BY year, month, total_revenue DESC;
    """).fetchdf()
    # ROW_NUMBER assigns a unique sequential number to each row within each (year, month) group, ordered by total revenue descending.
    # This could be also structured otherwise using CTEs and the whole process of ROW_NUMBER would return the ranked products of each month

    # 3. Monthly Revenue Trends
    monthly_revenue_df = con.execute("""
        SELECT
            d.year,
            d.month,
            ROUND(SUM(f.total_amount), 2) AS monthly_revenue
        FROM fact_order f
        JOIN dim_date d ON f.min_date_sk = d.date_sk
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
    """).fetchdf()

    # Optional: Add a year-month label column for plotting
    monthly_revenue_df["year_month"] = monthly_revenue_df.apply(
        lambda row: f"{int(row['year'])}-{int(row['month']):02d}", axis=1
    )

    print("Monthly Revenue Trends (Graph-Ready):")
    print(monthly_revenue_df)

    return {
        "top_customers": top_customers_df,
        "top_products_by_month": top_products_by_month,
        "monthly_revenue": monthly_revenue_df
    }
