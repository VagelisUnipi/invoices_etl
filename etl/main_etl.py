import os
import pandas as pd
import duckdb
from pathlib import Path

def is_row_valid(row):
    try:
        invoice_id = str(row['Invoice']) if not pd.isna(row['Invoice']) else None
        stock_code = str(row['StockCode']) if not pd.isna(row['StockCode']) else None
        customer_id = str(row['Customer ID']) if not pd.isna(row['Customer ID']) else None
        quantity = int(row['Quantity'])
        price = float(row['Price'])

        if not invoice_id or not stock_code:
            return False

        _ = quantity * price  # check total_price is computable
        return True

    except (ValueError, TypeError):
        return False

def load_and_prepare_data(csv_path, db_path='./db/invoice_dw.db', encoding='windows-1252'):
    dir_path = os.path.dirname(db_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    df = pd.read_csv(csv_path, encoding=encoding, dtype={"Customer ID": "string"})
    print(f"✅ Loaded CSV with shape {df.shape}")

    valid_rows = []
    defect_rows = []

    for _, row in df.iterrows():
        if is_row_valid(row):
            valid_rows.append(row)
        else:
            defect_rows.append(row)

    df_valid = pd.DataFrame(valid_rows)
    df_defect = pd.DataFrame(defect_rows).astype(str)

    print(f"\n🔎 Validation complete:")
    print(f"  ✔ Valid rows: {len(df_valid)}")
    print(f"  ❌ Defective rows: {len(df_defect)}")

    con = duckdb.connect(database=db_path)

    con.register('valid_df', df_valid)

    con.execute("""
    CREATE OR REPLACE TABLE order_items AS
    SELECT
        ROW_NUMBER() OVER () AS id,
        *
    FROM valid_df;
    """)

    if len(defect_rows):
        con.register('defect_df', df_defect)
        con.execute("""
        CREATE OR REPLACE TABLE defected_order_items AS
        SELECT
            ROW_NUMBER() OVER () AS id,
            *
        FROM defect_df;
        """)


 

    print(f"\n💾 Data written to DuckDB database file: {db_path}")
    print("📂 You can now open this file in DBeaver.")
    print("\n📄 order_items schema:")
    print(con.execute("PRAGMA table_info('order_items');").fetchdf()[['name', 'type']])

    return con  # keep connection open for next steps

def load_sql(filename: str) -> str:
    path = Path(__file__).parent.parent / 'sql' / filename  # Adjusted path as you requested
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def run_sql_transformations(con):
    sql_files = [
        "dim_customer.sql",
        "dim_product.sql",
        "dim_date.sql",
        "dim_location.sql",
        "fact_order_item.sql",
        "fact_order.sql"
    ]

    for file in sql_files:
        table_name = file.replace(".sql", "")
        print(f"Running on DuckDB: {file} → {table_name}")
        sql = load_sql(file)
        try:
            con.execute(sql)
            print(f"✅ Executed {file}")
        except Exception as e:
            print(f"❌ Error in {file}: {e}")
            print("✅ DuckDB-only SQL ETL completed.")

def run_full_etl(csv_path="../data/invoices.csv", db_path="../db/invoice_dw.db"):
    con = load_and_prepare_data(csv_path, db_path)
    run_sql_transformations(con)
    con.close()
    print("🔐 DuckDB connection closed.")

if __name__ == "__main__":
    run_full_etl()
