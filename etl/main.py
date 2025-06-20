import os
import pandas as pd
from pathlib import Path

def is_row_valid(row):
    try:
        invoice_id = str(row['Invoice']) if not pd.isna(row['Invoice']) else None
        stock_code = str(row['StockCode']) if not pd.isna(row['StockCode']) else None
        customer_id = str(row['Customer ID']) if not pd.isna(row['Customer ID']) else None
        quantity = int(row['Quantity']) if not pd.isna(row['Quantity']) else None
        price = float(row['Price']) if not pd.isna(row['Price']) else None
        invoice_date = pd.to_datetime(row['InvoiceDate']) if not pd.isna(row['InvoiceDate']) else None

        # First level filtering is not accepting 'breaking' values
        # Since we are talking about sales any type of defect in any of the columns below needs specification from business requirements to handle 
        # so for now data that do not match 'basic' rules will be kept in a seperate dataset
        if not invoice_id or not stock_code or not price or not quantity or not invoice_date:
            return False

        _ = quantity * price  # make sure it's computable
        return True

    except (ValueError, TypeError):
        return False

# This is the first layer of loadin the data from the csv for exploratory reasons 
# and by making use of the is_row_valid function the data is seperated for easier investigation

def load_and_prepare_data(con, csv_path, encoding='windows-1252'): # enforcement of this encoding happened after having trouble to load data with UTF-8
    # Specification of Customer ID as string is due to auto-infered type from DuckDB (assigned it as numeric only) 
    # This happened because when it was sampling the columns data it fell on numeric values and this resulted having customer id name "TEST" fail type checking
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

    print(f"\n Validation complete:")
    print(f" Valid rows: {len(df_valid)}")
    print(f" Defective rows: {len(df_defect)}")

    con.register('valid_df', df_valid)

    # This table will be used to store the data that has passed the major disqualifications 
    con.execute("""
    CREATE OR REPLACE TABLE order_items AS
    SELECT
        ROW_NUMBER() OVER () AS id,
        *
    FROM valid_df;
    """)

    # This table is for storing the defective data for further handling and analysis
    if len(defect_rows):
        con.register('defect_df', df_defect)
        con.execute("""
        CREATE OR REPLACE TABLE defected_order_items AS
        SELECT
            ROW_NUMBER() OVER () AS id,
            *
        FROM defect_df;
        """)

    print(f"\n Data written to DuckDB database")
    print("\n order_items schema:")
    print(con.execute("PRAGMA table_info('order_items');").fetchdf()[['name', 'type']])

# Extract contents of sql files as string 
def load_sql(filename: str) -> str:
    path = Path(__file__).parent.parent / 'sql' / filename  
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

# This function executes the sql code that creates and populates the star schema
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
        print(f"Running SQL file: {file} → {table_name}")
        sql = load_sql(file)
        try:
            con.execute(sql)
            print(f"✅ Executed {file}")
        except Exception as e:
            print(f"❌ Error in {file}: {e}")

# Invocation of all parts
def run_full_etl(con, csv_path="../data/invoices.csv"):
    load_and_prepare_data(con, csv_path)
    run_sql_transformations(con)
