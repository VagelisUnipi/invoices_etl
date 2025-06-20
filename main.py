import duckdb
from etl.main import run_full_etl  # Main ETL pipeline (data load, clean, transform)
from etl.reporting import generate_reports  # Reporting logic using the transformed data

def main():
    # Define paths to DuckDB and raw CSV data
    db_path = "./db/invoice_dw.db"
    csv_path = "./data/invoices.csv"

    print(f"Connecting to DuckDB at: {db_path}")
    con = duckdb.connect(database=db_path)

    try:
        # Step 1: Run full ETL pipeline (loads + splits defected rows + saves to DuckDB)
        run_full_etl(con, csv_path=csv_path)

        # Step 2: Generate reporting outputs from tables
        report_data = generate_reports(con)

        # Step 3: Optionally print and save report results
        print("\n Reporting Output:")
        for name, df in report_data.items():
            print(f"\n➡ {name.upper()}:\n", df.head())

            # Save each report as CSV in ./reports/
            output_path = f"./reports/{name}.csv"
            df.to_csv(output_path, index=False, float_format="%.2f")
            print(f"Saved: {output_path}")

    finally:
        con.close()
        print("DuckDB connection closed.")

if __name__ == "__main__":
    main()
