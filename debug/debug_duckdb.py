import duckdb
import sys
import os

def inspect_duckdb(db_path):
    # Check if file exists to avoid creating a new empty DB
    if not os.path.exists(db_path):
        print(f"Error: File '{db_path}' not found.")
        return

    try:
        # Connect in read-only mode to prevent accidental locks or changes
        conn = duckdb.connect(database=db_path, read_only=True)
        
        # Query for table names, column names, and data types
        query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_name, ordinal_position;
        """
        results = conn.execute(query).fetchall()

        if not results:
            print("No tables found in the database.")
            return

        current_table = None
        for table, column, dtype in results:
            if table != current_table:
                print(f"\n{table}")
                current_table = table
            print(f"- {column}: {dtype}")

        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <path_to_duckdb_file>")
    else:
        inspect_duckdb(sys.argv[1])
