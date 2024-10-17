import pandas as pd
import sqlite3

# Step 1: Extract
def extract_data(file_path):
    """
    Function to extract data from a CSV file
    :param file_path: Path to the input CSV file
    :return: pandas DataFrame
    """
    try:
        data = pd.read_csv(file_path)
        print("Data extraction complete.")
        return data
    except Exception as e:
        print(f"Error in extracting data: {e}")
        return None

# Step 2: Transform
def transform_data(data):
    """
    Function to clean and transform data
    :param data: pandas DataFrame
    :return: Transformed DataFrame
    """
    try:
        # Remove rows with missing values
        data_cleaned = data.dropna()

        # Convert 'date' column to datetime
        data_cleaned['date'] = pd.to_datetime(data_cleaned['date'])

        # Create a new column 'total_price' (e.g., quantity * unit_price)
        data_cleaned['total_price'] = data_cleaned['quantity'] * data_cleaned['unit_price']

        print("Data transformation complete.")
        return data_cleaned
    except Exception as e:
        print(f"Error in transforming data: {e}")
        return None

# Step 3: Load
def load_data_to_db(data, db_name, table_name):
    """
    Function to load transformed data into a SQLite database
    :param data: pandas DataFrame
    :param db_name: Database file name
    :param table_name: Table name in the database
    """
    try:
        # Connect to the SQLite database (or create it)
        conn = sqlite3.connect(db_name)

        # Load data into a table
        data.to_sql(table_name, conn, if_exists='replace', index=False)

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()
        
        print("Data loaded successfully into the database.")
    except Exception as e:
        print(f"Error in loading data to the database: {e}")

# Main ETL Function
def run_etl_pipeline():
    # File paths and database configurations
    input_file_path = 'sales_data.csv'  # Example input file
    db_file_name = 'sales_data.db'
    db_table_name = 'sales'

    # Extract the data from the CSV file
    data = extract_data(input_file_path)
    if data is None:
        return

    # Transform the data (cleaning, adding new columns)
    transformed_data = transform_data(data)
    if transformed_data is None:
        return

    # Load the transformed data into the database (SQLite for this example)
    load_data_to_db(transformed_data, db_file_name, db_table_name)

# Run the ETL pipeline
if __name__ == '__main__':
    run_etl_pipeline()

# Make sure this pipeline stays public.
