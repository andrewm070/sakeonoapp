import pandas as pd
from datetime import datetime, timedelta
import os

def process_vip_export(input_file, start_date_str, month_columns):
    """
    Process VIP export data and create two output files:
    1. accounts_to_import.csv - unique account information
    2. depletions_to_import.csv - unpivoted depletion data
    
    Args:
        input_file (str): Path to the input VIP export CSV file
        start_date_str (str): Start date in YYYY-MM-DD format for the first month
        month_columns (list): List of column names for monthly sales data
    """
    # Read the input CSV file, using the second row as the header
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file, header=1)
    print("Columns in DataFrame:")
    print(list(df.columns))
    
    # 1. Create accounts_to_import.csv
    print("Creating accounts file...")
    
    # Select and rename columns for accounts file (using correct capitalization and spacing)
    accounts_df = df[[
        'Retail Accounts',
        'Classes of Trade',
        'OnOff Premises',
        'Address',
        'City',
        'State',
        'Zip Code'
    ]].copy()
    
    # Rename columns to match required output format
    accounts_df.columns = [
        'std_account_name',
        'class_of_trade',
        'on_off_premise',
        'address',
        'city',
        'state',
        'zip_code'
    ]
    
    # Remove duplicates and save
    accounts_df = accounts_df.drop_duplicates()
    accounts_df.to_csv('accounts_to_import.csv', index=False)
    print(f"Saved {len(accounts_df)} unique accounts to accounts_to_import.csv")
    
    # 2. Create depletions_to_import.csv
    print("Creating depletions file...")
    
    # Get the start date
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    
    # Create a list to store the unpivoted data
    depletions_data = []
    
    # Process each monthly column
    for i, month_col in enumerate(month_columns):
        if month_col not in df.columns:
            print(f"Warning: Column {month_col} not found in input file")
            continue
            
        # Calculate the date for this month
        current_date = start_date + timedelta(days=30 * i)
        order_date = current_date.strftime('%Y-%m-%d')
        
        # Create a temporary dataframe for this month
        month_df = df[['Retail Accounts', month_col]].copy()
        month_df.columns = ['std_account_name_for_lookup', 'cases']
        
        # Add order_date and sku columns
        month_df['order_date'] = order_date
        month_df['sku'] = 'DEFAULT_SKU'
        
        # Filter out zero or empty cases
        month_df = month_df[month_df['cases'].notna() & (month_df['cases'] != 0)]
        
        depletions_data.append(month_df)
    
    # Combine all monthly data
    depletions_df = pd.concat(depletions_data, ignore_index=True)
    
    # Save to CSV
    depletions_df.to_csv('depletions_to_import.csv', index=False)
    print(f"Saved {len(depletions_df)} depletion records to depletions_to_import.csv")

if __name__ == "__main__":
    # Configuration
    INPUT_FILE = "vip_export_raw.csv"
    START_DATE = "2024-04-01"  # Start date for the first month in the data
    
    # Define your monthly column names here - using the actual DataFrame column names
    MONTH_COLUMNS = [
        '9L', '9L.1', '9L.2', '9L.3', '9L.4', '9L.5', '9L.6', '9L.7', '9L.8', '9L.9', '9L.10', '9L.11', '9L.12'
    ]
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found!")
    else:
        process_vip_export(INPUT_FILE, START_DATE, MONTH_COLUMNS) 