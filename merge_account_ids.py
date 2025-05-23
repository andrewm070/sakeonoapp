import pandas as pd
import uuid

def merge_account_ids_with_depletions():
    """
    Merge account IDs with depletions data, creating a new CSV with the account IDs included.
    Generates UUIDs for accounts if they don't exist.
    """
    print("Reading input files...")
    
    # Read the accounts file
    accounts_df = pd.read_csv('accounts_to_import.csv')
    
    # Generate UUIDs for accounts if they don't exist
    if 'id' not in accounts_df.columns:
        print("Generating UUIDs for accounts...")
        accounts_df['id'] = [str(uuid.uuid4()) for _ in range(len(accounts_df))]
        # Save the accounts with their new IDs
        accounts_df.to_csv('accounts_with_ids.csv', index=False)
        print("Saved accounts with generated IDs to accounts_with_ids.csv")
    
    # Read the depletions file
    depletions_df = pd.read_csv('depletions_to_import.csv')
    
    print(f"Found {len(accounts_df)} accounts and {len(depletions_df)} depletions")
    
    # Create a dictionary for faster lookups
    account_id_map = dict(zip(accounts_df['std_account_name'], accounts_df['id']))
    
    # Initialize list to store matched depletions
    matched_depletions = []
    skipped_count = 0
    
    # Process each depletion row
    for _, row in depletions_df.iterrows():
        account_name = row['std_account_name_for_lookup']
        account_id = account_id_map.get(account_name)
        
        if account_id is None:
            print(f"Warning: No matching account ID found for '{account_name}'")
            skipped_count += 1
            continue
            
        matched_depletions.append({
            'account_id': account_id,
            'order_date': row['order_date'],
            'cases': row['cases'],
            'sku': row['sku']
        })
    
    # Create the final DataFrame
    final_df = pd.DataFrame(matched_depletions)
    
    # Save to CSV
    final_df.to_csv('depletions_final_for_import.csv', index=False)
    
    print(f"\nProcessing complete:")
    print(f"- Successfully matched: {len(matched_depletions)} depletions")
    print(f"- Skipped: {skipped_count} depletions")
    print(f"- Output saved to: depletions_final_for_import.csv")

if __name__ == "__main__":
    merge_account_ids_with_depletions() 
    