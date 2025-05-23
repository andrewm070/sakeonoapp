import pandas as pd
# No need to import uuid here anymore, as we're not generating them in this script

def merge_true_supabase_ids_with_depletions():
    """
    Merge Supabase account IDs (from accounts_with_supabase_ids.csv) with depletions data.
    Ensures data types are compatible with Supabase.
    """
    print("Starting the merge process with true Supabase IDs...")

    # --- Step 1: Read the CORRECT accounts file ---
    # This file MUST be the one downloaded from Supabase AFTER importing accounts,
    # and it MUST contain the 'id' column with Supabase-generated UUIDs.
    accounts_file_name = 'accounts_with_supabase_ids.csv'
    try:
        print(f"Reading Supabase accounts data from: {accounts_file_name}")
        # Ensure the 'id' column (Supabase UUID) is read as a string
        accounts_df = pd.read_csv(accounts_file_name, dtype={'id': str})
        print(f"Successfully read {accounts_file_name}. Found {len(accounts_df)} accounts.")
    except FileNotFoundError:
        print(f"CRITICAL ERROR: The file '{accounts_file_name}' was not found.")
        print("This file should be exported from your Supabase 'Accounts' table after importing accounts.")
        print("It must contain the 'id' column with the Supabase-generated UUIDs.")
        return # Stop the script

    # Check if essential columns exist in accounts_df
    if 'id' not in accounts_df.columns:
        print(f"CRITICAL ERROR: The 'id' column (for Supabase UUIDs) is missing in '{accounts_file_name}'.")
        print("Please ensure you downloaded the correct file from Supabase and that it includes the 'id' column.")
        return
    if 'std_account_name' not in accounts_df.columns: # Or whatever your standard account name column is
        print(f"CRITICAL ERROR: The 'std_account_name' column is missing in '{accounts_file_name}'.")
        print("This column is needed for matching.")
        return

    # --- Step 2: Read the depletions file ---
    # This is the output from your first script (unpivot_vip_data.py)
    depletions_file_name = 'depletions_to_import.csv'
    try:
        print(f"Reading depletions data from: {depletions_file_name}")
        depletions_df = pd.read_csv(depletions_file_name)
        print(f"Successfully read {depletions_file_name}. Found {len(depletions_df)} depletions rows.")
    except FileNotFoundError:
        print(f"CRITICAL ERROR: The file '{depletions_file_name}' was not found.")
        print("This file should be the output of your first script (e.g., unpivot_vip_data.py).")
        return

    if 'std_account_name_for_lookup' not in depletions_df.columns: # Or whatever your lookup name column is
        print(f"CRITICAL ERROR: The 'std_account_name_for_lookup' column is missing in '{depletions_file_name}'.")
        print("This column is needed for matching.")
        return

    # --- Step 3: Prepare for matching (case-insensitive, strip spaces) ---
    print("Preparing account names for matching (lowercase, strip spaces)...")
    accounts_df['match_col_accounts'] = accounts_df['std_account_name'].astype(str).str.lower().str.strip()
    depletions_df['match_col_depletions'] = depletions_df['std_account_name_for_lookup'].astype(str).str.lower().str.strip()

    # --- Step 4: Create a dictionary for faster lookups using the CORRECT Supabase IDs ---
    # Key: normalized account name, Value: Supabase UUID
    # Using 'match_col_accounts' as the key
    print("Creating account ID lookup map...")
    account_id_map = dict(zip(accounts_df['match_col_accounts'], accounts_df['id']))
    print(f"Lookup map created with {len(account_id_map)} entries.")

    # --- Step 5: Process each depletion row and match ---
    print("Processing depletions and matching to accounts...")
    matched_depletions_data = []
    unmatched_account_names = set() # To store unique names that didn't match
    skipped_due_to_invalid_cases = 0

    for _, row in depletions_df.iterrows():
        # Use the normalized account name from depletions_df for lookup
        normalized_lookup_name = row['match_col_depletions']
        account_id_from_map = account_id_map.get(normalized_lookup_name) # Use .get() for safer lookup

        if account_id_from_map is None:
            unmatched_account_names.add(row['std_account_name_for_lookup']) # Store original name for warning
            continue # Skip this depletion row if no account ID found

        # Ensure cases is a valid number
        try:
            cases = float(row['cases'])
            if pd.isna(cases) or cases <= 0: # Assuming 0 cases is not a valid depletion to import
                # print(f"Debug: Invalid cases value for '{row['std_account_name_for_lookup']}': {row['cases']}")
                skipped_due_to_invalid_cases +=1
                continue
        except (ValueError, TypeError):
            # print(f"Debug: Invalid cases value (type error) for '{row['std_account_name_for_lookup']}': {row['cases']}")
            skipped_due_to_invalid_cases += 1
            continue

        matched_depletions_data.append({
            'account_id': account_id_from_map, # This is now the Supabase UUID
            'order_date': row['order_date'],
            'cases': cases,
            'sku': row['sku']
        })

    # --- Step 6: Create the final DataFrame ---
    if not matched_depletions_data:
        print("CRITICAL WARNING: No depletions were matched to accounts. The output file will be empty or not created.")
        print("Please check for warnings about unmatched account names and verify your input files.")
        return

    final_df = pd.DataFrame(matched_depletions_data)
    print(f"Created final DataFrame with {len(final_df)} matched depletions.")

    # --- Step 7: Ensure data types are correct for Supabase ---
    print("Ensuring correct data types for final output...")
    final_df['account_id'] = final_df['account_id'].astype(str) # Already string, but good practice
    final_df['cases'] = final_df['cases'].astype(float) # Supabase numeric can take float
    try:
        final_df['order_date'] = pd.to_datetime(final_df['order_date']).dt.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Warning: Could not convert all 'order_date' values to YYYY-MM-DD format. Error: {e}")
        print("Please check the 'order_date' column in 'depletions_to_import.csv'.")
    final_df['sku'] = final_df['sku'].astype(str)

    # Optional: Remove any rows that might have become all NaN after processing (though less likely now)
    # final_df = final_df.dropna(how='all')

    # --- Step 8: Save to CSV ---
    output_file_name = 'depletions_final_for_import.csv'
    try:
        final_df.to_csv(output_file_name, index=False)
        print(f"\nProcessing complete:")
        print(f"- Successfully processed for matching: {len(final_df)} depletions")
        if unmatched_account_names:
            print(f"- WARNING: Could not find Supabase IDs for {len(unmatched_account_names)} unique account names. These depletions were SKIPPED:")
            for name in sorted(list(unmatched_account_names))[:10]: # Print first 10 for brevity
                 print(f"  - {name}")
            if len(unmatched_account_names) > 10:
                print(f"  ... and {len(unmatched_account_names) - 10} more.")
        if skipped_due_to_invalid_cases > 0:
            print(f"- Skipped {skipped_due_to_invalid_cases} depletions due to invalid or zero 'cases' values.")
        print(f"- Output saved to: {output_file_name}")

        # Print a sample of the final data if it's not empty
        if not final_df.empty:
            print("\nSample of final data (first 5 rows):")
            print(final_df.head().to_string())
        else:
            print("\nNote: The final output file is empty as no depletions could be fully processed and matched.")

    except Exception as e:
        print(f"CRITICAL ERROR: Could not save the output file '{output_file_name}'. Error: {e}")


if __name__ == "__main__":
    merge_true_supabase_ids_with_depletions()