
import os
import traceback
import data_preprocessing
import database

def process_and_load_csv(file_path, batch_size=1000, max_rows=None):
    """
    Process a CSV file and load it directly into a processed table.
    This version correctly captures and passes the list of detected date columns.
    
    Args:
        file_path (str): Path to the CSV file.
        batch_size (int): Number of rows to process in a batch.
        max_rows (int): Maximum number of rows to process, None for all.
        
    Returns:
        dict: Processing statistics.
    """
    try:
        # Check if the CSV file exists
        if not os.path.exists(file_path):
            return {
                "error": f"CSV file {file_path} not found. Please check if the file exists.",
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Use the preprocess_data function, which now returns the detected date columns
        print(f"Preprocessing CSV file: {file_path}")
        processed_df, ip_detection_result, datetime_separation_result, date_columns_detected = data_preprocessing.preprocess_data(file_path)
        
        if processed_df is None:
            return {
                "error": "Failed to preprocess file",
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Apply max_rows limit if specified
        if max_rows is not None and max_rows > 0:
            processed_df = processed_df.head(max_rows)
            print(f"Limited to {max_rows} rows as requested")
        
        # Get IP columns from the detection result
        ip_columns = ip_detection_result.get('ip_columns', [])
        
        # Register the CSV file in the database, passing the reliable date_columns_detected list
        file_id = database.register_csv_file(
            file_path, 
            len(processed_df), 
            len(processed_df.columns),
            date_columns_detected,  
            ip_columns,
            datetime_separation_result
        )
        
        if file_id is None:
            return {
                "error": "Failed to register CSV file in the database",
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Save the processed data to the database
        file_name = os.path.basename(file_path)
        result = database.save_processed_data_to_db(
            processed_df, 
            file_id, 
            file_name, 
            ip_detection_result, 
            datetime_separation_result, 
            batch_size
        )
        
        # Add file_id and other info to the final result
        result["file_id"] = file_id
        result["datetime_separation_info"] = datetime_separation_result
        
        return result
        
    except Exception as e:
        print(f"Error processing and loading CSV: {e}")
        traceback.print_exc()
        return {
            "error": str(e),
            "processed_count": 0,
            "skipped_count": 0,
            "error_count": 0,
            "total_count": 0
        }
    
def process_and_load_to_existing_table(file_path, target_table, batch_size=1000):
    """
    Processes a CSV, validates its schema against a target table (ignoring id
    and processed_at), and appends data.

    Args:
        file_path (str): The path to the CSV file.
        target_table (str): The name of the existing table to append data to.
        batch_size (int): The number of rows to insert in each batch.

    Returns:
        dict: A dictionary containing processing statistics or an error message.
    """
    try:
        print(f"Starting process to append '{file_path}' to table '{target_table}'.")

        # Step 1: Preprocess the CSV data
        processed_df, _, _, _ = data_preprocessing.preprocess_data(file_path) #

        if processed_df is None:
            return {"error": "Failed to preprocess the CSV file.", "status_code": 500}
        
        if processed_df.empty:
            return {"error": "Preprocessing resulted in an empty dataset. No data to append."}

        # Step 2: Get columns from file and table for schema validation
        file_columns = sorted(processed_df.columns.tolist())
        
        table_columns_result = database.get_table_columns(target_table)

        if isinstance(table_columns_result, dict) and "error" in table_columns_result:
            return {**table_columns_result, "status_code": 404}

        # **MODIFICATION**: Ignore 'id' and 'processed_at' for the schema check
        filtered_table_columns = sorted([
            col for col in table_columns_result if col not in ('id', 'processed_at')
        ])
        
        print(f"File columns for validation ({len(file_columns)}): {file_columns}")
        print(f"Table columns for validation ({len(filtered_table_columns)}): {filtered_table_columns}")

        # Step 3: Compare the file columns against the filtered table columns
        if file_columns != filtered_table_columns:
            file_only = list(set(file_columns) - set(filtered_table_columns))
            table_only = list(set(filtered_table_columns) - set(file_columns))
            
            error_message = f"Schema mismatch for table '{target_table}'. Data was not appended."
            details = {
                "columns_in_file_only": file_only,
                "columns_in_table_only": table_only,
                "note": "'id' and 'processed_at' columns were ignored during this check."
            }
            print(f"ERROR: {error_message} - Details: {details}")
            return {"error": error_message, "details": details, "status_code": 400}

        # Step 4: If validation passes, call the database function to append the data
        print("Schema validation successful. Appending data...")
        result = database.append_to_existing_table(
            processed_df,
            target_table,
            batch_size
        ) #

        return result

    except Exception as e:
        print(f"Error in service layer while processing for existing table: {e}")
        traceback.print_exc()
        return {"error": str(e), "status_code": 500}