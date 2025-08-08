import os
import traceback
import data_preprocessing
import database
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

def process_and_load_csv(file_path, batch_size=1000, max_rows=None):
    """
    Optimized: Process a CSV file and load it directly into a processed table using chunked, parallel, and vectorized processing.
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
        
        print(f"Preprocessing CSV file: {file_path}")
        
        # Use chunked reading for large files
        chunks = []
        for chunk in pd.read_csv(file_path, chunksize=batch_size, encoding='utf-8', low_memory=False):
            # Vectorized preprocessing per chunk
            processed_df, ip_detection_result, datetime_separation_result, date_columns_detected = data_preprocessing.preprocess_dataframe(chunk)
            if processed_df is not None and not processed_df.empty:
                chunks.append((processed_df, ip_detection_result, datetime_separation_result, date_columns_detected))
        
        if not chunks:
            return {
                "error": "Failed to preprocess file",
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Concatenate all processed chunks
        processed_df = pd.concat([c[0] for c in chunks], ignore_index=True)
        ip_detection_result = chunks[0][1]  # Use first chunk's detection (or merge if needed)
        datetime_separation_result = chunks[0][2]
        date_columns_detected = chunks[0][3]
        
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
        
        file_name = os.path.basename(file_path)
        result = database.save_processed_data_to_db(
            processed_df, 
            file_id, 
            file_name, 
            ip_detection_result, 
            datetime_separation_result, 
            batch_size
        )
        
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
    Optimized: Processes a CSV, validates its schema against a target table, and appends data using chunked and vectorized processing.
    """
    try:
        print(f"Starting process to append '{file_path}' to table '{target_table}'.")
        
        chunks = []
        for chunk in pd.read_csv(file_path, chunksize=batch_size, encoding='utf-8', low_memory=False):
            processed_df, _, _, _ = data_preprocessing.preprocess_dataframe(chunk)
            if processed_df is not None and not processed_df.empty:
                chunks.append(processed_df)
        
        if not chunks:
            return {"error": "Failed to preprocess the CSV file.", "status_code": 500}
        
        processed_df = pd.concat(chunks, ignore_index=True)
        
        if processed_df.empty:
            return {"error": "Preprocessing resulted in an empty dataset. No data to append."}
        
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

def preprocess_and_append_data(df, target_table):
    """
    Takes a raw DataFrame, preprocesses it, and appends it to a target table.
    If the table does not exist, it creates it first.
    """
    try:
        print(f"Service call to preprocess and append data to '{target_table}'.")
        
        # Step 1: Preprocess the DataFrame
        processed_df, _, _, _ = data_preprocessing.preprocess_dataframe(df)

        if processed_df is None or processed_df.empty:
            return {"error": "Preprocessing resulted in an empty dataset. No data to append."}

        # Step 2: Check if the target table exists
        if not database.table_exists(target_table):
            print(f"Table '{target_table}' not found. Creating it now.")
            # If table doesn't exist, create it and insert the data
            return database.create_table_from_df(processed_df, target_table)
        else:
            print(f"Table '{target_table}' already exists. Appending data.")
            # If table exists, just append the data
            return database.append_to_existing_table(processed_df, target_table)

    except Exception as e:
        print(f"Error in service layer during preprocess_and_append: {e}")
        traceback.print_exc()
        return {"error": str(e), "status_code": 500}