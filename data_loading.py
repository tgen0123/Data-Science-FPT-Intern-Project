# data_loading.py
from flask import Blueprint, jsonify, request, g, current_app
from functools import wraps
from datetime import datetime
import pandas as pd
import json
import pyodbc
import numpy as np
import os
import re
import collections
import traceback
import time
import multiprocessing
import math
from helper import get_db, get_cursor, dict_from_row, require_api_key, is_admin
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from logger import data_logger, query_logger, error_logger, log_error, log_db_query

# Create a Blueprint for data loading and preprocessing routes
data_bp = Blueprint('data', __name__, url_prefix='/api/data')

# ---- Preprocessing Functions ----

def deduplicate_columns(columns):
    """Deduplicate column names for compatibility."""
    counter = collections.Counter()
    new_cols = []
    seen = set()
    for col in columns:
        col = str(col).strip().lower()
        col = re.sub(r'[^a-zA-Z0-9]', '_', col)
        if not col:
            col = 'unnamed'
        base_col = col
        suffix = counter[base_col]
        while col in seen:
            suffix += 1
            col = f"{base_col}_{suffix}"
        counter[base_col] += 1
        seen.add(col)
        new_cols.append(col)
    return new_cols

def detect_noisy_columns(df):
    """
    Automatically detect XML/noisy columns without hardcoding specific column names.
    
    Args:
        df: DataFrame to analyze
    Returns:
        list: Detected noisy columns
    """
    noisy_columns = []

    for col in df.columns:
        if df[col].dtype == object:  # Only check text columns
            # Convert to string in case we have non-string objects
            sample_values = df[col].astype(str).dropna().head(50)

            if len(sample_values) == 0:
                continue

            # Calculate metrics for this column
            avg_length = sample_values.str.len().mean()

            # Check for XML patterns (opening/closing tags or XML declarations)
            xml_pattern = re.compile(r'<[^>]+>|<\?xml')
            xml_matches = sum(sample_values.apply(lambda x: bool(xml_pattern.search(x))))
            xml_ratio = xml_matches / len(sample_values) if len(sample_values) > 0 else 0

            # Check for very long text
            is_long_text = avg_length > 500

            # Special character density (brackets, quotes, etc.)
            special_char_pattern = re.compile(r'[<>{}[\]\"\'=:]')
            special_char_density = sample_values.apply(
                lambda x: len(special_char_pattern.findall(x)) / len(x) if len(x) > 0 else 0
            ).mean()

            # Decision to flag based on combined metrics
            if (xml_ratio > 0.3 or              # Has XML content
                (is_long_text and special_char_density > 0.05) or  # Long text with many special chars
                avg_length > 1000):             # Extremely long text
                noisy_columns.append(col)
                data_logger.info(f"Column '{col}' detected as noisy: xml_ratio={xml_ratio:.2f}, avg_length={avg_length:.1f}, special_char_density={special_char_density:.3f}")

    msg = f"Noisy columns: {noisy_columns}"
    print(msg)
    data_logger.info(msg)
    return noisy_columns

def find_duplicate_columns(df):
    """
    Find columns with identical values using a smarter approach.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        tuple: (list of results, list of columns to drop)
    """
    # Get all columns to check
    columns = df.columns.tolist()
    
    if len(columns) < 2:
        return ["Less than 2 columns found"], []

    # Initialize groups
    groups = []
    processed_columns = set()

    # Process each column
    for col1 in columns:
        # Skip if we've already processed this column
        if col1 in processed_columns:
            continue

        # Start a new group with this column
        current_group = [col1]
        processed_columns.add(col1)

        # Compare with all other unprocessed columns
        for col2 in columns:
            if col2 in processed_columns:
                continue

            # Check if columns are equal (handling NaN values)
            try:
                is_equal = ((df[col1] == df[col2]) | 
                          (pd.isna(df[col1]) & pd.isna(df[col2]))).all()
                
                if is_equal:
                    current_group.append(col2)
                    processed_columns.add(col2)
            except:
                # Skip comparison if types are incompatible
                continue

        # Add this group to our results
        if len(current_group) > 1:  # Only add if we found duplicates
            groups.append(current_group)

    # Format results and decide which columns to drop
    results = []
    columns_to_drop = []
    
    for group in groups:
        if len(group) > 1:
            # Create a scoring system for column names
            scores = {}
            
            for col in group:
                col_lower = col.lower()
                score = 0
                
                # Prefer shorter names (often cleaner/simpler)
                score -= len(col) * 0.1
                
                # Prefer names without underscores or special characters
                score -= col.count('_') * 0.5
                score -= sum(c in '!@#$%^&*()+-={}[]|\\:;"\'<>,.?/' for c in col) * 1
                
                # Prefer common field names
                common_names = ['id', 'name', 'user', 'username', 'timestamp', 'time', 
                               'date', 'ip', 'address', 'email', 'status']
                               
                for name in common_names:
                    if name == col_lower or col_lower.endswith('_' + name) or col_lower.startswith(name + '_'):
                        score += 3
                    elif name in col_lower:
                        score += 1
                
                # Store the score
                scores[col] = score
            
            # Keep the column with the highest score
            keep_col = max(scores, key=scores.get)
            drop_cols = [col for col in group if col != keep_col]
            
            columns_to_drop.extend(drop_cols)
            results.append(f"Column '{keep_col}' equals to columns {drop_cols} - keeping '{keep_col}'")

    return results, columns_to_drop

def analyze_correlation(df, threshold=0.95):
    """
    Analyze correlation between numeric columns.
    
    Args:
        df (DataFrame): DataFrame to analyze
        threshold (float): Correlation threshold to report
        
    Returns:
        dict: Dictionary with correlation information and columns to drop
    """
    try:
        # Get only numeric columns
        numeric_df = df.select_dtypes(include=['number'])
        
        if numeric_df.shape[1] < 2:
            return {"error": "Not enough numeric columns for correlation analysis"}
            
        # Calculate correlation matrix
        corr_matrix = numeric_df.corr()
        
        # Find highly correlated pairs
        high_corr_pairs = []
        
        for i in range(len(corr_matrix.columns)):
            for j in range(i):
                if abs(corr_matrix.iloc[i, j]) >= threshold:
                    col1 = corr_matrix.columns[i]
                    col2 = corr_matrix.columns[j]
                    high_corr_pairs.append({
                        "column1": col1,
                        "column2": col2,
                        "correlation": corr_matrix.iloc[i, j]
                    })
        
        # Group highly correlated columns together
        correlated_groups = []
        
        # Start with all columns in separate groups
        remaining_cols = set(numeric_df.columns)
        
        while remaining_cols:
            # Start a new group with one column
            current_col = next(iter(remaining_cols))
            current_group = {current_col}
            remaining_cols.remove(current_col)
            
            # Flag to track if we found new connections
            expanded = True
            
            # Keep expanding the group as long as we find new connections
            while expanded:
                expanded = False
                
                # Find all columns correlated with any column in the current group
                for pair in high_corr_pairs:
                    col1, col2 = pair["column1"], pair["column2"]
                    
                    # If exactly one column is in current_group and one is in remaining_cols,
                    # add the one from remaining_cols to current_group
                    if col1 in current_group and col2 in remaining_cols:
                        current_group.add(col2)
                        remaining_cols.remove(col2)
                        expanded = True
                    elif col2 in current_group and col1 in remaining_cols:
                        current_group.add(col1)
                        remaining_cols.remove(col1)
                        expanded = True
            
            # Add the complete group to our list
            if len(current_group) > 1:  # Only add groups with at least 2 columns
                correlated_groups.append(current_group)
        
        # Determine which columns to drop from each group
        columns_to_drop = []
        
        for group in correlated_groups:
            # Convert group from set to list for consistent ordering
            group_list = list(group)
            
            # Keep the first column, drop the rest
            # This is arbitrary but consistent
            columns_to_drop.extend(group_list[1:])
        
        return {
            "numeric_columns": numeric_df.columns.tolist(),
            "high_correlation_pairs": high_corr_pairs,
            "correlated_groups": [list(group) for group in correlated_groups],
            "columns_to_drop": columns_to_drop
        }
        
    except Exception as e:
        return {"error": str(e)}

def process_chunk(chunk):
    """Process a single chunk of data"""
    try:
        # Deduplicate column names
        chunk.columns = deduplicate_columns(chunk.columns)
        
        # Drop columns with no variance
        dup_cols = [col for col in chunk.columns if chunk[col].nunique(dropna=False) == 1]
        if dup_cols:
            chunk = chunk.drop(columns=dup_cols)
            
        # Analyze correlation and drop highly correlated columns
        correlation_info = analyze_correlation(chunk, threshold=0.95)
        if "columns_to_drop" in correlation_info and correlation_info["columns_to_drop"]:
            chunk = chunk.drop(columns=correlation_info["columns_to_drop"], errors='ignore')
            
        # Find and drop duplicate columns
        _, duplicate_columns = find_duplicate_columns(chunk)
        if duplicate_columns:
            chunk = chunk.drop(columns=duplicate_columns, errors='ignore')
            
        # Drop noisy columns
        noisy_cols = detect_noisy_columns(chunk)
        if noisy_cols:
            chunk = chunk.drop(columns=noisy_cols)
            
        return chunk
    except Exception as e:
        print(f"Error processing chunk: {e}")
        return None

def process_in_parallel(file_path, chunk_size=10000, max_workers=None):
    """
    Process large CSV files in parallel using chunks
    
    Args:
        file_path (str): Path to the CSV file
        chunk_size (int): Number of rows per chunk
        max_workers (int): Maximum number of parallel workers
        
    Returns:
        DataFrame: Processed and combined DataFrame
    """
    if max_workers is None:
        max_workers = multiprocessing.cpu_count()
    
    data_logger.info(f"Starting parallel processing with {max_workers} workers")
    
    # First detect delimiter
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        sample = f.read(4096)
    delimiter = ',' if sample.count(',') > sample.count(';') else ';'
    data_logger.info(f"Detected delimiter: '{delimiter}'")
    
    # Get total rows
    total_rows = sum(1 for _ in pd.read_csv(file_path, sep=delimiter, chunksize=chunk_size, index_col=False))
    total_chunks = math.ceil(total_rows/chunk_size)
    data_logger.info(f"Total chunks to process: {total_chunks} ({total_rows} rows)")
    
    processed_chunks = []
    
    try:
        # Create a ProcessPoolExecutor for parallel processing
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Process chunks in parallel
            chunk_reader = pd.read_csv(file_path, sep=delimiter, chunksize=chunk_size, index_col=False)
            futures = [executor.submit(process_chunk, chunk) for chunk in chunk_reader]
            
            # Collect results
            for i, future in enumerate(futures):
                try:
                    chunk_result = future.result()
                    if chunk_result is not None:
                        processed_chunks.append(chunk_result)
                        data_logger.info(f"Successfully processed chunk {i+1}/{len(futures)}")
                    else:
                        data_logger.warning(f"Chunk {i+1}/{len(futures)} returned None")
                except Exception as e:
                    error_logger.error(f"Error processing chunk {i+1}: {str(e)}")
                    error_logger.error(traceback.format_exc())
                    
        if not processed_chunks:
            error_logger.error("No chunks were successfully processed")
            return None
            
        # Combine all processed chunks
        data_logger.info("Combining processed chunks...")
        final_df = pd.concat(processed_chunks, ignore_index=True)
        
        # Final deduplication across all data
        initial_rows = len(final_df)
        final_df = final_df.drop_duplicates()
        dropped_rows = initial_rows - len(final_df)
        data_logger.info(f"Removed {dropped_rows} duplicate rows from final dataset")
        
        return final_df
        
    except Exception as e:
        error_logger.error(f"Error in parallel processing: {str(e)}")
        error_logger.error(traceback.format_exc())
        return None

def preprocess_data(file_path):
    """
    Preprocess data using parallel processing for large files
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        DataFrame: Cleaned and preprocessed DataFrame
    """
    try:
        # Check file size to determine processing method
        file_size = os.path.getsize(file_path)
        large_file_threshold = 100 * 1024 * 1024  # 100MB
        
        if file_size > large_file_threshold:
            msg = f"Large file detected ({file_size/1024/1024:.2f} MB). Using parallel processing..."
            print(msg)
            data_logger.info(msg)
            return process_in_parallel(file_path)
        else:
            msg = f"Small file detected ({file_size/1024/1024:.2f} MB). Processing normally..."
            print(msg)
            data_logger.info(msg)
            
            try:
                # First, detect the delimiter by reading a few lines
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    sample = f.read(4096)  # Read first 4KB
                
                # Count occurrences of potential delimiters in the sample
                delimiters = {',': 0, ';': 0, '\t': 0, '|': 0}
                for d in delimiters:
                    delimiters[d] = sample.count(d)
                
                # Choose the most frequent delimiter
                max_count = 0
                for d, count in delimiters.items():
                    if count > max_count:
                        max_count = count
                        delimiter = d
                
                msg = f"Detected delimiter: '{delimiter}'"
                print(msg)
                data_logger.info(msg)
                
                # Load original dataset with the detected delimiter
                df_original = pd.read_csv(file_path, sep=delimiter, index_col=False)
                df = df_original.copy()
                
                msg = f"Original dataset shape: {df.shape}"
                print(msg)
                data_logger.info(msg)
                
                # Step 1: Deduplicate column names
                df.columns = deduplicate_columns(df.columns)
                msg = f"Deduplicated columns: {list(df.columns)}"
                print(msg)
                data_logger.info(msg)
                
                # Step 2: Drop columns with no variance
                dup_cols = [col for col in df.columns if df[col].nunique(dropna=False) == 1]
                if dup_cols:
                    df = df.drop(columns=dup_cols)
                    msg = f"Dropped {len(dup_cols)} columns with no variance"
                    print(msg)
                    data_logger.info(msg)
                    msg = f"Columns dropped due to no variance: {dup_cols}"
                    print(msg)
                    data_logger.info(msg)
                
                # Step 3: Use analyze_correlation to identify and drop highly correlated columns
                correlation_info = analyze_correlation(df, threshold=0.95)
                
                if "columns_to_drop" in correlation_info and correlation_info["columns_to_drop"]:
                    columns_to_drop = correlation_info["columns_to_drop"]
                    df = df.drop(columns=columns_to_drop, errors='ignore')
                    msg = f"Dropped {len(columns_to_drop)} highly correlated columns"
                    print(msg)
                    data_logger.info(msg)
                    msg = f"Columns dropped due to high correlation: {columns_to_drop}"
                    print(msg)
                    data_logger.info(msg)
                    
                    # Print and log correlation pairs
                    print("High correlation pairs:")
                    data_logger.info("High correlation pairs:")
                    for pair in correlation_info.get("high_correlation_pairs", []):
                        msg = f" - {pair['column1']} & {pair['column2']}: {pair['correlation']:.2f}"
                        print(msg)
                        data_logger.info(msg)
                    
                    # Print and log correlated groups
                    print("Correlated groups:")
                    data_logger.info("Correlated groups:")
                    for i, group in enumerate(correlation_info.get("correlated_groups", [])):
                        msg = f" - Group {i+1}: {group} - kept {group[0]}"
                        print(msg)
                        data_logger.info(msg)
                
                # Step 4: Find and drop duplicate columns automatically
                equality_results, duplicate_columns_to_drop = find_duplicate_columns(df)
                
                # Print and log equality analysis results
                for result in equality_results:
                    print(result)
                    data_logger.info(result)
                
                # Drop the identified duplicate columns
                if duplicate_columns_to_drop:
                    df = df.drop(columns=duplicate_columns_to_drop, errors='ignore')
                    msg = f"Dropped {len(duplicate_columns_to_drop)} duplicate columns"
                    print(msg)
                    data_logger.info(msg)
                    msg = f"Columns dropped due to duplication: {duplicate_columns_to_drop}"
                    print(msg)
                    data_logger.info(msg)
                
                # Step 5: Drop noisy columns
                noisy_cols = detect_noisy_columns(df)
                if noisy_cols:
                    df = df.drop(columns=noisy_cols)
                    msg = f"Dropped {len(noisy_cols)} noisy columns"
                    print(msg)
                    data_logger.info(msg)
                    msg = f"Columns dropped due to noisy content: {noisy_cols}"
                    print(msg)
                    data_logger.info(msg)
                
                # Step 6: Check for missing values
                missing_values = df.isnull().sum()
                print("Missing values in remaining columns:")
                data_logger.info("Missing values in remaining columns:")
                missing_vals = missing_values[missing_values > 0]
                print(missing_vals)
                data_logger.info(f"\n{missing_vals}")
                
                # Step 7: Check for duplicates
                duplicates = df.duplicated().sum()
                msg = f"Number of duplicate rows: {duplicates}"
                print(msg)
                data_logger.info(msg)
                
                msg = f"Final preprocessed dataset shape: {df.shape}"
                print(msg)
                data_logger.info(msg)
                msg = f"Final columns: {list(df.columns)}"
                print(msg)
                data_logger.info(msg)
                return df
                
            except Exception as e:
                error_msg = f"Error preprocessing data: {e}"
                print(error_msg)
                error_logger.error(error_msg)
                print(traceback.format_exc())
                error_logger.error(traceback.format_exc())
                return None
    except Exception as e:
        error_msg = f"Error preprocessing data: {e}"
        print(error_msg)
        error_logger.error(error_msg)
        print(traceback.format_exc())
        error_logger.error(traceback.format_exc())
        return None

# ---- Data Loading and Saving Functions ----

def register_csv_file(file_path, row_count, column_count):
    """
    Register a CSV file in the database registry.
    
    Args:
        file_path (str): Path to the CSV file
        row_count (int): Number of rows in the CSV
        column_count (int): Number of columns in the CSV
        
    Returns:
        int: File ID in the database registry
    """
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Create csv_registry table if it doesn't exist
        create_table_query = '''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[csv_registry]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[csv_registry] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                [file_name] NVARCHAR(255) NOT NULL,
                [file_path] NVARCHAR(1000) NOT NULL,
                [row_count] INT NOT NULL,
                [column_count] INT NOT NULL,
                [loaded_at] DATETIME DEFAULT GETDATE(),
                [is_processed] BIT DEFAULT 1
            )
            
            CREATE INDEX [idx_csv_registry_is_processed] ON [dbo].[csv_registry] ([is_processed])
        END
        '''
        query_logger.info(create_table_query)
        # log_db_query(query_logger, create_table_query)
        cursor.execute(create_table_query)
        
        # Extract filename from path
        file_name = os.path.basename(file_path)
        
        # Check if this file is already registered
        check_query = 'SELECT id FROM csv_registry WHERE file_path = ?'
        query_logger.info(check_query, {'file_path': file_path})
        # log_db_query(query_logger, check_query, {'file_path': file_path})
        cursor.execute(check_query, (file_path,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing entry
            update_query = '''
            UPDATE csv_registry 
            SET row_count = ?, column_count = ?, loaded_at = GETDATE(), is_processed = 1
            WHERE file_path = ?
            '''
            params = (row_count, column_count, file_path)
            query_logger.info(update_query, params)
            # log_db_query(query_logger, update_query, params)
            cursor.execute(update_query, params)
            file_id = existing[0]
            data_logger.info(f"Updated existing file registration with ID: {file_id}")
        else:
            # Insert new entry
            insert_query = '''
            INSERT INTO csv_registry (file_name, file_path, row_count, column_count, is_processed)
            VALUES (?, ?, ?, ?, 1)
            '''
            params = (file_name, file_path, row_count, column_count)
            query_logger.info(insert_query)
            # log_db_query(query_logger, insert_query, params)
            cursor.execute(insert_query, params)
            
            # Get the ID of the inserted file
            cursor.execute('SELECT @@IDENTITY')
            file_id = cursor.fetchone()[0]
            data_logger.info(f"Created new file registration with ID: {file_id}")
        
        db.commit()
        return file_id
    
    except Exception as e:
        error_msg = f"Error registering CSV file: {str(e)}"
        print(error_msg)
        error_logger.info(f"File: {file_path}, Rows: {row_count}, Columns: {column_count}")
        return None

def bulk_insert_to_db(df, table_name, conn):
    """Perform bulk insert of DataFrame to SQL Server table"""
    data_logger.info(f"Starting bulk insert for table: {table_name}")
    start_time = time.time()
    
    try:
        # First get the structure of the target table
        cursor = conn.cursor()
        schema_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        query_logger.info(schema_query)
        # log_db_query(query_logger, schema_query)
        cursor.execute(schema_query)
        target_columns = cursor.fetchall()
        data_logger.info(f"Found {len(target_columns)} columns in target table")
        
        # Create temporary table structure
        temp_table = f"#temp_{table_name}"
        column_definitions = []
        
        # Skip the identity column (id) when inserting
        target_columns = [col for col in target_columns if col[0].lower() != 'id']
        
        # Prepare column definitions
        for col_name, data_type, max_length in target_columns:
            df_col = next((c for c in df.columns if c.lower().replace(' ', '_').replace('-', '_').replace('.', '_') == col_name.lower()), None)
            
            if df_col is None and col_name.lower() != 'processed_at':
                error_msg = f"Required column {col_name} not found in DataFrame"
                error_logger.error(error_msg)
                raise ValueError(error_msg)
            
            sql_type = get_sql_type(data_type, max_length, df_col, df)
            column_definitions.append(f"[{col_name}] {sql_type}")
        
        create_temp_table = f"""
        CREATE TABLE {temp_table} (
            {', '.join(column_definitions)}
        )
        """
        query_logger.info(create_temp_table)
        # log_db_query(query_logger, create_temp_table)
        cursor.execute(create_temp_table)
        
        # Prepare insert statement
        insert_columns = [col[0] for col in target_columns if col[0].lower() not in ('id', 'processed_at')]
        placeholders = ','.join(['?' for _ in insert_columns])
        column_list = ','.join([f'[{col}]' for col in insert_columns])
        insert_sql = f"INSERT INTO {temp_table} ({column_list}) VALUES ({placeholders})"
        query_logger.info(insert_sql)
        # log_db_query(query_logger, f"Insert statement template: {insert_sql}")
        
        # Prepare and insert data in batches
        rows = prepare_rows_for_insert(df, insert_columns)
        batch_size = 10000
        total_batches = (len(rows) - 1) // batch_size + 1
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            batch_num = i // batch_size + 1
            data_logger.info(f"Inserting batch {batch_num}/{total_batches} ({len(batch)} rows)")
            cursor.executemany(insert_sql, batch)
            conn.commit()
        
        # Copy data from temp table to actual table
        final_insert = f"""
        INSERT INTO {table_name} ({column_list})
        SELECT {column_list} FROM {temp_table}
        """
        query_logger.info(final_insert)
        # log_db_query(query_logger, final_insert)
        cursor.execute(final_insert)
        conn.commit()
        
        duration = time.time() - start_time
        data_logger.info(f"Completed bulk insert to {table_name} in {duration}")
        
    except Exception as e:
        error_logger.info(f"Error during bulk insert to {table_name}")
        raise
    finally:
        try:
            drop_query = f"DROP TABLE {temp_table}"
            query_logger.info(drop_query)
            # log_db_query(query_logger, drop_query)
            cursor.execute(drop_query)
            conn.commit()
        except Exception as e:
            error_logger.info(f"Failed to drop temporary table {temp_table}")

def get_sql_type(data_type, max_length, df_col, df):
    """Helper function to determine SQL type for a column"""
    if data_type in ('float', 'real'):
        return "FLOAT"
    elif data_type in ('bigint', 'int'):
        return data_type.upper()
    elif data_type == 'bit':
        return "BIT"
    elif data_type in ('datetime', 'datetime2'):
        return "DATETIME2"
    else:
        if max_length == -1:
            return "NVARCHAR(MAX)"
        else:
            return f"NVARCHAR({max_length})"

def prepare_rows_for_insert(df, insert_columns):
    """Helper function to prepare rows for bulk insert"""
    rows = []
    df_columns = [c for c in df.columns if c.lower().replace(' ', '_').replace('-', '_').replace('.', '_') in 
                 [col.lower() for col in insert_columns]]
    
    for _, row in df[df_columns].iterrows():
        row_values = []
        for val in row:
            if pd.isna(val):
                row_values.append(None)
            elif isinstance(val, (np.int64, np.int32)):
                row_values.append(int(val))
            elif isinstance(val, (np.float64, np.float32)):
                row_values.append(float(val))
            elif isinstance(val, bool):
                row_values.append(int(val))
            elif isinstance(val, pd.Timestamp):
                row_values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                row_values.append(str(val))
        rows.append(tuple(row_values))
    
    return rows
def save_processed_data_to_db(processed_df, file_path, batch_size=10000, target_table=None):
    """
    Save preprocessed data directly to a database table.
    
    Args:
        processed_df: Preprocessed DataFrame
        file_path: Original CSV file path
        batch_size: Size of batches for bulk insert
        target_table: Optional target table name. If provided, data will be inserted into this existing table
    """
    start_time = time.time()
    processing_times = {}
    
    try:
        db = get_db()
        cursor = db.cursor()
        
        if target_table:
            # Check if target table exists
            cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{target_table}'
            """)
            
            if cursor.fetchone()[0] == 0:
                error_msg = f"Target table '{target_table}' does not exist"
                error_logger.error(error_msg)
                raise ValueError(error_msg)
                
            table_name = target_table
            msg = f"Using existing table: {table_name}"
            print(msg)
            data_logger.info(msg)
        else:
            # Original logic for creating new table
            base_filename = os.path.splitext(os.path.basename(file_path))[0]
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_filename)
            if not table_name[0].isalpha():
                table_name = "tbl_" + table_name
            msg = f"Creating new table: {table_name}"
            print(msg)
            data_logger.info(msg)
        
        # Time table creation preparation
        table_prep_start = time.time()
        
        # Get columns from the preprocessed DataFrame
        processed_columns = processed_df.columns.tolist()
        msg = f"Saving data with {len(processed_columns)} columns: {processed_columns}"
        print(msg)
        data_logger.info(msg)
        
        # Create a map of column names to their SAFE SQL names
        column_mapping = {}
        timestamp_pattern = re.compile(r'time|date|timestamp', re.IGNORECASE)
        
        for col in processed_columns:
            safe_col = col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            column_mapping[col] = safe_col
        
        processing_times['table_preparation'] = time.time() - table_prep_start
        
        if not target_table:
            # Only create new table if not using existing table
            # Time table drop if exists
            drop_start = time.time()
            cursor.execute(f"""
            IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))
            DROP TABLE [dbo].[{table_name}]
            """)
            db.commit()
            data_logger.info(f"Dropped existing table: {table_name}")
            processing_times['table_drop'] = time.time() - drop_start
            
            # Time schema creation
            schema_start = time.time()
            # Create table with appropriate column types
            column_definitions = ["[id] INT IDENTITY(1,1) PRIMARY KEY"]
            
            for col in processed_columns:
                safe_col = column_mapping[col]
                col_type = processed_df[col].dtype
                
                if col_type == 'float64' or col_type == 'float32':
                    sql_type = "FLOAT"
                elif col_type == 'int64' or col_type == 'int32':
                    sql_type = "BIGINT"
                elif col_type == 'bool':
                    sql_type = "BIT"
                elif col_type == 'datetime64[ns]':
                    sql_type = "DATETIME2"
                else:
                    # For string columns, determine appropriate length
                    if col_type == 'object':
                        max_length = processed_df[col].astype(str).str.len().max()
                        if max_length < 50:
                            sql_type = f"NVARCHAR(50)"
                        elif max_length < 255:
                            sql_type = f"NVARCHAR(255)"
                        else:
                            sql_type = "NVARCHAR(MAX)"
                    else:
                        sql_type = "NVARCHAR(255)"
                
                column_definitions.append(f"[{safe_col}] {sql_type}")
                data_logger.info(f"Column '{safe_col}' mapped to SQL type: {sql_type}")
            
            # Add processed_at timestamp column
            column_definitions.append("[processed_at] DATETIME2 DEFAULT GETDATE()")
            
            create_table_sql = f"""
            CREATE TABLE [dbo].[{table_name}] (
                {', '.join(column_definitions)}
            )
            """
            cursor.execute(create_table_sql)
            db.commit()
            data_logger.info(f"Created new table: {table_name}")
            processing_times['schema_creation'] = time.time() - schema_start
        else:
            processing_times['table_drop'] = 0
            processing_times['schema_creation'] = 0

        # Time bulk insert operation
        insert_start = time.time()
        msg = "Starting bulk insert operation..."
        print(msg)
        data_logger.info(msg)
        bulk_insert_to_db(processed_df, table_name, db)
        processing_times['bulk_insert'] = time.time() - insert_start
        
        # Calculate total time
        total_time = time.time() - start_time
        
        # Print and log timing summary
        print("\nProcessing Time Summary:")
        data_logger.info("\nProcessing Time Summary:")
        print(f"{'Operation':<20} {'Time (seconds)':<15} {'Percentage':<10}")
        print("-" * 45)
        for operation, duration in processing_times.items():
            percentage = (duration / total_time) * 100
            msg = f"{operation:<20} {duration:,.2f}s{percentage:>11.1f}%"
            print(msg)
            data_logger.info(msg)
        print("-" * 45)
        final_msg = f"{'Total time':<20} {total_time:,.2f}s{'100.0':>11}%\n"
        print(final_msg)
        data_logger.info(final_msg)

        return {
            "processed_count": len(processed_df),
            "skipped_count": 0,
            "error_count": 0,
            "total_count": len(processed_df),
            "processed_table": table_name,
            "processing_times": {
                "table_preparation": round(processing_times['table_preparation'], 2),
                "table_drop": round(processing_times['table_drop'], 2),
                "schema_creation": round(processing_times['schema_creation'], 2),
                "bulk_insert": round(processing_times['bulk_insert'], 2),
                "total_time": round(total_time, 2)
            }
        }
        
    except Exception as e:
        error_msg = f"Error saving processed data to DB: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
        print(traceback.format_exc())
        error_logger.error(traceback.format_exc())
        return {
            "error": str(e),
            "processed_count": 0,
            "skipped_count": 0,
            "error_count": 0,
            "total_count": 0
        }

def process_and_load_csv(file_path, batch_size=1000, max_rows=None, target_table=None):
    """
    Process a CSV file and load it directly into a processed table.
    
    Args:
        file_path (str): Path to the CSV file
        batch_size (int): Number of rows to process in a batch
        max_rows (int): Maximum number of rows to process, None for all
        target_table (str): Optional target table name. If provided, data will be inserted into this existing table
        
    Returns:
        dict: Processing statistics
    """
    try:
        data_logger.info(f"Starting to process CSV file: {file_path}")
        data_logger.info(f"Parameters: batch_size={batch_size}, max_rows={max_rows}, target_table={target_table}")
        
        # Check if the CSV file exists
        if not os.path.exists(file_path):
            error_msg = f"CSV file {file_path} not found. Please check if the file exists."
            error_logger.error(error_msg)
            return {
                "error": error_msg,
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Use the preprocess_data function to preprocess the data from the CSV file
        data_logger.info(f"Preprocessing CSV file: {file_path}")
        processed_df = preprocess_data(file_path)
        
        if processed_df is None:
            error_msg = "Failed to preprocess file"
            error_logger.error(error_msg)
            return {
                "error": error_msg,
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Apply max_rows limit if specified
        if max_rows is not None and max_rows > 0:
            processed_df = processed_df.head(max_rows)
            data_logger.info(f"Limited to {max_rows} rows as requested")
        
        # Register the CSV file in the database
        data_logger.info(f"Registering CSV file in database: {file_path}")
        file_id = register_csv_file(
            file_path, 
            len(processed_df), 
            len(processed_df.columns)
        )
        
        if file_id is None:
            error_msg = "Failed to register CSV file in the database"
            error_logger.error(error_msg)
            return {
                "error": error_msg,
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Save the processed data to the database
        data_logger.info("Starting to save processed data to database")
        result = save_processed_data_to_db(processed_df, file_path, batch_size, target_table)
        
        # Add file_id to the result
        result["file_id"] = file_id
        
        if "error" in result:
            error_logger.error(f"Error saving data to database: {result['error']}")
        else:
            data_logger.info(f"Successfully processed {result['processed_count']} records into {result.get('processed_table')}")
            data_logger.info(f"Processing times: {result.get('processing_times', {})}")
        
        return result
        
    except Exception as e:
        error_msg = f"Error processing and loading CSV: {str(e)}"
        error_logger.error(error_msg)
        error_logger.error(traceback.format_exc())
        return {
            "error": str(e),
            "processed_count": 0,
            "skipped_count": 0,
            "error_count": 0,
            "total_count": 0
        }
    
# Load and process a CSV file
@data_bp.route('/load', methods=['POST'])
@require_api_key
def load_data_endpoint():
    """
    Load and process a CSV file, then save directly to the database.
    
    Requires an admin API key.
    """
    if not is_admin(request.api_user):
        data_logger.warning(f"Unauthorized access attempt from user: {request.api_user}")
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403    # Parse parameters
    try:
        # Get parameters from request using helper function
        file_path, batch_size, max_rows, target_table = parse_request_params(request)

        # Log request parameters
        data_logger.info(f"Data load request - file_path: {file_path}, batch_size: {batch_size}, max_rows: {max_rows}, target_table: {target_table}")

        # Validate required parameters
        if not file_path:
            error_logger.error("Missing required parameter: file_path")
            return jsonify({
                "error": "file_path required in request body, form data, or query parameters",
                "help": "Set Content-Type to application/json and provide {'file_path': 'your/file/path.csv'} in the request body"
            }), 400

        # Process and load the CSV file
        result = process_and_load_csv(file_path, batch_size, max_rows, target_table)
        
        if "error" in result:
            error_logger.error(f"Failed to process CSV file: {result['error']}")
            return jsonify({
                "success": False,
                "error": result["error"],
                "stats": {
                    "processed": result["processed_count"],
                    "skipped": result["skipped_count"],
                    "errors": result["error_count"],
                    "total": result["total_count"]
                }
            }), 500
            
        data_logger.info(f"Successfully processed CSV file: {file_path}")
        return jsonify({
            "success": True,
            "message": f"Successfully processed {result['processed_count']} records into {result.get('processed_table')}",
            "stats": {
                "processed": result["processed_count"],
                "skipped": result["skipped_count"],
                "errors": result["error_count"],
                "total": result["total_count"]
            },
            "file_id": result.get("file_id"),
            "processed_table": result.get('processed_table'),
            "processing_times": result.get('processing_times', {})
        })

    except Exception as e:
        error_msg = f"Unexpected error in load_data_endpoint: {str(e)}"
        error_logger.error(error_msg)
        error_logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

# Get file details
@data_bp.route('/file-details/<string:table_name>', methods=['GET'])
@require_api_key
def get_file_details(table_name):
    """Get details about a specific table."""
    if not is_admin(request.api_user):
        data_logger.warning(f"Unauthorized access attempt from user: {request.api_user}")
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    try:
        cursor = get_cursor()
        
        # Sanitize the table name for SQL safety
        processed_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        if not processed_table_name[0].isalpha():
            processed_table_name = "tbl_" + processed_table_name
            
        # Check if table exists and get its details        
        table_info_query = f'''
        SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{processed_table_name}'
        '''
        query_logger.info(table_info_query)
        # log_db_query(query_logger, table_info_query)
        cursor.execute(table_info_query)
        table_info = cursor.fetchone()
        
        if not table_info:
            error_logger.error(f"Table not found: {processed_table_name}")
            return jsonify({"error": "Table not found"}), 404
            
        # Get column information
        column_info_query = f'''
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{processed_table_name}'
        ORDER BY ORDINAL_POSITION
        '''
        query_logger.info(column_info_query)
        # log_db_query(query_logger, column_info_query)
        cursor.execute(column_info_query)
        
        columns = []
        for col in cursor.fetchall():
            column_info = {
                "name": col[0],
                "type": col[1],
                "max_length": col[2]
            }
            columns.append(column_info)
            
        # Get row count
        count_query = f"SELECT COUNT(*) FROM [{processed_table_name}]"
        query_logger.info(count_query)
        # log_db_query(query_logger, count_query)
        cursor.execute(count_query)
        total_rows = cursor.fetchone()[0]
        
        response_data = {
            "table_name": processed_table_name,
            "schema": table_info[0],
            "table_type": table_info[2],
            "total_rows": total_rows,
            "columns": columns,
            "column_count": len(columns),
            "description": f"Details for table {processed_table_name}"
        }
        
        data_logger.info(f"Successfully retrieved details for table: {processed_table_name}")
        return jsonify(response_data)
        
    except Exception as e:
        error_logger.info(f"Error getting details for table: {table_name}")
        return jsonify({"error": str(e)}), 500

# Get processed table data
@data_bp.route('/processed-table-data/<string:table_name>', methods=['GET'])
@require_api_key
def get_processed_sample(table_name):
    """Get all data from a processed table using table name."""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    try:
        cursor = get_cursor()
        
        # Sanitize the table name for SQL safety
        processed_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        if not processed_table_name[0].isalpha():
            processed_table_name = "tbl_" + processed_table_name
            
        # Check if table exists
        cursor.execute(f'''
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{processed_table_name}'
        ''')
        
        if cursor.fetchone()[0] == 0:
            return jsonify({"error": f"Processed table for table name {table_name} not found"}), 404
        
        # Get column names
        cursor.execute(f'''
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{processed_table_name}'
        ORDER BY ORDINAL_POSITION
        ''')
        
        columns = [row[0] for row in cursor.fetchall()]
        
        # Get sample data (limit to 50 rows or use limit param)
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        cursor.execute(f'''
        SELECT *
        FROM [{processed_table_name}]
        ORDER BY id
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
        ''')
        
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        all_data = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                # Handle datetime conversion
                if isinstance(row[i], datetime):
                    row_dict[col] = row[i].isoformat()
                else:
                    row_dict[col] = row[i]
            all_data.append(row_dict)
        
        # Get total record count
        cursor.execute(f"SELECT COUNT(*) FROM [{processed_table_name}]")
        total_records = cursor.fetchone()[0]
        return jsonify({
            "table_name": processed_table_name,
            "columns": columns,
            "data": all_data,
            "total_records": total_records,
            "limit": limit,
            "offset": offset,
            "description": f"Data from processed table {processed_table_name}"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get stats about loaded files
@data_bp.route('/stats', methods=['GET'])
@require_api_key
def csv_file_stats():
    """Get statistics about loaded CSV files."""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
        
    try:
        cursor = get_cursor()
        
        # Get total files
        total_query = 'SELECT COUNT(*) FROM csv_registry'
        query_logger.info(total_query)
        # log_db_query(query_logger, total_query)
        cursor.execute(total_query)
        total_files = cursor.fetchone()[0]
        
        # Get processed vs unprocessed
        status_query = 'SELECT is_processed, COUNT(*) FROM csv_registry GROUP BY is_processed'
        query_logger.info(status_query)
        # log_db_query(query_logger, status_query)
        cursor.execute(status_query)
        
        processed_files = 0
        unprocessed_files = 0
        
        for row in cursor.fetchall():
            if row[0]:  # is_processed = 1
                processed_files = row[1]
            else:  # is_processed = 0
                unprocessed_files = row[1]
        
        # Get file details
        file_details_query = '''
        SELECT id, file_name, file_path, row_count, column_count, loaded_at, is_processed
        FROM csv_registry
        ORDER BY loaded_at DESC
        '''
        query_logger.info(file_details_query)
        # log_db_query(query_logger, file_details_query)
        cursor.execute(file_details_query)
        
        files = []
        for row in cursor.fetchall():
            files.append({
                "id": row[0],
                "file_name": row[1],
                "file_path": row[2],
                "row_count": row[3],
                "column_count": row[4],
                "loaded_at": row[5].isoformat() if row[5] else None,
                "is_processed": bool(row[6])
            })
        
        # Get processed tables
        tables_query = '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'processed_data_%'
        '''
        query_logger.info(tables_query)
        # log_db_query(query_logger, tables_query)
        cursor.execute(tables_query)
        
        processed_tables = [row[0] for row in cursor.fetchall()]
        
        response_data = {
            "total_files": total_files,
            "processed_files": processed_files,
            "unprocessed_files": unprocessed_files,
            "files": files,
            "processed_tables": processed_tables,
            "processed_tables_count": len(processed_tables)
        }
        
        data_logger.info(f"Successfully retrieved stats. Total files: {total_files}, Processed: {processed_files}, Unprocessed: {unprocessed_files}")
        return jsonify(response_data)
        
    except Exception as e:
        error_logger.info(f"Error retrieving CSV file stats")
        return jsonify({"error": str(e)}), 500

def parse_request_params(request):
    """Parse request parameters from different sources"""
    file_path = None
    batch_size = 1000
    max_rows = None
    target_table = None

    data_logger.info("Parsing request parameters...")

    # Check JSON data
    if request.is_json:
        data_logger.info("Found JSON data in request")
        data = request.get_json()
        if data:
            file_path = data.get('file_path')
            batch_size = data.get('batch_size', 1000)
            max_rows = data.get('max_rows')
            target_table = data.get('target_table')
            data_logger.info(f"Parsed parameters from JSON: file_path={file_path}, batch_size={batch_size}, max_rows={max_rows}, target_table={target_table}")
    
    # Check form data
    if file_path is None and request.form:
        data_logger.info("Found form data in request")
        file_path = request.form.get('file_path')
        batch_size = int(request.form.get('batch_size', 1000))
        max_rows = int(request.form.get('max_rows')) if 'max_rows' in request.form else None
        target_table = request.form.get('target_table')
        data_logger.info(f"Parsed parameters from form data: file_path={file_path}, batch_size={batch_size}, max_rows={max_rows}, target_table={target_table}")
    
    # Check query parameters
    if file_path is None and request.args:
        data_logger.info("Found query parameters in request")
        file_path = request.args.get('file_path')
        batch_size = int(request.args.get('batch_size', 1000))
        max_rows = int(request.args.get('max_rows')) if 'max_rows' in request.args else None
        target_table = request.args.get('target_table')
        data_logger.info(f"Parsed parameters from query string: file_path={file_path}, batch_size={batch_size}, max_rows={max_rows}, target_table={target_table}")
    
    if file_path is None:
        data_logger.warning("No file_path parameter found in request")
    
    return file_path, batch_size, max_rows, target_table