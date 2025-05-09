# data_loading.py
import pandas as pd
import json
from flask import Blueprint, jsonify, request, g, current_app
from functools import wraps
import pyodbc
import numpy as np
from datetime import datetime
import os
import re
import collections
import traceback
from helper import get_db, get_cursor, dict_from_row, require_api_key, is_admin
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import math

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

    print(f"Noisy columns: {noisy_columns}")
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
    
    # First detect delimiter
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        sample = f.read(4096)
    delimiter = ',' if sample.count(',') > sample.count(';') else ';'
    
    # Get total rows
    total_rows = sum(1 for _ in pd.read_csv(file_path, sep=delimiter, chunksize=chunk_size))
    print(f"Total chunks to process: {math.ceil(total_rows/chunk_size)}")
    
    processed_chunks = []
    
    try:
        # Create a ProcessPoolExecutor for parallel processing
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Process chunks in parallel
            chunk_reader = pd.read_csv(file_path, sep=delimiter, chunksize=chunk_size)
            futures = [executor.submit(process_chunk, chunk) for chunk in chunk_reader]
            
            # Collect results
            for i, future in enumerate(futures):
                try:
                    chunk_result = future.result()
                    if chunk_result is not None:
                        processed_chunks.append(chunk_result)
                    print(f"Processed chunk {i+1}/{len(futures)}")
                except Exception as e:
                    print(f"Error processing chunk {i+1}: {e}")
                    
        if not processed_chunks:
            return None
            
        # Combine all processed chunks
        final_df = pd.concat(processed_chunks, ignore_index=True)
        
        # Final deduplication across all data
        final_df = final_df.drop_duplicates()
        
        return final_df
        
    except Exception as e:
        print(f"Error in parallel processing: {e}")
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
            print(f"Large file detected ({file_size/1024/1024:.2f} MB). Using parallel processing...")
            return process_in_parallel(file_path)
        else:
            print(f"Small file detected ({file_size/1024/1024:.2f} MB). Processing normally...")
            # Original processing code for small files
            try:
                # First, detect the delimiter by reading a few lines
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    sample = f.read(4096)  # Read first 4KB
                
                # Check for common delimiters in the sample
                delimiter = ','  # Default delimiter
                
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
                
                print(f"Detected delimiter: '{delimiter}'")
                
                # Load original dataset with the detected delimiter
                df_original = pd.read_csv(file_path, sep=delimiter)
                df = df_original.copy()
                
                print(f"Original dataset shape: {df.shape}")
                
                # Step 1: Deduplicate column names
                df.columns = deduplicate_columns(df.columns)
                print(f"Deduplicated columns: {list(df.columns)}")
                
                # Step 2: Drop columns with no variance
                dup_cols = [col for col in df.columns if df[col].nunique(dropna=False) == 1]
                if dup_cols:
                    df = df.drop(columns=dup_cols)
                    print(f"Dropped {len(dup_cols)} columns with no variance")
                    print(f"Columns dropped due to no variance: {dup_cols}")
                
                # Step 3: Use analyze_correlation to identify and drop highly correlated columns
                correlation_info = analyze_correlation(df, threshold=0.95)
                
                if "columns_to_drop" in correlation_info and correlation_info["columns_to_drop"]:
                    columns_to_drop = correlation_info["columns_to_drop"]
                    df = df.drop(columns=columns_to_drop, errors='ignore')
                    print(f"Dropped {len(columns_to_drop)} highly correlated columns")
                    print(f"Columns dropped due to high correlation: {columns_to_drop}")
                    
                    # Print the correlation pairs for reference
                    print("High correlation pairs:")
                    for pair in correlation_info.get("high_correlation_pairs", []):
                        print(f" - {pair['column1']} & {pair['column2']}: {pair['correlation']:.2f}")
                    
                    # Print the correlated groups for reference
                    print("Correlated groups:")
                    for i, group in enumerate(correlation_info.get("correlated_groups", [])):
                        print(f" - Group {i+1}: {group} - kept {group[0]}")
                
                # Step 4: Find and drop duplicate columns automatically
                equality_results, duplicate_columns_to_drop = find_duplicate_columns(df)
                
                # Print the equality analysis results
                for result in equality_results:
                    print(result)
                
                # Drop the identified duplicate columns
                if duplicate_columns_to_drop:
                    df = df.drop(columns=duplicate_columns_to_drop, errors='ignore')
                    print(f"Dropped {len(duplicate_columns_to_drop)} duplicate columns")
                    print(f"Columns dropped due to duplication: {duplicate_columns_to_drop}")
                
                # Step 5: Drop noisy columns
                noisy_cols = detect_noisy_columns(df)
                if noisy_cols:
                    df = df.drop(columns=noisy_cols)
                    print(f"Dropped {len(noisy_cols)} noisy columns")
                    print(f"Columns dropped due to noisy content: {noisy_cols}")
                
                # Step 6: Check for missing values
                missing_values = df.isnull().sum()
                print("Missing values in remaining columns:")
                print(missing_values[missing_values > 0])
                
                # Step 7: Check for duplicates
                duplicates = df.duplicated().sum()
                print(f"Number of duplicate rows: {duplicates}")
                
                print(f"Final preprocessed dataset shape: {df.shape}")
                print(f"Final columns: {list(df.columns)}")
                return df
                
            except Exception as e:
                print(f"Error preprocessing data: {e}")
                traceback.print_exc()
                return None
    except Exception as e:
        print(f"Error preprocessing data: {e}")
        traceback.print_exc()
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
        cursor.execute('''
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
        ''')
        
        # Extract filename from path
        file_name = os.path.basename(file_path)
        
        # Check if this file is already registered
        cursor.execute('SELECT id FROM csv_registry WHERE file_path = ?', (file_path,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing entry
            cursor.execute('''
            UPDATE csv_registry 
            SET row_count = ?, column_count = ?, loaded_at = GETDATE(), is_processed = 1
            WHERE file_path = ?
            ''', (row_count, column_count, file_path))
            file_id = existing[0]
        else:
            # Insert new entry
            cursor.execute('''
            INSERT INTO csv_registry (file_name, file_path, row_count, column_count, is_processed)
            VALUES (?, ?, ?, ?, 1)
            ''', (file_name, file_path, row_count, column_count))
            
            # Get the ID of the inserted file
            cursor.execute('SELECT @@IDENTITY')
            file_id = cursor.fetchone()[0]
        
        db.commit()
        return file_id
    
    except Exception as e:
        print(f"Error registering CSV file: {e}")
        traceback.print_exc()
        return None

def bulk_insert_to_db(df, table_name, conn):
    """
    Perform bulk insert of DataFrame to SQL Server table
    """
    # First get the structure of the target table
    cursor = conn.cursor()
    cursor.execute(f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """)
    target_columns = cursor.fetchall()
    
    # Map DataFrame columns to match target table structure
    df_processed = df.copy()
    
    # Format timestamp columns before creating temp table
    time_pattern = re.compile(r'time|date|timestamp', re.IGNORECASE)
    for col in df_processed.columns:
        if time_pattern.search(col):
            # Handle various timestamp formats
            if df_processed[col].dtype == 'object':
                # Remove quotes if present
                df_processed[col] = df_processed[col].str.replace("'", "")
                # Convert to datetime
                df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')
            # Ensure consistent timezone format
            if df_processed[col].dtype == 'datetime64[ns]':
                df_processed[col] = df_processed[col].dt.tz_localize(None)
    
    # Create temporary table structure
    temp_table = f"#temp_{table_name}"
    column_definitions = []
    
    # Skip the identity column (id) when inserting
    target_columns = [col for col in target_columns if col[0].lower() != 'id']
    
    # Prepare column definitions for temp table and process data
    for col_name, data_type, max_length in target_columns:
        # Find corresponding DataFrame column
        df_col = next((c for c in df.columns if c.lower().replace(' ', '_').replace('-', '_').replace('.', '_') == col_name.lower()), None)
        
        if df_col is None:
            if col_name.lower() == 'processed_at':
                continue
            else:
                raise ValueError(f"Required column {col_name} not found in DataFrame")
        
        # Add column definition
        if data_type in ('float', 'real'):
            sql_type = "FLOAT"
        elif data_type in ('bigint', 'int'):
            sql_type = data_type.upper()
        elif data_type == 'bit':
            sql_type = "BIT"
        elif data_type in ('datetime', 'datetime2'):
            sql_type = "DATETIME2"
            # Additional datetime formatting if needed
            if df_col in df_processed and df_processed[df_col].dtype == 'datetime64[ns]':
                df_processed[df_col] = df_processed[df_col].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            if max_length == -1:
                sql_type = "NVARCHAR(MAX)"
            else:
                sql_type = f"NVARCHAR({max_length})"
        
        column_definitions.append(f"[{col_name}] {sql_type}")
    
    create_temp_table = f"""
    CREATE TABLE {temp_table} (
        {', '.join(column_definitions)}
    )
    """
    cursor.execute(create_temp_table)
    
    # Enable fast_executemany for better performance
    cursor.fast_executemany = True
    
    try:
        # Get final list of columns (excluding id and processed_at)
        insert_columns = [col[0] for col in target_columns if col[0].lower() not in ('id', 'processed_at')]
        
        # Prepare the insert statement with explicit column names
        placeholders = ','.join(['?' for _ in insert_columns])
        column_list = ','.join([f'[{col}]' for col in insert_columns])
        insert_sql = f"INSERT INTO {temp_table} ({column_list}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples for bulk insert
        rows = []
        df_columns = [c for c in df_processed.columns if c.lower().replace(' ', '_').replace('-', '_').replace('.', '_') in [col.lower() for col in insert_columns]]
        
        for _, row in df_processed[df_columns].iterrows():
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
        
        # Perform bulk insert in batches
        batch_size = 10000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            print(f"Inserted batch {i//batch_size + 1} of {(len(rows)-1)//batch_size + 1}")
        
        # Copy data from temp table to actual table
        cursor.execute(f"""
        INSERT INTO {table_name} ({column_list})
        SELECT {column_list} FROM {temp_table}
        """)
        conn.commit()
        
    finally:
        # Always try to drop the temporary table
        try:
            cursor.execute(f"DROP TABLE {temp_table}")
            conn.commit()
        except:
            pass  # Ignore errors when dropping temp table

def save_processed_data_to_db(processed_df, file_id, batch_size=10000):
    """
    Save preprocessed data directly to a new database table using bulk insert.
    """
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Get columns from the preprocessed DataFrame
        processed_columns = processed_df.columns.tolist()
        print(f"Saving data with {len(processed_columns)} columns: {processed_columns}")
        
        # Create a map of column names to their SAFE SQL names
        column_mapping = {}
        timestamp_pattern = re.compile(r'time|date|timestamp', re.IGNORECASE)
        
        for col in processed_columns:
            safe_col = col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            column_mapping[col] = safe_col
        
        # Create the processed_data table
        table_name = f"processed_data_{file_id}"
        
        cursor.execute(f"""
        IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))
        DROP TABLE [dbo].[{table_name}]
        """)
        db.commit()
        
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
                        sql_type = f"NVARCHAR({max(max_length * 2, 50)})"
                    elif max_length < 255:
                        sql_type = "NVARCHAR(255)"
                    else:
                        sql_type = "NVARCHAR(MAX)"
                else:
                    sql_type = "NVARCHAR(255)"
            
            column_definitions.append(f"[{safe_col}] {sql_type}")
        
        # Add processed_at timestamp column
        column_definitions.append("[processed_at] DATETIME2 DEFAULT GETDATE()")
        
        create_table_sql = f"""
        CREATE TABLE [dbo].[{table_name}] (
            {', '.join(column_definitions)}
        )
        """
        cursor.execute(create_table_sql)
        db.commit()

        # Perform bulk insert
        print("Starting bulk insert operation...")
        bulk_insert_to_db(processed_df, table_name, db)
        print("Bulk insert completed successfully")

        return {
            "processed_count": len(processed_df),
            "skipped_count": 0,
            "error_count": 0,
            "total_count": len(processed_df),
            "processed_table": table_name
        }
        
    except Exception as e:
        print(f"Error saving processed data to DB: {e}")
        traceback.print_exc()
        return {
            "error": str(e),
            "processed_count": 0,
            "skipped_count": 0,
            "error_count": 0,
            "total_count": 0
        }

def process_and_load_csv(file_path, batch_size=1000, max_rows=None):
    """
    Process a CSV file and load it directly into a processed table.
    
    Args:
        file_path (str): Path to the CSV file
        batch_size (int): Number of rows to process in a batch
        max_rows (int): Maximum number of rows to process, None for all
        
    Returns:
        dict: Processing statistics
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
        
        # Use the preprocess_data function to preprocess the data from the CSV file
        print(f"Preprocessing CSV file: {file_path}")
        processed_df = preprocess_data(file_path)
        
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
        
        # Register the CSV file in the database
        file_id = register_csv_file(
            file_path, 
            len(processed_df), 
            len(processed_df.columns)
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
        result = save_processed_data_to_db(processed_df, file_id, batch_size)
        
        # Add file_id to the result
        result["file_id"] = file_id
        
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
    
# Load and process a CSV file
@data_bp.route('/load', methods=['POST'])
@require_api_key
def load_data_endpoint():
    """
    Load and process a CSV file, then save directly to the database.
    
    Requires an admin API key.
    """
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    # Try to get file_path from different request formats
    file_path = None
    batch_size = 1000
    max_rows = None
    
    # Check if JSON data
    if request.is_json:
        data = request.get_json()
        if data:
            if 'file_path' in data:
                file_path = data['file_path']
            if 'batch_size' in data:
                batch_size = data['batch_size']
            if 'max_rows' in data:
                max_rows = data['max_rows']
    
    # Check form data if file_path not found
    if file_path is None and request.form:
        if 'file_path' in request.form:
            file_path = request.form['file_path']
        if 'batch_size' in request.form:
            batch_size = int(request.form['batch_size'])
        if 'max_rows' in request.form:
            max_rows = int(request.form['max_rows'])
    
    # Check query params if still not found
    if file_path is None and 'file_path' in request.args:
        file_path = request.args.get('file_path')
        if 'batch_size' in request.args:
            batch_size = int(request.args.get('batch_size'))
        if 'max_rows' in request.args:
            max_rows = int(request.args.get('max_rows'))
    
    # If still not found, return error
    if file_path is None:
        return jsonify({
            "error": "file_path required in request body, form data, or query parameters",
            "help": "Set Content-Type to application/json and provide {'file_path': 'your/file/path.csv'} in the request body"
        }), 400
        
    try:
        # Process and load the CSV file directly
        result = process_and_load_csv(file_path, batch_size, max_rows)
        
        if "error" in result:
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
        else:
            return jsonify({
                "success": True,
                "message": f"Successfully processed {result['processed_count']} records into {result.get('processed_table', 'processed_data_' + str(result.get('file_id', 0)))}",
                "stats": {
                    "processed": result["processed_count"],
                    "skipped": result["skipped_count"],
                    "errors": result["error_count"],
                    "total": result["total_count"]
                },
                "file_id": result.get("file_id"),
                "processed_table": result.get('processed_table')
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get file details
@data_bp.route('/file-details/<int:file_id>', methods=['GET'])
@require_api_key
def get_file_details(file_id):
    """Get details about a specific CSV file."""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    try:
        cursor = get_cursor()
        
        # Get file info
        cursor.execute('''
        SELECT id, file_name, file_path, row_count, column_count, loaded_at, is_processed
        FROM csv_registry
        WHERE id = ?
        ''', (file_id,))
        
        file_info = cursor.fetchone()
        
        if not file_info:
            return jsonify({"error": "File not found"}), 404
        
        # Get processed table name for this file
        processed_table_name = f"processed_data_{file_id}"
        
        # Check if processed table exists
        cursor.execute(f'''
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{processed_table_name}'
        ''')
        
        processed_table_exists = cursor.fetchone()[0] > 0
        
        # Get processed record count if table exists
        if processed_table_exists:
            cursor.execute(f"SELECT COUNT(*) FROM [{processed_table_name}]")
            processed_count = cursor.fetchone()[0]
        else:
            processed_count = 0
        
        # We don't store raw data anymore, so just get the original CSV columns
        try:
            # Try to read the first few rows to get column names
            df_sample = pd.read_csv(file_info[2], nrows=5)
            raw_columns = df_sample.columns.tolist()
            
            # Sample rows from the original CSV
            raw_sample_rows = df_sample.head(5).to_dict('records')
        except Exception as e:
            print(f"Error reading CSV file for preview: {e}")
            raw_columns = []
            raw_sample_rows = []
        
        return jsonify({
            "file_id": file_info[0],
            "file_name": file_info[1],
            "file_path": file_info[2],
            "row_count": file_info[3],
            "column_count": file_info[4],
            "loaded_at": file_info[5].isoformat() if file_info[5] else None,
            "is_processed": bool(file_info[6]),
            "csv_file": file_info[2],
            "raw_columns": raw_columns,
            "raw_sample": raw_sample_rows,
            "processed_table": processed_table_name if processed_table_exists else None,
            "processed_count": processed_count
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get processed table data
@data_bp.route('/processed-table-data/<int:file_id>', methods=['GET'])
@require_api_key
def get_processed_sample(file_id):
    """Get all data from a processed table."""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    try:
        cursor = get_cursor()
        
        # Get table name for the processed data
        processed_table_name = f"processed_data_{file_id}"
        
        # Check if table exists
        cursor.execute(f'''
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{processed_table_name}'
        ''')
        
        if cursor.fetchone()[0] == 0:
            return jsonify({"error": f"Processed table for file ID {file_id} not found"}), 404
        
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
            "file_id": file_id,
            "table_name": processed_table_name,
            "columns": columns,
            "data": all_data,
            "total_records": total_records,
            "limit": limit,
            "offset": offset
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
        cursor.execute('SELECT COUNT(*) FROM csv_registry')
        total_files = cursor.fetchone()[0]
        
        # Get processed vs unprocessed
        cursor.execute('SELECT is_processed, COUNT(*) FROM csv_registry GROUP BY is_processed')
        
        processed_files = 0
        unprocessed_files = 0
        
        for row in cursor.fetchall():
            if row[0]:  # is_processed = 1
                processed_files = row[1]
            else:  # is_processed = 0
                unprocessed_files = row[1]
        
        # Get file details
        cursor.execute('''
        SELECT id, file_name, file_path, row_count, column_count, loaded_at, is_processed
        FROM csv_registry
        ORDER BY loaded_at DESC
        ''')
        
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
        cursor.execute('''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'processed_data_%'
        ''')
        
        processed_tables = [row[0] for row in cursor.fetchall()]
        
        return jsonify({
            "total_files": total_files,
            "processed_files": processed_files,
            "unprocessed_files": unprocessed_files,
            "files": files,
            "processed_tables": processed_tables,
            "processed_tables_count": len(processed_tables)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500