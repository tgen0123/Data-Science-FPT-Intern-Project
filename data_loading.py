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
    """Deduplicate column names for compatibility - PRESERVES underscores."""
    counter = collections.Counter()
    new_cols = []
    seen = set()
    
    print("=== COLUMN NAME PROCESSING ===")
    data_logger.info("=== COLUMN NAME PROCESSING ===")
    
    for col in columns:
        original_col = col
        col = str(col).strip().lower()
        
        # FIXED: Preserve underscores and only replace truly problematic characters
        # Replace spaces, dots, hyphens with underscores, but keep existing underscores
        col = re.sub(r'[\s\.\-]+', '_', col)      # Replace whitespace, dots, hyphens
        col = re.sub(r'[^\w]', '_', col)          # Replace non-word chars (\w includes letters, digits, _)
        col = re.sub(r'_+', '_', col)             # Replace multiple underscores with single
        
        # Don't strip leading underscores - they might be meaningful!
        # Only strip trailing underscores if they were added
        col = col.rstrip('_') if col.endswith('_') and not original_col.strip().endswith('_') else col
        
        print(f"  '{original_col}' → '{col}'")
        data_logger.info(f"  '{original_col}' → '{col}'")
        
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
    
    print(f"Final columns: {new_cols}")
    data_logger.info(f"Final columns: {new_cols}")
    return new_cols

def detect_and_parse_dates(df, threshold=0.7):
    """
    Detect and parse potential date columns in the DataFrame.
    
    Args:
        df: DataFrame to analyze and modify
        threshold: Minimum ratio of valid dates required to convert column (default 0.7 = 70%)
    Returns:
        DataFrame: DataFrame with date columns converted to datetime
    """
    date_columns_converted = []
    
    for col in df.columns:
        if df[col].dtype == 'object':  # Only check string/object columns
            # Skip if column is mostly empty
            non_null_count = df[col].notna().sum()
            if non_null_count < len(df) * 0.1:  # Less than 10% non-null
                continue
                
            # Get sample of non-null values for testing
            sample_values = df[col].dropna().astype(str)
            if len(sample_values) == 0:
                continue
                
            # Check if column name suggests it's a date
            col_lower = col.lower()
            date_indicators = ['date', 'time', 'timestamp', 'created', 'updated', 'modified', 
                             'start', 'end', 'birth', 'dob', 'expire', 'due', 'schedule', '_time']
            
            # Enhanced date detection for column names
            name_suggests_date = (
                any(indicator in col_lower for indicator in date_indicators) or
                col_lower.startswith('_time') or 
                col_lower.endswith('_time') or
                col_lower == '_time'
            )
            
            # DEBUG: Print sample values to see what we're working with
            print(f"\n=== Analyzing column '{col}' ===")
            print(f"Sample values: {sample_values.head(3).tolist()}")
            print(f"Column name suggests date: {name_suggests_date}")
            data_logger.info(f"Analyzing column '{col}' - name suggests date: {name_suggests_date}")
            
            # Try to parse as dates
            try:
                # Test with a sample first to avoid processing large columns unnecessarily
                test_sample = sample_values.head(min(1000, len(sample_values)))
                
                # Try multiple date parsing approaches
                parsed_sample = None
                successful_format = None
                
                # Approach 1: pandas datetime inference (without deprecated parameter)
                try:
                    parsed_sample = pd.to_datetime(test_sample, errors='coerce')
                    if parsed_sample.notna().sum() >= len(test_sample) * threshold:
                        successful_format = "pandas_infer"
                        print(f"✅ Column '{col}' parsed using pandas datetime inference")
                        data_logger.info(f"Column '{col}' parsed using pandas datetime inference")
                    else:
                        parsed_sample = None
                except Exception as e:
                    print(f"❌ Pandas inference failed: {e}")
                    data_logger.warning(f"Pandas inference failed for column '{col}': {e}")
                
                # Approach 2: Common date formats if first approach failed
                if parsed_sample is None:
                    print("Trying specific date formats...")
                    data_logger.info("Trying specific date formats...")
                    
                    common_formats = [
                        # Standard date formats
                        '%Y-%m-%d',           # 2024-01-15
                        '%m/%d/%Y',           # 01/15/2024
                        '%d/%m/%Y',           # 15/01/2024
                        '%d-%m-%Y',           # 15-01-2024
                        '%Y%m%d',             # 20240115
                        '%d-%b-%Y',           # 15-Jan-2024
                        '%b %d, %Y',          # Jan 15, 2024
                        
                        # DateTime formats
                        '%Y-%m-%d %H:%M:%S',  # 2024-01-15 10:30:00
                        '%m/%d/%Y %H:%M:%S',  # 01/15/2024 10:30:00
                        '%d/%m/%Y %H:%M:%S',  # 15/01/2024 10:30:00
                        '%d %m %Y %H:%M',     # 22 11 2024 00:00
                        
                        # ISO 8601 formats - FIXED ORDER (most specific first)
                        '%Y-%m-%dT%H:%M:%S.%f%z',  # 2024-11-22T00:00:02.000000+0700 (6 digits)
                        '%Y-%m-%dT%H:%M:%S%z',     # 2024-11-22T00:00:02+0700
                        '%Y-%m-%dT%H:%M:%S.%fZ',   # 2024-01-15T10:30:00.123456Z
                        '%Y-%m-%dT%H:%M:%S.%f',    # 2024-01-15T10:30:00.123456
                        '%Y-%m-%dT%H:%M:%SZ',      # 2024-01-15T10:30:00Z
                        '%Y-%m-%dT%H:%M:%S',       # 2024-01-15T10:30:00
                        
                        # Additional common formats
                        '%Y-%m-%d %H:%M:%S.%f',   # 2024-01-15 10:30:00.123456
                        '%m/%d/%Y %H:%M:%S.%f',   # 01/15/2024 10:30:00.123456
                    ]
                    
                    for date_format in common_formats:
                        try:
                            print(f"  Trying format: {date_format}")
                            
                            # Special handling for milliseconds vs microseconds
                            if '.%f' in date_format and any('.000' in str(val) for val in test_sample.head(3)):
                                # Convert milliseconds to microseconds for parsing
                                adjusted_sample = test_sample.str.replace(r'\.(\d{3})(\+|Z)', r'.\g<1>000\g<2>', regex=True)
                                print(f"    Adjusted sample for microseconds: {adjusted_sample.head(1).tolist()}")
                                format_parsed = pd.to_datetime(adjusted_sample, format=date_format, errors='coerce')
                            else:
                                format_parsed = pd.to_datetime(test_sample, format=date_format, errors='coerce')
                            
                            valid_ratio = format_parsed.notna().sum() / len(test_sample)
                            print(f"    Valid ratio: {valid_ratio:.2f} (threshold: {threshold})")
                            
                            if valid_ratio >= threshold:
                                parsed_sample = format_parsed
                                successful_format = date_format
                                print(f"✅ Column '{col}' parsed using format: {date_format}")
                                data_logger.info(f"Column '{col}' parsed using format: {date_format}")
                                break
                            
                        except Exception as format_error:
                            print(f"    ❌ Format {date_format} failed: {format_error}")
                            continue
                
                # Approach 3: Manual preprocessing for common timezone issues
                if parsed_sample is None:
                    print("Trying manual preprocessing for timezone issues...")
                    data_logger.info("Trying manual preprocessing for timezone issues...")
                    try:
                        # Handle the specific case of .000+timezone by converting to .000000
                        preprocessed_sample = test_sample.str.replace(
                            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{3})(\+\d{4})', 
                            r'\1.\2000\3', 
                            regex=True
                        )
                        
                        print(f"  Preprocessed sample: {preprocessed_sample.head(3).tolist()}")
                        
                        # Try parsing the preprocessed version
                        format_parsed = pd.to_datetime(preprocessed_sample, format='%Y-%m-%dT%H:%M:%S.%f%z', errors='coerce')
                        valid_ratio = format_parsed.notna().sum() / len(test_sample)
                        print(f"  Preprocessed valid ratio: {valid_ratio:.2f}")
                        
                        if valid_ratio >= threshold:
                            parsed_sample = format_parsed
                            successful_format = "manual_preprocessing"
                            print(f"✅ Column '{col}' parsed using manual preprocessing")
                            data_logger.info(f"Column '{col}' parsed using manual preprocessing")
                        
                    except Exception as e:
                        print(f"  ❌ Manual preprocessing failed: {e}")
                
                # Approach 4: Flexible pandas parsing as final fallback
                if parsed_sample is None:
                    print("Trying flexible pandas parsing as final fallback...")
                    data_logger.info("Trying flexible pandas parsing as final fallback...")
                    try:
                        # This is more flexible but slower - handles many edge cases
                        parsed_sample = pd.to_datetime(test_sample, errors='coerce', utc=False)
                        valid_ratio = parsed_sample.notna().sum() / len(test_sample)
                        print(f"  Flexible parsing valid ratio: {valid_ratio:.2f}")
                        
                        if valid_ratio >= threshold:
                            successful_format = "flexible_pandas"
                            print(f"✅ Column '{col}' parsed using flexible pandas parsing")
                            data_logger.info(f"Column '{col}' parsed using flexible pandas parsing")
                            
                    except Exception as e:
                        print(f"  ❌ Flexible parsing failed: {e}")
                
                # Check if we have enough valid dates
                if parsed_sample is not None:
                    valid_ratio = parsed_sample.notna().sum() / len(test_sample)
                    
                    # Lower threshold if column name suggests it's a date
                    effective_threshold = threshold * 0.5 if name_suggests_date else threshold
                    print(f"Effective threshold: {effective_threshold:.2f}, Valid ratio: {valid_ratio:.2f}")
                    
                    if valid_ratio >= effective_threshold:
                        # Parse the entire column using the same successful method
                        try:
                            print(f"Applying successful format '{successful_format}' to entire column...")
                            data_logger.info(f"Applying successful format '{successful_format}' to entire column '{col}'")
                            
                            if len(sample_values) == len(test_sample):
                                # We already parsed the full sample
                                full_parsed = parsed_sample
                            else:
                                # Apply the same successful parsing method to the full column
                                if successful_format == "pandas_infer":
                                    full_parsed = pd.to_datetime(sample_values, errors='coerce')
                                elif successful_format == "flexible_pandas":
                                    full_parsed = pd.to_datetime(sample_values, errors='coerce', utc=False)
                                elif successful_format == "manual_preprocessing":
                                    preprocessed_full = sample_values.str.replace(
                                        r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{3})(\+\d{4})', 
                                        r'\1.\2000\3', 
                                        regex=True
                                    )
                                    full_parsed = pd.to_datetime(preprocessed_full, format='%Y-%m-%dT%H:%M:%S.%f%z', errors='coerce')
                                else:
                                    # Use the specific format that worked
                                    if '.%f' in successful_format and any('.000' in str(val) for val in sample_values.head(10)):
                                        adjusted_full = sample_values.str.replace(r'\.(\d{3})(\+|Z)', r'.\g<1>000\g<2>', regex=True)
                                        full_parsed = pd.to_datetime(adjusted_full, format=successful_format, errors='coerce')
                                    else:
                                        full_parsed = pd.to_datetime(sample_values, format=successful_format, errors='coerce')
                            
                            # Apply the parsed dates to the original DataFrame
                            df.loc[df[col].notna(), col] = full_parsed
                            
                            # Convert the column to datetime
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                            
                            final_valid_ratio = df[col].notna().sum() / non_null_count
                            date_columns_converted.append({
                                'column': col,
                                'valid_ratio': final_valid_ratio,
                                'name_suggested_date': name_suggests_date,
                                'format_used': successful_format
                            })
                            
                            print(f"✅ Successfully converted column '{col}' to datetime (final valid ratio: {final_valid_ratio:.2f}, format: {successful_format})")
                            data_logger.info(f"Successfully converted column '{col}' to datetime (final valid ratio: {final_valid_ratio:.2f}, format: {successful_format})")
                            
                        except Exception as e:
                            print(f"❌ Failed to convert full column '{col}' to datetime: {str(e)}")
                            data_logger.error(f"Failed to convert full column '{col}' to datetime: {str(e)}")
                    else:
                        print(f"❌ Valid ratio {valid_ratio:.2f} below threshold {effective_threshold:.2f}")
                else:
                    print(f"❌ No valid parsing method found for column '{col}'")
                            
            except Exception as e:
                # Skip this column if parsing fails
                print(f"❌ Could not parse column '{col}' as date: {str(e)}")
                data_logger.error(f"Could not parse column '{col}' as date: {str(e)}")
                continue
    
    if date_columns_converted:
        print(f"\n🎉 Successfully converted {len(date_columns_converted)} columns to datetime:")
        data_logger.info(f"Successfully converted {len(date_columns_converted)} columns to datetime:")
        
        # Log details for each converted column
        for item in date_columns_converted:
            msg = f"  ✅ {item['column']}: {item['valid_ratio']:.1%} valid dates, format: {item['format_used']}"
            print(msg)
            data_logger.info(msg)
    else:
        print("\n❌ No date columns detected or converted")
        data_logger.info("No date columns detected or converted")
    
    return df

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
        
        # Add date detection to chunk processing
        chunk = detect_and_parse_dates(chunk)
        
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
                
                print(f"Original dataset shape: {df.shape}")
                
                # Step 1: Deduplicate column names
                df.columns = deduplicate_columns(df.columns)
                print(f"Deduplicated columns: {list(df.columns)}")
                
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
                print(f"Number of duplicate rows: {duplicates}")
                
                print(f"Final preprocessed dataset shape: {df.shape}")
                print(f"Final columns: {list(df.columns)}")
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

def register_csv_file(file_path, row_count, column_count, date_columns=None):
    """
    Register a CSV file in the database registry.
    
    Args:
        file_path (str): Path to the CSV file
        row_count (int): Number of rows in the CSV
        column_count (int): Number of columns in the CSV
        date_columns (list): List of columns that were detected as dates
        
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
                [loaded_at] DATETIME2 DEFAULT GETDATE(),
                [is_processed] BIT DEFAULT 1,
                [date_columns_detected] NVARCHAR(MAX) NULL
            )
            
            CREATE INDEX [idx_csv_registry_is_processed] ON [dbo].[csv_registry] ([is_processed])
        END
        ''')
        
        # Extract filename from path
        file_name = os.path.basename(file_path)
        
        # Prepare date columns info for storage
        date_columns_json = None
        if date_columns:
            date_columns_json = json.dumps(date_columns)
        
        # Check if this file is already registered
        cursor.execute('SELECT id FROM csv_registry WHERE file_path = ?', (file_path,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing entry
            update_query = '''
            UPDATE csv_registry 
            SET row_count = ?, column_count = ?, loaded_at = GETDATE(), is_processed = 1, date_columns_detected = ?
            WHERE file_path = ?
            ''', (row_count, column_count, file_path))
            file_id = existing[0]
            data_logger.info(f"Updated existing file registration with ID: {file_id}")
        else:
            # Insert new entry
            cursor.execute('''
            INSERT INTO csv_registry (file_name, file_path, row_count, column_count, is_processed)
            VALUES (?, ?, ?, ?, 1)
            ''', (file_name, file_path, row_count, column_count))
            
            # Get the ID of the inserted file
            cursor.execute('SELECT @@IDENTITY')
            file_id = cursor.fetchone()[0]
            data_logger.info(f"Created new file registration with ID: {file_id}")
        
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
    
    # Reset index to ensure no unnamed columns
    df_processed.reset_index(drop=True, inplace=True)
    
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

def save_processed_data_to_db(processed_df, file_path, batch_size=10000, target_table=None):
    """
    Save preprocessed data directly to a database table with proper datetime handling.
    
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
        
        # Get columns from the preprocessed DataFrame
        processed_columns = processed_df.columns.tolist()
        msg = f"Saving data with {len(processed_columns)} columns: {processed_columns}"
        print(msg)
        data_logger.info(msg)
        
        # FIXED: Identify date columns - works for both timezone-naive and timezone-aware
        date_columns = []
        for col in processed_columns:
            col_dtype_str = str(processed_df[col].dtype)
            print(f"🔍 Checking column '{col}': dtype = '{col_dtype_str}'")
            data_logger.info(f"Checking column '{col}': dtype = '{col_dtype_str}'")
            
            if 'datetime64' in col_dtype_str:
                date_columns.append(col)
                print(f"  ✅ '{col}' identified as datetime column")
                data_logger.info(f"'{col}' identified as datetime column")
            else:
                print(f"  ❌ '{col}' is not a datetime column")
        
        if date_columns:
            print(f"🕐 Date columns detected for database storage: {date_columns}")
            data_logger.info(f"Date columns detected for database storage: {date_columns}")
            
            # CONVERT TIMEZONE-AWARE DATETIMES TO NAIVE (CRITICAL FIX)
            for date_col in date_columns:
                print(f"  Processing datetime column: {date_col}")
                data_logger.info(f"Processing datetime column: {date_col}")
                col_dtype_str = str(processed_df[date_col].dtype)
                
                # Check if the datetime column is timezone-aware
                if 'datetime64[ns,' in col_dtype_str or (hasattr(processed_df[date_col].dt, 'tz') and processed_df[date_col].dt.tz is not None):
                    print(f"    ⚠️  Column '{date_col}' is timezone-aware ({col_dtype_str}), converting to naive (local time)")
                    data_logger.info(f"Column '{date_col}' is timezone-aware ({col_dtype_str}), converting to naive")
                    # Convert to local time and remove timezone info for SQL Server compatibility
                    try:
                        processed_df[date_col] = processed_df[date_col].dt.tz_convert(None)
                    except:
                        # If tz_convert fails, try removing timezone directly
                        processed_df[date_col] = processed_df[date_col].dt.tz_localize(None)
                else:
                    print(f"    ✅ Column '{date_col}' is timezone-naive ({col_dtype_str})")
                    data_logger.info(f"Column '{date_col}' is timezone-naive ({col_dtype_str})")
                
                # Ensure the column is datetime64[ns] after conversion (no timezone)
                processed_df[date_col] = pd.to_datetime(processed_df[date_col])
                final_dtype = str(processed_df[date_col].dtype)
                print(f"    Final dtype for '{date_col}': {final_dtype}")
                data_logger.info(f"Final dtype for '{date_col}': {final_dtype}")
        else:
            print("❌ No date columns detected for database storage")
            data_logger.info("No date columns detected for database storage")
        
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
        print(f"Saving data with {len(processed_columns)} columns: {processed_columns}")
        
        # Create a map of column names to their SAFE SQL names
        column_mapping = {}
        timestamp_pattern = re.compile(r'time|date|timestamp', re.IGNORECASE)
        
        for col in processed_columns:
            if col.lower() == 'id':
                # Rename to avoid conflict with primary key
                safe_col = 'original_id'
            else:
                # Create a safe SQL column name
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
                # Skip adding a column definition if it would conflict with primary key
                if col.lower() == 'id':
                    continue  # We'll handle this as 'original_id' below
                    
                safe_col = column_mapping[col]
                col_type = processed_df[col].dtype
                col_type_str = str(col_type)
                
                print(f"🔍 Processing column '{col}' for SQL mapping:")
                print(f"    pandas dtype: {col_type}")
                print(f"    dtype string: '{col_type_str}'")
                data_logger.info(f"Processing column '{col}' for SQL mapping: dtype={col_type_str}")
                
                # FIXED: Map pandas dtypes to SQL Server types with proper datetime handling
                if 'datetime64' in col_type_str:
                    # ✅ This works for both timezone-naive and timezone-aware
                    sql_type = "DATETIME2(3)"  # 3 decimal places for milliseconds
                    print(f"    ✅ DATETIME DETECTED: '{safe_col}' mapped to SQL type: {sql_type}")
                    data_logger.info(f"DATETIME DETECTED: '{safe_col}' mapped to SQL type: {sql_type}")
                elif col_type_str in ['float64', 'float32']:
                    sql_type = "FLOAT"
                    print(f"    Mapped to FLOAT")
                elif col_type_str in ['int64', 'int32']:
                    sql_type = "BIGINT"  # Use BIGINT to avoid overflow
                    print(f"    Mapped to BIGINT")
                elif col_type_str == 'bool':
                    sql_type = "BIT"
                    print(f"    Mapped to BIT")
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
            data_logger.info(f"Created new table: {table_name}")
            processing_times['schema_creation'] = time.time() - schema_start
        else:
            processing_times['table_drop'] = 0
            processing_times['schema_creation'] = 0

        # Time bulk insert operation
        insert_start = time.time()
        print("Starting bulk insert operation...")
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
        print(f"{'Total time':<20} {total_time:,.2f}s{'100.0':>11}%\n")

        return {
            "processed_count": processed_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
            "total_count": processed_count + skipped_count + error_count,
            "processed_table": table_name,
            "date_columns": date_columns,
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
            print(f"Limited to {max_rows} rows as requested")
        
        # Register the CSV file in the database
        data_logger.info(f"Registering CSV file in database: {file_path}")
        file_id = register_csv_file(
            file_path, 
            len(processed_df), 
            len(processed_df.columns),
            date_columns
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
            "date_columns_detected": result.get('date_columns', []),
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
        ''')
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
        ''')
        
        columns = []
        for col in cursor.fetchall():
            column_info = {
                "name": col[0],
                "type": col[1],
                "max_length": col[2]
            }
            columns.append(column_info)
            
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM [{processed_table_name}]")
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
            # Parse date columns if available
            date_columns = None
            if row[7]:  # date_columns_detected
                try:
                    date_columns = json.loads(row[7])
                except:
                    date_columns = None
                    
            files.append({
                "id": row[0],
                "file_name": row[1],
                "file_path": row[2],
                "row_count": row[3],
                "column_count": row[4],
                "loaded_at": row[5].isoformat() if row[5] else None,
                "is_processed": bool(row[6]),
                "date_columns_detected": date_columns
            })
        
        # Get processed tables
        tables_query = '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'processed_data_%'
        ''')
        
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