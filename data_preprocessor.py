# data_preprocessor.py
import pandas as pd
import numpy as np
import re
import collections
from datetime import datetime

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

def clean_username(username):
    """Clean and normalize username from domain format."""
    if pd.isna(username):
        return username

    # Extract just the username from email format if present
    if isinstance(username, str) and '@' in username:
        username = username.split('@')[0]

    return username

def extract_department(full_name):
    """Extract department information from fully qualified username."""
    if pd.isna(full_name) or not isinstance(full_name, str):
        return None

    # Split by slashes
    parts = full_name.split('/')

    # Extract department (usually the fourth part)
    dept = parts[3] if len(parts) > 3 else None

    return dept

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

def group_equal_object_columns(df):
    """
    Group columns that have identical values to detect duplicates.
    
    Args:
        df: DataFrame to analyze
    Returns:
        tuple: (list of results, list of columns to drop)
    """
    # Get only the object columns
    object_columns = df.select_dtypes(include=['object']).columns.tolist()

    if len(object_columns) < 2:
        return ["Less than 2 object columns found"], []

    # Initialize groups
    groups = []
    processed_columns = set()

    # Process each column
    for col1 in object_columns:
        # Skip if we've already processed this column
        if col1 in processed_columns:
            continue

        # Start a new group with this column
        current_group = [col1]
        processed_columns.add(col1)

        # Compare with all other unprocessed columns
        for col2 in object_columns:
            if col2 in processed_columns:
                continue

            # Check if columns are equal (handling NaN values)
            is_equal = ((df[col1] == df[col2]) |
                      (pd.isna(df[col1]) & pd.isna(df[col2]))).all()

            if is_equal:
                current_group.append(col2)
                processed_columns.add(col2)

        # Add this group to our results
        if len(current_group) > 0:
            groups.append(current_group)

    # Format results nicely as in the notebook
    results = []
    columns_to_drop = []
    
    # Prefer to keep columns with certain names for key fields
    preferred_columns = {
        'username': ['subjectusername', 'username', 'user', 'name'],
        'timestamp': ['timestamp', '_time', 'time', 'date', 'datetime'],
        'source_ip': ['callingstationid', 'sourceip', 'source_ip', 'clientip', 'ip'],
        'fullname': ['fullyqualifiedsubjectusername', 'fullname', 'full_name']
    }
    
    for group in groups:
        if len(group) > 1:
            # This is a group of identical columns - choose one to keep
            first_col = group[0]
            other_cols = group[1:]
            
            # Check if any high-priority column names exist in this group
            keep_col = first_col  # Default to keeping the first column
            for category, priority_list in preferred_columns.items():
                for priority_col in priority_list:
                    if priority_col in group:
                        keep_col = priority_col
                        break
                if keep_col != first_col:
                    break
            
            # Add all columns except the one we're keeping to the drop list
            drop_cols = [col for col in group if col != keep_col]
            columns_to_drop.extend(drop_cols)
            
            results.append(f"Column '{first_col}' equals to columns {other_cols} - keeping '{keep_col}'")
        else:
            results.append(f"Column '{group[0]}' is unique")

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
        # We'll use a graph-based approach with connected components
        # to identify groups of correlated columns
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

def preprocess_data(file_path):
    """
    Preprocess data using the approach from the notebook.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        DataFrame: Cleaned and preprocessed DataFrame
    """
    try:
        # Load original dataset
        df_original = pd.read_csv(file_path)
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
        equality_results, duplicate_columns_to_drop = group_equal_object_columns(df)
        
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
        
        # Step 8: Format timestamp columns
        # Look for common timestamp column names
        timestamp_cols = [col for col in df.columns if col in ['systemtime', '_time', 'timestamp']]
        
        if 'systemtime' in timestamp_cols and '_time' in timestamp_cols:
            # Remove quotes if it's a string type column
            if df['systemtime'].dtype == 'object':
                df['systemtime'] = df['systemtime'].str.replace("'", "")
            
            # Convert both to datetime
            df['systemtime'] = pd.to_datetime(df['systemtime'], errors='coerce')
            df['_time'] = pd.to_datetime(df['_time'], errors='coerce')
            
            # Convert systemtime to UTC+7
            df['systemtime'] = df['systemtime'] + pd.Timedelta(hours=7)
            
            # Format both with timezone marker
            df['systemtime'] = df['systemtime'].dt.strftime('%Y-%m-%d %H:%M:%S') + '+07:00'
            df['_time'] = df['_time'].dt.strftime('%Y-%m-%d %H:%M:%S') + '+07:00'
            
            # Check if they're equal - if so, drop systemtime and rename _time
            are_equal = np.array_equal(df['systemtime'].values, df['_time'].values)
            if are_equal:
                df = df.drop(['systemtime'], axis=1)
                print("Dropped 'systemtime' as it's identical to '_time'")
                # Rename _time to timestamp
                df['timestamp'] = df['_time']
                df = df.drop(['_time'], axis=1)
                print("Renamed '_time' to 'timestamp'")
            
        # Step 9: Extract time features
        if 'timestamp' in df.columns:
            # First, check what type of data we're dealing with
            if df['timestamp'].dtype == 'object':  # If it's still a string
                # Convert to datetime first, then extract hour
                timestamps = pd.to_datetime(df['timestamp'].str.replace('\\+07:00', ''), errors='coerce')
                df['hour_of_day'] = timestamps.dt.hour
            else:  # If it's already datetime type
                # Just extract the hour directly
                df['hour_of_day'] = df['timestamp'].dt.hour
                
            # Create time category based on hour
            df['time_category'] = pd.cut(
                df['hour_of_day'],
                bins=[0, 6, 12, 18, 24],
                labels=['night_(0-6)', 'morning_(6-12)', 'afternoon_(12-18)', 'evening_(18-24)'],
                include_lowest=True
            )
            print("Added time features: 'hour_of_day' and 'time_category'")
            print(f"Hour of day range: {df['hour_of_day'].min()} to {df['hour_of_day'].max()}")
            print(f"Null values in hour_of_day: {df['hour_of_day'].isnull().sum()}")
        
        # Step 10: Clean username column
        if 'subjectusername' in df.columns:
            df['subjectusername'] = df['subjectusername'].apply(clean_username)
            print("Cleaned 'subjectusername' column")
        
        # Step 11: Extract department information
        if 'fullyqualifiedsubjectusername' in df.columns:
            df['department'] = df['fullyqualifiedsubjectusername'].apply(extract_department)
            print("Extracted 'department' from 'fullyqualifiedsubjectusername'")
        
        print(f"Final preprocessed dataset shape: {df.shape}")
        print(f"Final columns: {list(df.columns)}")
        return df
        
    except Exception as e:
        print(f"Error preprocessing data: {e}")
        return None