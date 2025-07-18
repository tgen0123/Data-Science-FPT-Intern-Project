import os
import pyodbc
import traceback
import pandas as pd
from flask import g, current_app
import utils
import numpy as np

def initialize_schema():
    """
    Checks and creates all necessary tables in the database.
    This function is intended to be called once on application startup.
    """
    try:
        db = get_db()
        cursor = db.cursor()
        print("Checking and creating database tables...")
        
        # API Keys Table
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[api_keys]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[api_keys] (
                [key] NVARCHAR(255) PRIMARY KEY,
                [username] NVARCHAR(255) NOT NULL,
                [rate_limit] INT DEFAULT 100,
                [is_admin] BIT DEFAULT 0
            )
            PRINT 'Created table: api_keys'
        END
        ''')
        
        # CSV Registry Table
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[csv_registry]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[csv_registry] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                file_name NVARCHAR(255) NOT NULL,
                file_path NVARCHAR(1000) NOT NULL,
                row_count INT NOT NULL,
                column_count INT NOT NULL,
                loaded_at DATETIME2 DEFAULT GETDATE(),
                is_processed BIT DEFAULT 1,
                date_columns_detected NVARCHAR(MAX) NULL,
                ip_columns_detected NVARCHAR(MAX) NULL,
                datetime_separation_info NVARCHAR(MAX) NULL
            )
            CREATE INDEX [idx_csv_registry_file_name] ON [dbo].[csv_registry] ([file_name])
            PRINT 'Created table: csv_registry'
        END
        ''')
        
        # Splunk Alerts Table
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[splunk_alerts]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[splunk_alerts] (
                id INT IDENTITY(1,1) PRIMARY KEY,
                alert_name NVARCHAR(255),
                search_name NVARCHAR(255),
                severity NVARCHAR(50),
                trigger_time DATETIME2,
                result_count INT,
                search_query NVARCHAR(MAX),
                alert_data NVARCHAR(MAX),
                source_ip NVARCHAR(45),
                user_agent NVARCHAR(500),
                received_at DATETIME2 DEFAULT GETDATE(),
                processed BIT DEFAULT 0,
                processed_at DATETIME2 NULL,
                error_message NVARCHAR(MAX) NULL
            )
            CREATE INDEX [idx_splunk_alerts_alert_name] ON [dbo].[splunk_alerts] ([alert_name])
            PRINT 'Created table: splunk_alerts'
        END
        ''')

        db.commit()
        print("Schema initialization complete.")
    except Exception as e:
        print(f"An error occurred during schema initialization: {e}")
        traceback.print_exc()

def get_db():
    """
    Get database connection, creating one if needed.
    
    Returns:
        pyodbc.Connection: A connection to the SQL Server database
    """
    db = getattr(g, '_database', None)
    if db is None:
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={current_app.config['SQLSERVER_HOST']};"
            f"DATABASE={current_app.config['SQLSERVER_DB']};"
            # f"UID={current_app.config['SQLSERVER_USER']};"
            # f"PWD={current_app.config['SQLSERVER_PASS']};"
            f"Trusted_Connection=yes;"  # Use this for Windows Authentication
            "Encrypt=yes;"
            "TrustServerCertificate=yes;" 
        )
        db = g._database = pyodbc.connect(connection_string)
    return db

def get_cursor():
    """
    Get a cursor from the database connection.
    
    Returns:
        pyodbc.Cursor: A cursor object for executing SQL queries
    """
    return get_db().cursor()

def dict_from_row(row, columns):
    """
    Convert a pyodbc row to a dictionary.
    
    Args:
        row (pyodbc.Row): A pyodbc row result from a query
        columns (list[str]): List of column names from cursor.description
        
    Returns:
        dict: Dictionary mapping column names to row values
    """
    return {columns[i]: row[i] for i in range(len(columns))}

def get_file_id_by_name(file_name):
    """
    Look up a file's ID by its name.
    
    Args:
        file_name (str): Name of the file to look up
        
    Returns:
        int: File ID if found, None otherwise
    """
    try:
        db = get_db()
        cursor = db.cursor()
        
        cursor.execute('SELECT id FROM csv_registry WHERE file_name = ?', (file_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        print(f"Error looking up file ID: {e}")
        traceback.print_exc()
        return None

def get_file_path_from_name(file_name, base_dir=None):
    """
    Get the full file path from a filename, either from the database or by constructing it.
    
    Args:
        file_name (str): Name of the file
        base_dir (str): Base directory to use if file not in database
        
    Returns:
        str: Full file path
    """
    try:
        # First check if the file is already in our database
        db = get_db()
        cursor = db.cursor()
        
        cursor.execute('SELECT file_path FROM csv_registry WHERE file_name = ?', (file_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        elif base_dir:
            # Construct path from base directory
            return os.path.join(base_dir, file_name)
        else:
            # Try to get the base directory from app config
            base_dir = current_app.config.get('DATA_DIRECTORY', '/path/to/csv/files')
            return os.path.join(base_dir, file_name)
    except Exception as e:
        print(f"Error getting file path: {e}")
        traceback.print_exc()
        
        # Fallback to just using the filename
        if base_dir:
            return os.path.join(base_dir, file_name)
        else:
            # Try to get the base directory from app config
            base_dir = current_app.config.get('DATA_DIRECTORY', '/path/to/csv/files')
            return os.path.join(base_dir, file_name)
        
def register_csv_file(file_path, row_count, column_count, date_columns=None, ip_columns=None, datetime_separation_info=None):
    """
    FIXED: Register a CSV file in the database registry with proper numpy type conversion.
    This function assumes the 'csv_registry' table already exists.
    
    Args:
        file_path (str): Path to the CSV file
        row_count (int): Number of rows in the CSV
        column_count (int): Number of columns in the CSV
        date_columns (list): List of columns that were detected as dates
        ip_columns (list): List of columns that were detected as IP addresses
        datetime_separation_info (dict): Information about datetime column separation
        
    Returns:
        int: File ID in the database registry
    """
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Extract filename from path
        file_name = os.path.basename(file_path)
        print(f"Preparing data for JSON serialization...")
        
        # Prepare info for storage
        date_columns_json = utils.safe_json_dumps(date_columns) if date_columns else None
        ip_columns_json = utils.safe_json_dumps(ip_columns) if ip_columns else None
        datetime_separation_json = utils.safe_json_dumps(datetime_separation_info) if datetime_separation_info else None
        
        # Check if this file is already registered
        cursor.execute('SELECT id FROM csv_registry WHERE file_path = ?', (file_path,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing entry
            file_id = existing[0]
            cursor.execute('''
            UPDATE csv_registry 
            SET row_count = ?, column_count = ?, loaded_at = GETDATE(), is_processed = 1, 
                date_columns_detected = ?, ip_columns_detected = ?, datetime_separation_info = ?
            WHERE id = ?
            ''', (row_count, column_count, date_columns_json, ip_columns_json, datetime_separation_json, file_id))
            print(f"Updated existing file registration with ID: {file_id}")
        else:
            # Insert new entry
            cursor.execute('''
            INSERT INTO csv_registry (file_name, file_path, row_count, column_count, is_processed, 
                                    date_columns_detected, ip_columns_detected, datetime_separation_info)
            VALUES (?, ?, ?, ?, 1, ?, ?, ?)
            ''', (file_name, file_path, row_count, column_count, date_columns_json, ip_columns_json, datetime_separation_json))
            
            # Get the ID of the inserted file
            cursor.execute('SELECT @@IDENTITY')
            file_id = cursor.fetchone()[0]
            print(f"Inserted new file registration with ID: {file_id}")
        
        db.commit()
        return file_id
    
    except Exception as e:
        print(f"Error registering CSV file: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_processed_data_to_db(processed_df, file_id, file_name, ip_detection_result, datetime_separation_result, batch_size=1000):
    """
    Save preprocessed data directly to a new database table with proper datetime, IP handling, and separated date/time columns.
    Handles any dataset structure without hardcoded column names.
    
    Args:
        processed_df (DataFrame): Preprocessed DataFrame
        file_id (int): ID of the file in the database registry
        ip_detection_result (dict): Result from IP detection containing detected IP columns
        datetime_separation_result (dict): Result from datetime separation containing separation info
        batch_size (int): Number of rows to insert in a batch
        
    Returns:
        dict: Processing statistics
    """
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Get columns from the preprocessed DataFrame
        processed_columns = processed_df.columns.tolist()
        print(f"Saving data with {len(processed_columns)} columns: {processed_columns}")
        
        # Get IP columns from detection result
        ip_columns = ip_detection_result.get('ip_columns', [])
        print(f"IP columns to be saved as strings: {ip_columns}")
        
        # Get separated columns info
        separated_date_columns = []
        separated_hour_columns = []
        for original_col, info in datetime_separation_result.items():
            separated_date_columns.append(info['date_column'])
            separated_hour_columns.append(info['hour_column'])
        
        print(f"Separated date columns: {separated_date_columns}")
        print(f"Separated hour columns: {separated_hour_columns}")
        
        # FIXED: Identify date columns - works for both timezone-naive and timezone-aware
        original_datetime_columns = []
        for col in processed_columns:
            col_dtype_str = str(processed_df[col].dtype)
            print(f"Checking column '{col}': dtype = '{col_dtype_str}'")
            
            if 'datetime64' in col_dtype_str:
                original_datetime_columns.append(col)
                print(f"  '{col}' identified as original datetime column")
            else:
                print(f"  '{col}' is not a datetime column")
        
        if original_datetime_columns:
            print(f"Original datetime columns detected for database storage: {original_datetime_columns}")
            
            # FIXED: Convert timezone-aware datetimes to naive while PRESERVING original time values
            for date_col in original_datetime_columns:
                print(f"  Processing datetime column: {date_col}")
                col_dtype_str = str(processed_df[date_col].dtype)
                
                # Check if the datetime column is timezone-aware
                if 'datetime64[ns,' in col_dtype_str or (hasattr(processed_df[date_col].dt, 'tz') and processed_df[date_col].dt.tz is not None):
                    print(f"    Column '{date_col}' is timezone-aware ({col_dtype_str})")
                    print(f"    PRESERVING original timezone values (not converting to UTC)")
                    
                    # FIXED: Use tz_localize(None) instead of tz_convert(None)
                    # This removes timezone info WITHOUT converting the time values
                    try:
                        # Sample before conversion
                        sample_before = processed_df[date_col].dropna().iloc[0] if len(processed_df[date_col].dropna()) > 0 else None
                        print(f"    Before: {sample_before}")
                        
                        # Remove timezone info while preserving the actual time values
                        processed_df[date_col] = processed_df[date_col].dt.tz_localize(None)
                        
                        # Sample after conversion
                        sample_after = processed_df[date_col].dropna().iloc[0] if len(processed_df[date_col].dropna()) > 0 else None
                        print(f"    After:  {sample_after}")
                        print(f"    Successfully preserved original time values")
                        
                    except Exception as e:
                        print(f"    Error with tz_localize(None), trying alternative method: {e}")
                        # Alternative: manually strip timezone
                        try:
                            processed_df[date_col] = processed_df[date_col].dt.tz_localize(None)
                        except:
                            # Last resort: convert to string and reparse
                            processed_df[date_col] = pd.to_datetime(processed_df[date_col].dt.strftime('%Y-%m-%d %H:%M:%S.%f'))
                else:
                    print(f"    Column '{date_col}' is timezone-naive ({col_dtype_str})")
                
                # Ensure the column is datetime64[ns] after conversion (no timezone)
                processed_df[date_col] = pd.to_datetime(processed_df[date_col])
                final_dtype = str(processed_df[date_col].dtype)
                print(f"    Final dtype for '{date_col}': {final_dtype}")
        else:
            print("No original datetime columns detected for database storage")
        
        # Create a map of column names to their SAFE SQL names
        column_mapping = {}
        for col in processed_columns:
            if col.lower() == 'id':
                # Rename to avoid conflict with primary key
                safe_col = 'original_id'
            else:
                # Create a safe SQL column name
                safe_col = col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            column_mapping[col] = safe_col
        
        # Create the processed_data table
        sanitized_name = utils.sanitize_for_table_name(file_name)
        table_name = sanitized_name
        
        cursor.execute(f'''
        IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))
        DROP TABLE [dbo].[{table_name}]
        ''')
        db.commit()
        
        # Define SQL column types for the processed data
        column_definitions = ["[id] INT IDENTITY(1,1) PRIMARY KEY"]
        
        for col in processed_columns:
            # Skip adding a column definition if it would conflict with primary key
            if col.lower() == 'id':
                continue  # We'll handle this as 'original_id' below
                
            safe_col = column_mapping[col]
            col_type = processed_df[col].dtype
            col_type_str = str(col_type)
            
            print(f"Processing column '{col}' for SQL mapping:")
            print(f"    pandas dtype: {col_type}")
            print(f"    dtype string: '{col_type_str}'")
            
            # Map pandas dtypes to SQL Server types with proper datetime, IP, and separated column handling
            if 'datetime64' in col_type_str:
                sql_type = "DATETIME2(3)"  # 3 decimal places for milliseconds
                print(f"    ORIGINAL DATETIME DETECTED: '{safe_col}' mapped to SQL type: {sql_type}")
            elif col in separated_date_columns:
                sql_type = "DATE"
                print(f"    SEPARATED DATE COLUMN DETECTED: '{safe_col}' mapped to SQL type: {sql_type}")
            elif col in separated_hour_columns:
                sql_type = "INT"  # 3 decimal places for milliseconds
                print(f"    SEPARATED HOUR COLUMN DETECTED: '{safe_col}' mapped to SQL type: {sql_type}")
            elif col in ip_columns:
                # Handle IP columns with appropriate length - FORCE STRING TYPE
                sql_type = "NVARCHAR(45)"  # IPv6 addresses can be up to 45 chars
                print(f"    IP COLUMN DETECTED: '{safe_col}' mapped to SQL type: {sql_type}")
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
                # Determine appropriate length for string columns based on actual data
                if col_type_str in ['object', 'string']:
                    # Sample non-null values to determine length
                    sample_values = processed_df[col].dropna().astype(str).head(100)
                    if len(sample_values) > 0:
                        max_length = sample_values.str.len().max()
                        
                        # Categorize string columns by their content
                        # IP addresses - use non-capturing groups to avoid warning
                        ip_pattern = r'(?:\d{1,3}\.){3}\d{1,3}'
                        
                        if (col.lower().endswith('ip') or 
                            'address' in col.lower() or 
                            processed_df[col].astype(str).str.contains(ip_pattern, regex=True).mean() > 0.5):
                            sql_type = "NVARCHAR(45)"  # IPv6 addresses can be up to 45 chars
                        # User identifiers
                        elif any(name in col.lower() for name in ['user', 'name', 'login', 'email', 'account']):
                            sql_type = "NVARCHAR(255)"
                        # Session or identifier columns
                        elif any(name in col.lower() for name in ['id', 'session', 'identifier', 'token', 'key']):
                            sql_type = "NVARCHAR(255)"
                        # Short text columns
                        elif max_length < 50:
                            sql_type = f"NVARCHAR({max(max_length * 2, 50)})"
                        # Medium text columns
                        elif max_length < 255:
                            sql_type = "NVARCHAR(255)"
                        # Large text columns
                        else:
                            sql_type = "NVARCHAR(MAX)"
                    else:
                        # Default for empty columns
                        sql_type = "NVARCHAR(255)"
                else:
                    # Unknown data type
                    sql_type = "NVARCHAR(MAX)"
                
                print(f"    Mapped to {sql_type}")
            column_definitions.append(f"[{safe_col}] {sql_type}")
        
        # Add original_id column if needed (if there was an id column in the data)
        if 'id' in processed_columns:
            column_definitions.append("[original_id] BIGINT")
            
        # Add processed_at timestamp column
        column_definitions.append("[processed_at] DATETIME2 DEFAULT GETDATE()")
        
        # Create the table
        create_table_sql = f'''
        CREATE TABLE [dbo].[{table_name}] (
            {', '.join(column_definitions)}
        )
        '''
        
        cursor.execute(create_table_sql)
        db.commit()
        print(f"Created table {table_name} with proper datetime, IP, and separated date/time columns")
        
        # Insert data into processed table
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        # Process in batches
        total_rows = len(processed_df)
        print(f"Starting to process {total_rows} rows in batches of {batch_size}")
        
        for i in range(0, total_rows, batch_size):
            end_idx = min(i + batch_size, total_rows)
            batch = processed_df.iloc[i:end_idx]
            
            for _, row in batch.iterrows():
                try:
                    # Get only the columns with non-null values
                    valid_columns = []
                    valid_values = []
                    valid_placeholders = []
                    
                    for col in processed_columns:
                        if not pd.isna(row[col]):
                            # Handle the id column specially
                            if col.lower() == 'id':
                                # Store as original_id instead
                                valid_columns.append('original_id')
                            else:
                                safe_col = column_mapping[col]
                                valid_columns.append(safe_col)
                            
                            # Convert value to appropriate type for SQL
                            value = row[col]
                            col_dtype_str = str(processed_df[col].dtype)
                            
                            # ENHANCED: Handle different column types properly for SQL Server
                            if 'datetime64' in col_dtype_str:
                                # Handle original datetime columns
                                if pd.isna(value):
                                    value = None
                                else:
                                    # Convert pandas Timestamp to Python datetime for pyodbc
                                    if hasattr(value, 'to_pydatetime'):
                                        # Convert to native Python datetime object
                                        python_dt = value.to_pydatetime()
                                        
                                        # Remove timezone info if present (SQL Server DATETIME2 doesn't store timezone)
                                        if python_dt.tzinfo is not None:
                                            python_dt = python_dt.replace(tzinfo=None)
                                        
                                        value = python_dt
                                        print(f"   Converted original datetime: {value} (type: {type(value)})")
                                    else:
                                        # Fallback: convert to string in ISO format
                                        value = pd.to_datetime(value).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Remove last 3 digits for milliseconds
                                        print(f"   Fallback datetime conversion: {value}")
                            elif col in separated_date_columns:
                                # Handle separated date columns (date component only)
                                if pd.isna(value):
                                    value = None
                                else:
                                    # Convert date object to date for SQL Server DATE type
                                    if hasattr(value, 'date'):
                                        value = value.date() if hasattr(value, 'date') else value
                                    print(f"    Converted separated date: {value} (type: {type(value)})")
                            elif col in separated_hour_columns:
                                # Handle separated time columns (time component only)
                                if pd.isna(value):
                                    value = None
                                else:
                                    # Convert time object to time for SQL Server TIME type
                                    if hasattr(value, 'time'):
                                        value = value.time() if hasattr(value, 'time') else value
                                    print(f"    Converted separated time: {value} (type: {type(value)})")
                            elif col in ip_columns:
                                value = str(value) if not pd.isna(value) else None
                                print(f"    IP Column '{col}' converted to string: {value}")
                            elif isinstance(value, (np.int64, np.int32)):
                                value = int(value)
                            elif isinstance(value, (np.float64, np.float32)):
                                value = float(value)
                            elif isinstance(value, bool):
                                value = int(value)
                            
                            # Add value and placeholder
                            valid_values.append(value)
                            valid_placeholders.append('?')
                    
                    # Only insert if we have valid column data
                    if valid_columns:
                        # Insert into processed_data table
                        column_str = ', '.join([f"[{col}]" for col in valid_columns])
                        placeholder_str = ', '.join(valid_placeholders)
                        
                        insert_sql = f'''
                        INSERT INTO [{table_name}]
                        ({column_str})
                        VALUES ({placeholder_str})
                        '''
                    
                        cursor.execute(insert_sql, valid_values)
                        processed_count += 1
                    else:
                        skipped_count += 1
                    
                except Exception as e:
                    print(f"Error processing row {processed_count + skipped_count + error_count}: {e}")
                    print(f"   Row data: {dict(row)}")
                    error_count += 1
            
            # Commit batch
            db.commit()
            if i // batch_size % 10 == 0:  # Print every 10 batches
                print(f"Processed batch {i//batch_size + 1}/{(total_rows-1)//batch_size + 1} ({processed_count} rows processed so far)")
        
        print(f"✅ Completed data insertion: {processed_count} processed, {skipped_count} skipped, {error_count} errors")
        
        return {
            "processed_count": processed_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
            "total_count": processed_count + skipped_count + error_count,
            "processed_table": table_name,
            "original_datetime_columns": original_datetime_columns,
            "separated_date_columns": separated_date_columns,
            "separated_hour_columns": separated_hour_columns,
            "ip_columns": ip_columns
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
    
def append_to_existing_table(df, table_name, batch_size=1000):
    """
    Appends a DataFrame to an existing table, skipping rows that are exact
    duplicates of existing records based on ALL common columns.

    Args:
        df (pd.DataFrame): The preprocessed DataFrame to append.
        table_name (str): The name of the existing target table.
        batch_size (int): Kept for signature consistency.

    Returns:
        dict: A dictionary of processing statistics.
    """
    db = get_db()
    cursor = db.cursor()
    temp_table_name = f"#{table_name}_temp_insert"

    try:
        # Step 1: Get the FULL schema of the target table with the CORRECTED QUERY.
        # This new query builds the full data type string (e.g., 'nvarchar(255)').
        get_schema_sql = """
        SELECT
            c.name,
            CASE
                WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary')
                    THEN CONCAT(UPPER(t.name), '(', IIF(c.max_length = -1, 'MAX', CAST(c.max_length AS VARCHAR(10))), ')')
                WHEN t.name IN ('decimal', 'numeric')
                    THEN CONCAT(UPPER(t.name), '(', c.precision, ',', c.scale, ')')
                WHEN t.name IN ('time', 'datetime2', 'datetimeoffset')
                    THEN CONCAT(UPPER(t.name), '(', c.scale, ')')
                ELSE UPPER(t.name)
            END AS data_type
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(?)
        """
        cursor.execute(get_schema_sql, table_name)
        
        table_schema = cursor.fetchall()
        if not table_schema:
             return {"error": f"Could not retrieve columns for table '{table_name}'.", "status_code": 500}
        
        target_columns_with_types = {row.name: row.data_type for row in table_schema}
        target_columns = set(target_columns_with_types.keys())

        # Step 2: Sanitize DataFrame columns and find ALL common columns.
        df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
        common_columns = list(set(df.columns).intersection(target_columns))
        
        if not common_columns:
            return {"error": "No matching columns found between the file and the target table.", "status_code": 400}

        # Step 3: Create a temporary table.
        create_temp_cols = [f"[{col}] {target_columns_with_types[col]}" for col in common_columns]
        cursor.execute(f"IF OBJECT_ID('tempdb..{temp_table_name}') IS NOT NULL DROP TABLE {temp_table_name}")
        cursor.execute(f"CREATE TABLE {temp_table_name} ({', '.join(create_temp_cols)})")
        
        # Step 4: Bulk insert into the temporary table.
        records_to_insert = [tuple(row) for row in df[common_columns].itertuples(index=False)]
        insert_sql = f"INSERT INTO {temp_table_name} ({', '.join(f'[{c}]' for c in common_columns)}) VALUES ({', '.join(['?' for _ in common_columns])})"
        
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, records_to_insert)
        print(f"Staged {len(records_to_insert)} records in a temporary table.")

        # Step 5: Insert from temp table, using ALL common columns for the duplicate check.
        join_conditions = " AND ".join(
            f"(T.[{col}] = S.[{col}] OR (T.[{col}] IS NULL AND S.[{col}] IS NULL))" for col in common_columns
        )
        
        deduplicate_sql = f"""
        INSERT INTO [{table_name}] ({', '.join(f'[{c}]' for c in common_columns)})
        SELECT {', '.join(f'S.[{c}]' for c in common_columns)}
        FROM {temp_table_name} AS S
        WHERE NOT EXISTS (
            SELECT 1 FROM [{table_name}] AS T WHERE {join_conditions}
        )
        """
        
        cursor.execute(deduplicate_sql)
        inserted_count = cursor.rowcount
        db.commit()

        # Step 6: Clean up the temporary table.
        cursor.execute(f"DROP TABLE {temp_table_name}")
        db.commit()

        print(f"Successfully inserted {inserted_count} new, unique records.")
        return {
            "processed_count": inserted_count,
            "skipped_duplicate_count": len(records_to_insert) - inserted_count,
            "total_rows_in_file": len(df),
            "target_table": table_name,
            "checked_columns": common_columns
        }

    except Exception as e:
        db.rollback()
        print(f"A critical error occurred: {e}")
        traceback.print_exc()
        try:
            cursor.execute(f"IF OBJECT_ID('tempdb..{temp_table_name}') IS NOT NULL DROP TABLE {temp_table_name}")
            db.commit()
        except Exception as cleanup_e:
            print(f"Failed to cleanup temp table on error: {cleanup_e}")
        return {"error": str(e), "status_code": 500}
    
def get_table_columns(table_name):
    """
    Retrieves the column names for a given table from the database.

    Args:
        table_name (str): The name of the table to inspect.

    Returns:
        list or dict: A list of column names, or an error dictionary if the table is not found.
    """
    db = get_db()
    cursor = db.cursor()
    try:
        # Verify the target table exists
        cursor.execute("SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(?) AND type in (N'U')", table_name)
        if cursor.fetchone() is None:
            return {"error": f"Target table '{table_name}' does not exist in the database."}

        # Get column names, ordered by their position in the table for consistency
        query = "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id"
        cursor.execute(query, table_name)
        columns = [row.name for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"Error getting columns for table '{table_name}': {e}")
        traceback.print_exc()
        return {"error": str(e)}