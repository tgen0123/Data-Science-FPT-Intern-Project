# data_loading.py
import pandas as pd
import json
from flask import Blueprint, jsonify, request, g, current_app
from functools import wraps
import pyodbc
import numpy as np
from datetime import datetime
import os

# Import preprocessing module
from data_preprocessor import preprocess_data, clean_username, extract_department
from data_preprocessor import deduplicate_columns, analyze_correlation, group_equal_object_columns, detect_noisy_columns
from helper import get_db, get_cursor, dict_from_row, require_api_key, is_admin

# Create a Blueprint for data loading and preprocessing routes
data_bp = Blueprint('data', __name__, url_prefix='/api/data')

# ---- CSV File Management ----
# Keep track of loaded CSV files
csv_files = {}

# ---- Raw Data Loading Functions ----

def get_unique_column_names(columns):
    """
    Convert column names to SQL-safe format and ensure uniqueness.
    
    Args:
        columns (list): Original column names
        
    Returns:
        dict: Mapping from original column names to unique SQL-safe column names
    """
    unique_columns = set()
    col_mapping = {}
    
    for col in columns:
        # Sanitize column name
        original_col = col
        clean_col = str(col).lower().replace(' ', '_').replace('-', '_').replace('.', '_')
        
        # Handle duplicate column names
        base_col = clean_col
        counter = 1
        while clean_col in unique_columns:
            clean_col = f"{base_col}_{counter}"
            counter += 1
        
        unique_columns.add(clean_col)
        col_mapping[original_col] = clean_col
    
    return col_mapping

def load_raw_csv_to_db(file_path):
    """
    Load raw CSV data into the database without preprocessing.
    Store all columns from the CSV.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        tuple: (success, message, count)
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        
        # Store the file path for later processing
        file_name = file_path.split('/')[-1]
        csv_files[file_name] = file_path
        
        db = get_db()
        cursor = db.cursor()
        
        # Create csv_registry table if it doesn't exist
        # This table will store information about loaded CSV files
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
                [is_processed] BIT DEFAULT 0
            )
            
            CREATE INDEX [idx_csv_registry_is_processed] ON [dbo].[csv_registry] ([is_processed])
        END
        ''')
        
        # Check if this file is already registered
        cursor.execute('SELECT id FROM csv_registry WHERE file_path = ?', (file_path,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing entry
            cursor.execute('''
            UPDATE csv_registry 
            SET row_count = ?, column_count = ?, loaded_at = GETDATE()
            WHERE file_path = ?
            ''', (len(df), len(df.columns), file_path))
            file_id = existing[0]
        else:
            # Insert new entry
            cursor.execute('''
            INSERT INTO csv_registry (file_name, file_path, row_count, column_count)
            VALUES (?, ?, ?, ?)
            ''', (file_name, file_path, len(df), len(df.columns)))
            
            # Get the ID of the inserted file
            cursor.execute('SELECT @@IDENTITY')
            file_id = cursor.fetchone()[0]
        
        # Define reserved column names (that we're adding ourselves)
        reserved_columns = {'id'}
        
        # Get unique column names
        unique_columns = set()
        col_mapping = {}
        
        for col in df.columns:
            # Sanitize column name
            original_col = col
            clean_col = str(col).lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            
            # Handle reserved column names by prefixing them with 'csv_'
            if clean_col in reserved_columns:
                clean_col = f"csv_{clean_col}"
            
            # Handle duplicate column names
            base_col = clean_col
            counter = 1
            while clean_col in unique_columns or clean_col in reserved_columns:
                clean_col = f"{base_col}_{counter}"
                counter += 1
            
            unique_columns.add(clean_col)
            col_mapping[original_col] = clean_col
        
        # Create a table for this specific file's raw data
        table_name = f"raw_data_{file_id}"
        
        # Create columns list for the table based on CSV columns
        column_defs = []
        for original_col, clean_col in col_mapping.items():
            # Use NVARCHAR(MAX) for all columns initially
            column_defs.append(f"[{clean_col}] NVARCHAR(MAX)")
        
        # Create table
        create_table_sql = f'''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[{table_name}] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                {', '.join(column_defs)}
            )
        END
        '''
        cursor.execute(create_table_sql)
        db.commit()
        
        # Insert data in batches
        count = 0
        batch_size = 1000
        
        # Process in batches
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            for _, row in batch_df.iterrows():
                # Prepare column names and placeholders
                col_names = []
                placeholders = []
                values = []
                
                for original_col, clean_col in col_mapping.items():
                    col_names.append(f"[{clean_col}]")
                    placeholders.append('?')
                    values.append(str(row[original_col]) if not pd.isna(row[original_col]) else None)
                
                # Insert row
                insert_sql = f'''
                INSERT INTO [{table_name}] ({', '.join(col_names)})
                VALUES ({', '.join(placeholders)})
                '''
                cursor.execute(insert_sql, values)
                
                count += 1
            
            # Commit batch
            db.commit()
            print(f"Inserted batch {i//batch_size + 1}/{(len(df)-1)//batch_size + 1} ({count} rows so far)")
        
        return True, f"Successfully loaded {count} rows from {file_path}", count
    
    except Exception as e:
        print(f"Error loading raw CSV: {e}")
        return False, str(e), 0

# ---- Data Preprocessing Functions ----

def preprocess_table_data(file_id, batch_size=1000, max_rows=None):
    """
    Preprocess data from a raw_data table using the data_preprocessor module.
    Creates a table with columns based on only preprocessed data without duplicate suffixes.
    
    Args:
        file_id (int): ID of the file in csv_registry
        batch_size (int): Number of rows to process in a batch
        max_rows (int): Maximum number of rows to process, None for all
        
    Returns:
        dict: Processing statistics
    """
    try:
        import os
        from data_preprocessor import preprocess_data
        
        db = get_db()
        cursor = db.cursor()
        
        # Check if the file exists in the registry
        cursor.execute('''
        SELECT file_name, file_path, row_count
        FROM csv_registry
        WHERE id = ?
        ''', (file_id,))
        
        file_info = cursor.fetchone()
        if not file_info:
            return {
                "error": f"File ID {file_id} not found in registry",
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        file_name, file_path, total_rows = file_info
        
        # Check if the CSV file exists
        if not file_path or not os.path.exists(file_path):
            return {
                "error": f"CSV file {file_path} not found. Please check if the file exists at the path recorded in the registry.",
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Use the data_preprocessor module to preprocess the data from the CSV file
        print(f"Preprocessing CSV file: {file_path} using data_preprocessor.py")
        processed_df = preprocess_data(file_path)
        
        if processed_df is None:
            return {
                "error": "Failed to preprocess file using data_preprocessor.preprocess_data()",
                "processed_count": 0,
                "skipped_count": 0,
                "error_count": 0,
                "total_count": 0
            }
        
        # Get columns from the preprocessed DataFrame
        processed_columns = processed_df.columns.tolist()
        print(f"Preprocessed data has {len(processed_columns)} columns: {processed_columns}")
        
        # Create a map of column names to their SAFE SQL names, without adding duplicate suffixes
        # Skip column named 'id' if it exists in the preprocessed data to avoid conflict with primary key
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
        processed_table_name = f"processed_data_{file_id}"
        
        cursor.execute(f'''
        IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{processed_table_name}]') AND type in (N'U'))
        DROP TABLE [dbo].[{processed_table_name}]
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
            
            # Map pandas dtypes to SQL Server types
            if col_type == 'datetime64[ns]' or col in ['timestamp', 'systemtime', '_time']:
                sql_type = "DATETIME"
            elif col_type == 'float64' or col_type == 'float32':
                sql_type = "FLOAT"
            elif col_type == 'int64' or col_type == 'int32':
                if col == 'hour_of_day':
                    sql_type = "TINYINT"  # 0-255, enough for hours
                else:
                    sql_type = "BIGINT"  # Use BIGINT to avoid overflow
            elif col_type == 'bool':
                sql_type = "BIT"
            else:
                # For string/object types, use appropriate length
                if col in ['source_ip', 'callingstationid']:
                    sql_type = "NVARCHAR(45)"  # IPv6 addresses can be up to 45 chars
                elif col in ['time_category']:
                    sql_type = "NVARCHAR(50)"
                elif 'username' in col or col == 'user' or col == 'name':
                    sql_type = "NVARCHAR(255)" 
                elif col in ['department', 'session_id', 'vpn_gateway', 'nasidentifier', 'accountsessionidentifier']:
                    sql_type = "NVARCHAR(255)"
                else:
                    # For other text columns or unknown types
                    sql_type = "NVARCHAR(MAX)"
            
            # Note: We don't add NOT NULL constraint to avoid insertion errors
            column_definitions.append(f"[{safe_col}] {sql_type}")
        
        # Add original_id column if needed (if there was an id column in the data)
        if 'id' in processed_columns:
            column_definitions.append("[original_id] BIGINT")
            
        # Add processed_at timestamp column
        column_definitions.append("[processed_at] DATETIME DEFAULT GETDATE()")
        
        # Create the table
        create_table_sql = f'''
        CREATE TABLE [dbo].[{processed_table_name}] (
            {', '.join(column_definitions)}
        )
        '''
        
        cursor.execute(create_table_sql)
        
        # Create indexes on key columns
        for idx_col in ['username', 'subjectusername', 'source_ip', 'callingstationid', 'time_category']:
            if idx_col in processed_columns:
                safe_col = column_mapping[idx_col]
                cursor.execute(f'''
                CREATE INDEX [idx_{processed_table_name}_{safe_col}] 
                ON [dbo].[{processed_table_name}] ([{safe_col}])
                ''')
        
        db.commit()
        
        # Insert data into processed table
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        # Process in batches
        total_rows = len(processed_df)
        rows_to_process = min(total_rows, max_rows) if max_rows else total_rows
        
        for i in range(0, rows_to_process, batch_size):
            end_idx = min(i + batch_size, rows_to_process)
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
                            
                            # Handle special cases
                            if col == 'timestamp' and isinstance(value, str):
                                value = value.replace('+07:00', '')
                                try:
                                    value = pd.to_datetime(value)
                                except:
                                    value = None
                            elif col == 'hour_of_day':
                                try:
                                    hour_float = float(value)
                                    if hour_float.is_integer() and 0 <= hour_float <= 23:
                                        value = int(hour_float)
                                    else:
                                        value = None
                                except:
                                    value = None
                            
                            # Add value and placeholder
                            valid_values.append(value)
                            valid_placeholders.append('?')
                    
                    # Check if we have enough data to insert
                    if 'subjectusername' in processed_columns and 'callingstationid' in processed_columns:
                        username_idx = processed_columns.index('subjectusername')
                        ip_idx = processed_columns.index('callingstationid')
                        
                        if pd.isna(row[processed_columns[username_idx]]) or pd.isna(row[processed_columns[ip_idx]]):
                            skipped_count += 1
                            continue
                    
                    # Only insert if we have data
                    if valid_columns:
                        # Insert into processed_data table
                        column_str = ', '.join([f"[{col}]" for col in valid_columns])
                        placeholder_str = ', '.join(valid_placeholders)
                        
                        insert_sql = f'''
                        INSERT INTO [{processed_table_name}]
                        ({column_str})
                        VALUES ({placeholder_str})
                        '''
                        
                        cursor.execute(insert_sql, valid_values)
                        processed_count += 1
                    else:
                        skipped_count += 1
                    
                except Exception as e:
                    print(f"Error processing row: {e}")
                    error_count += 1
            
            # Commit batch
            db.commit()
            print(f"Processed batch {i//batch_size + 1}/{(rows_to_process-1)//batch_size + 1} ({processed_count} rows processed so far)")
        
        # Update csv_registry to mark file as processed
        cursor.execute('''
        UPDATE csv_registry
        SET is_processed = 1
        WHERE id = ?
        ''', (file_id,))
        db.commit()
        
        return {
            "processed_count": processed_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
            "total_count": processed_count + skipped_count + error_count,
            "processed_table": processed_table_name
        }
        
    except Exception as e:
        print(f"Error preprocessing table data: {e}")
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "processed_count": 0,
            "skipped_count": 0,
            "error_count": 0,
            "total_count": 0
        }

# ---- API Endpoints ----

@data_bp.route('/load', methods=['POST'])
@require_api_key
def load_raw_data_endpoint():
    """
    Load raw VPN data from a CSV file into the database without preprocessing.
    
    Requires an admin API key.
    """
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    # Try to get file_path from different request formats
    file_path = None
    
    # Check if JSON data
    if request.is_json:
        data = request.get_json()
        if data and 'file_path' in data:
            file_path = data['file_path']
    
    # Check form data if file_path not found
    if file_path is None and request.form and 'file_path' in request.form:
        file_path = request.form['file_path']
    
    # Check query params if still not found
    if file_path is None and 'file_path' in request.args:
        file_path = request.args.get('file_path')
    
    # If still not found, return error
    if file_path is None:
        return jsonify({
            "error": "file_path required in request body, form data, or query parameters",
            "help": "Set Content-Type to application/json and provide {'file_path': 'your/file/path.csv'} in the request body"
        }), 400
        
    try:
        success, message, count = load_raw_csv_to_db(file_path)
        
        if success:
            return jsonify({
                "success": True,
                "raw_records_loaded": count,
                "message": message
            })
        else:
            return jsonify({
                "success": False,
                "error": message
            }), 500
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@data_bp.route('/preprocess-db', methods=['POST'])
@require_api_key
def preprocess_db_data_endpoint():
    """
    Process data from the database raw_data table and load results into a separate processed_data table.
    Each file gets its own processed_data_{file_id} table.
    
    Requires an admin API key.
    """
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    # Try to get file_id from different request formats
    file_id = None
    batch_size = 1000
    max_rows = None
    
    # Check if JSON data
    if request.is_json:
        data = request.get_json()
        if data:
            if 'file_id' in data:
                file_id = data['file_id']
            if 'batch_size' in data:
                batch_size = data['batch_size']
            if 'max_rows' in data:
                max_rows = data['max_rows']
    
    # Check form data if file_id not found
    if file_id is None and request.form:
        if 'file_id' in request.form:
            file_id = int(request.form['file_id'])
        if 'batch_size' in request.form:
            batch_size = int(request.form['batch_size'])
        if 'max_rows' in request.form:
            max_rows = int(request.form['max_rows'])
    
    # Check query params if still not found
    if file_id is None and 'file_id' in request.args:
        file_id = int(request.args.get('file_id'))
        if 'batch_size' in request.args:
            batch_size = int(request.args.get('batch_size'))
        if 'max_rows' in request.args:
            max_rows = int(request.args.get('max_rows'))
    
    # If still not found, return error
    if file_id is None:
        return jsonify({
            "error": "file_id required in request body, form data, or query parameters",
            "help": "Set Content-Type to application/json and provide {'file_id': 1} in the request body"
        }), 400
    
    try:
        result = preprocess_table_data(file_id, batch_size, max_rows)
        
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
                "message": f"Successfully processed {result['processed_count']} records into {result.get('processed_table', 'processed_data_' + str(file_id))}",
                "stats": {
                    "processed": result["processed_count"],
                    "skipped": result["skipped_count"],
                    "errors": result["error_count"],
                    "total": result["total_count"]
                },
                "processed_table": result.get('processed_table', 'processed_data_' + str(file_id))
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

@data_bp.route('/processed-tables', methods=['GET'])
@require_api_key
def get_processed_tables():
    """Get a list of all processed data tables."""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    try:
        cursor = get_cursor()
        
        # Query all tables that start with processed_data_
        cursor.execute('''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'processed_data_%'
        ORDER BY table_name
        ''')
        
        tables = [row[0] for row in cursor.fetchall()]
        
        # Get record counts for each table
        table_stats = []
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
            count = cursor.fetchone()[0]
            
            # Extract file_id from table name
            file_id = int(table.replace('processed_data_', ''))
            
            # Get file info
            cursor.execute('''
            SELECT file_name, file_path
            FROM csv_registry
            WHERE id = ?
            ''', (file_id,))
            
            file_info = cursor.fetchone()
            if file_info:
                file_name, file_path = file_info
            else:
                file_name = None
                file_path = None
            
            table_stats.append({
                "table_name": table,
                "file_id": file_id,
                "file_name": file_name,
                "file_path": file_path,
                "record_count": count
            })
        
        return jsonify({
            "processed_tables": table_stats,
            "total_tables": len(table_stats)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
@data_bp.route('/raw-data/<int:file_id>', methods=['GET'])
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
        
        # Get table structure for the raw data
        raw_table_name = f"raw_data_{file_id}"
        
        cursor.execute(f'''
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{raw_table_name}'
        ORDER BY ORDINAL_POSITION
        ''')
        
        raw_columns = [row[0] for row in cursor.fetchall()]
        
        # Get sample rows from raw data (limit to 5)
        raw_sample_rows = []
        
        if raw_columns:
            # Skip the 'id' column
            columns_list = ', '.join([f"[{col}]" for col in raw_columns if col != 'id'])
            
            cursor.execute(f'''
            SELECT TOP 5 {columns_list}
            FROM [{raw_table_name}]
            ''')
            
            for row in cursor.fetchall():
                sample_row = {}
                for i, col in enumerate([c for c in raw_columns if c != 'id']):
                    sample_row[col] = row[i]
                raw_sample_rows.append(sample_row)
        
        return jsonify({
            "file_id": file_info[0],
            "file_name": file_info[1],
            "file_path": file_info[2],
            "row_count": file_info[3],
            "column_count": file_info[4],
            "loaded_at": file_info[5].isoformat() if file_info[5] else None,
            "is_processed": bool(file_info[6]),
            "raw_table": raw_table_name,
            "raw_columns": raw_columns,
            "raw_sample": raw_sample_rows,
            "processed_table": processed_table_name if processed_table_exists else None,
            "processed_count": processed_count
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
        
        # Get all data
        cursor.execute(f'''
        SELECT *
        FROM [{processed_table_name}]
        ORDER BY id
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
            "total_records": total_records
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500