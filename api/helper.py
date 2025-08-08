from datetime import datetime
from typing import List
from flask import request, jsonify
from authentication import generate_api_key
import database
import helper
import traceback

# --- keys.py helper ---

def create_new_api_key(username, rate_limit=100, is_admin=False):
    """
    Create a new API key for a user and store in database.
    
    Args:
        username (str): Username to associate with the key
        rate_limit (int, optional): Maximum requests per timeframe. Defaults to 100.
        is_admin (bool, optional): Whether the key has admin privileges. Defaults to False.
        
    Returns:
        str: The newly generated API key
    """
    new_key = generate_api_key()
    
    cursor = database.get_cursor()
    cursor.execute(
        'INSERT INTO api_keys ([key], username, rate_limit, is_admin) VALUES (?, ?, ?, ?)',
        (new_key, username, rate_limit, 1 if is_admin else 0)
    )
    db = database.get_db()
    db.commit()
    
    return new_key

# --- splunk.py helper ---

def parse_splunk_trigger_time(time_str: str) -> datetime:
    """
    Parses a time string from a Splunk webhook into a datetime object.

    This function iterates through a list of common time formats that Splunk uses.
    It handles various precisions, timezones, and the 'Z' (Zulu/UTC) suffix.
    If the provided string is empty or none of the formats match, it defaults
    to the current time.

    Args:
        time_str: The time string received from Splunk.

    Returns:
        A datetime object representing the parsed time, or the
        current time if parsing fails.
    """
    if not time_str:
        return datetime.now()
    
    # Common Splunk time formats
    time_formats: List[str] = [
        '%Y-%m-%d %H:%M:%S',           # 2024-07-02 10:30:00
        '%Y-%m-%dT%H:%M:%S',           # 2024-07-02T10:30:00
        '%Y-%m-%dT%H:%M:%SZ',          # 2024-07-02T10:30:00Z
        '%Y-%m-%dT%H:%M:%S.%f',        # 2024-07-02T10:30:00.123456
        '%Y-%m-%dT%H:%M:%S.%fZ',       # 2024-07-02T10:30:00.123456Z
        '%Y-%m-%dT%H:%M:%S%z',         # 2024-07-02T10:30:00+0700
        '%Y-%m-%dT%H:%M:%S.%f%z',      # 2024-07-02T10:30:00.123456+0700
    ]
    
    for fmt in time_formats:
        try:
            if time_str.endswith('Z'):
                time_str_clean: str = time_str[:-1]
                return datetime.strptime(time_str_clean, fmt.replace('Z', ''))
            else:
                return datetime.strptime(time_str, fmt)
        except ValueError:
            continue
    
    try:
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except:
        print(f"Could not parse time string: {time_str}, using current time")
        return datetime.now()

# --- data.py helper ---

def _get_load_request_params():
    """
    Helper to extract file path/name, batch size, and max rows from a request.
    This version correctly handles type conversion for all request types.
    """
    file_identifier = None
    identifier_key = None
    data = {}

    if request.is_json:
        data = request.get_json() or {}
    elif request.form:
        data = request.form
    elif request.args:
        data = request.args

    if 'file_path' in data:
        identifier_key = 'file_path'
    elif 'file_name' in data:
        identifier_key = 'file_name'

    if identifier_key:
        file_identifier = data.get(identifier_key)

    batch_size = 1000 
    raw_batch_size = data.get('batch_size')
    if raw_batch_size is not None:
        try:
            batch_size = int(raw_batch_size)
        except (ValueError, TypeError):
            pass

    max_rows = None 
    raw_max_rows = data.get('max_rows')
    if raw_max_rows is not None and str(raw_max_rows).strip():
        try:
            max_rows = int(raw_max_rows)
        except (ValueError, TypeError):
            pass

    return identifier_key, file_identifier, batch_size, max_rows

def get_processed_sample(file_id):
    """Get sample data from a processed table by file ID."""
    try:
        cursor = database.get_cursor()
        sanitized_name_query = "SELECT file_name FROM csv_registry WHERE id = ?"
        cursor.execute(sanitized_name_query, (file_id,))
        file_name_row = cursor.fetchone()
        if not file_name_row:
             return jsonify({"error": f"File ID {file_id} not found in registry"}), 404

        sanitized_name = helper.sanitize_for_table_name(file_name_row[0])
        table_name = f"{sanitized_name}"

        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)

        cursor.execute(f"SELECT * FROM [{table_name}] ORDER BY id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", (offset, limit))
        
        columns = [column[0] for column in cursor.description]
        data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return jsonify({
            "table_name": table_name,
            "data": data,
            "limit": limit,
            "offset": offset
        })
        
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Could not retrieve data. The processed table may not exist or the query failed. Details: {e}"}), 500
