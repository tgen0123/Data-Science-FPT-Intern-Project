import os
import traceback
from datetime import datetime
import pandas as pd
from flask import Blueprint, jsonify, request, current_app
import data_services
import database
import utils
import json
from auth import require_admin

data_bp = Blueprint('data', __name__, url_prefix='/api/data')

def _get_load_request_params():
    """
    Helper to extract file path/name, batch size, and max rows from a request.
    This version correctly handles type conversion for all request types.
    """
    file_identifier = None
    identifier_key = None
    data = {}

    # Consolidate request data source
    if request.is_json:
        data = request.get_json() or {}
    elif request.form:
        data = request.form
    elif request.args:
        data = request.args

    # Determine if we're looking for file_path or file_name
    if 'file_path' in data:
        identifier_key = 'file_path'
    elif 'file_name' in data:
        identifier_key = 'file_name'

    if identifier_key:
        file_identifier = data.get(identifier_key)

    # --- CORRECTED TYPE CONVERSION LOGIC ---
    # Get the value first, then safely convert its type.

    # Handle batch_size
    batch_size = 1000  # Default value
    raw_batch_size = data.get('batch_size')
    if raw_batch_size is not None:
        try:
            batch_size = int(raw_batch_size)
        except (ValueError, TypeError):
            # Keep default if conversion fails
            pass

    # Handle max_rows
    max_rows = None  # Default value
    raw_max_rows = data.get('max_rows')
    if raw_max_rows is not None and str(raw_max_rows).strip():
        try:
            max_rows = int(raw_max_rows)
        except (ValueError, TypeError):
            # Keep default if conversion fails
            pass

    return identifier_key, file_identifier, batch_size, max_rows

@data_bp.route('/load', methods=['POST'])
@require_admin
def load_data_endpoint():
    """
    Load and process a CSV file by its full path.
    """
    key, file_path, batch_size, max_rows = _get_load_request_params()

    if key != 'file_path' or not file_path:
        return jsonify({"error": "A 'file_path' is required."}), 400

    try:
        result = data_services.process_and_load_csv(file_path, batch_size, max_rows)

        if "error" in result:
            return jsonify({"success": False, "error": result["error"]}), 500
        else:
            safe_response_data = utils.convert_numpy_types(result)
            return jsonify({
                "success": True,
                "message": f"Successfully processed {result.get('processed_count', 0)} records.",
                "details": safe_response_data
            })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def get_file_details(file_id):
    """Get details about a specific CSV file by ID."""
    try:
        cursor = database.get_cursor()
        cursor.execute('''
        SELECT id, file_name, file_path, row_count, column_count, loaded_at, is_processed, 
               date_columns_detected, ip_columns_detected, datetime_separation_info
        FROM csv_registry WHERE id = ?
        ''', (file_id,))
        
        file_info = cursor.fetchone()
        if not file_info:
            return jsonify({"error": "File not found"}), 404

        # Dynamically build the response
        columns = [column[0] for column in cursor.description]
        details = dict(zip(columns, file_info))

        # Safely parse JSON fields
        for key in ['date_columns_detected', 'ip_columns_detected', 'datetime_separation_info']:
            if details.get(key):
                try:
                    details[key] = json.loads(details[key])
                except (json.JSONDecodeError, TypeError):
                    details[key] = {"error": "Could not decode stored JSON data."}
        
        return jsonify(details)
        
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@data_bp.route('/stats', methods=['GET'])
@require_admin
def csv_file_stats():
    """Get statistics about loaded CSV files."""
    try:
        cursor = database.get_cursor()
        cursor.execute('SELECT * FROM csv_registry ORDER BY loaded_at DESC')
        
        columns = [column[0] for column in cursor.description]
        files = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return jsonify({
            "total_files": len(files),
            "files": files
        })
        
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@data_bp.route('/file-details/name/<path:file_name>', methods=['GET'])
@require_admin
def get_file_details_by_name(file_name):
    """Get details about a specific CSV file by name."""
    try:
        file_id = database.get_file_id_by_name(file_name)
        if file_id is None:
            return jsonify({"error": f"File '{file_name}' not found in registry"}), 404
        return get_file_details(file_id)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


def get_processed_sample(file_id):
    """Get sample data from a processed table by file ID."""
    try:
        cursor = database.get_cursor()
        sanitized_name_query = "SELECT file_name FROM csv_registry WHERE id = ?"
        cursor.execute(sanitized_name_query, (file_id,))
        file_name_row = cursor.fetchone()
        if not file_name_row:
             return jsonify({"error": f"File ID {file_id} not found in registry"}), 404

        sanitized_name = utils.sanitize_for_table_name(file_name_row[0])
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

@data_bp.route('/processed-table-data/name/<path:file_name>', methods=['GET'])
@require_admin
def get_processed_sample_by_name(file_name):
    """Get sample data from a processed table by file name."""
    try:
        file_id = database.get_file_id_by_name(file_name)
        if file_id is None:
            return jsonify({"error": f"File '{file_name}' not found in registry"}), 404
        return get_processed_sample(file_id)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    
@data_bp.route('/load-to-existing', methods=['POST'])
@require_admin
def load_to_existing_table_endpoint():
    """
    Load data from a CSV file and append it to an existing SQL table.
    Expects a JSON body with 'file_path' and 'target_table'.
    """
    data = request.get_json()
    if not data or 'file_path' not in data or 'target_table' not in data:
        return jsonify({"error": "Request body must include 'file_path' and 'target_table'"}), 400

    file_path = data.get('file_path')
    target_table = data.get('target_table')

    try:
        # Check if the file exists locally before starting
        if not os.path.exists(file_path):
             return jsonify({"error": f"File '{file_path}' not found on the server"}), 404

        result = data_services.process_and_load_to_existing_table(file_path, target_table)

        if "error" in result:
            # Use a specific status code if the service layer provides one
            status_code = result.get("status_code", 500)
            return jsonify({"success": False, "error": result["error"]}), status_code
        else:
            safe_response_data = utils.convert_numpy_types(result)
            return jsonify({
                "success": True,
                "message": f"Successfully appended {result.get('processed_count', 0)} records to '{target_table}'.",
                "details": safe_response_data
            })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500