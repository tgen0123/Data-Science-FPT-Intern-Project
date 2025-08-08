import os
import traceback
from flask import Blueprint, jsonify, request
import data_handler
import database
import helper
from authentication import require_api_key, require_admin
from .helper import _get_load_request_params, get_processed_sample

data_bp = Blueprint('data', __name__, url_prefix='/api/data')

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
        result = data_handler.process_and_load_csv(file_path, batch_size, max_rows)

        if "error" in result:
            return jsonify({"success": False, "error": result["error"]}), 500
        else:
            safe_response_data = helper.convert_numpy_types(result)
            return jsonify({
                "success": True,
                "message": f"Successfully processed {result.get('processed_count', 0)} records.",
                "details": safe_response_data
            })

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


@data_bp.route('/<path:file_name>', methods=['GET'])
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
        if not os.path.exists(file_path):
             return jsonify({"error": f"File '{file_path}' not found on the server"}), 404

        result = data_handler.process_and_load_to_existing_table(file_path, target_table)

        if "error" in result:
            status_code = result.get("status_code", 500)
            return jsonify({"success": False, "error": result["error"]}), status_code
        else:
            safe_response_data = helper.convert_numpy_types(result)
            return jsonify({
                "success": True,
                "message": f"Successfully appended {result.get('processed_count', 0)} records to '{target_table}'.",
                "details": safe_response_data
            })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500