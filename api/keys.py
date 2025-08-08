from flask import Blueprint, jsonify, request
from authentication import require_api_key, require_admin
from database import get_db, get_cursor, dict_from_row
from .helper import create_new_api_key

keys_bp = Blueprint('api', __name__, url_prefix='/api')

@keys_bp.route('/key/generate', methods=['POST'])
@require_api_key
@require_admin
def create_api_key():
    """
    Generate a new API key - restricted to admin users.
    
    This endpoint creates a new API key associated with the specified username
    and configuration. Access is restricted to API keys with admin privileges.
    """
    data = request.get_json()
    if not data or 'username' not in data:
        return jsonify({"error": "Username required"}), 400
        
    rate_limit = data.get('rate_limit', 100)  # Default rate limit
    is_admin_user = data.get('is_admin', False)  # Default not admin
    
    try:
        new_key = create_new_api_key(
            data['username'], 
            rate_limit=rate_limit,
            is_admin=is_admin_user
        )
        
        return jsonify({
            "key": new_key, 
            "user": data['username'],
            "rate_limit": rate_limit,
            "is_admin": is_admin_user
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@keys_bp.route('/key/<key>', methods=['GET'])
@require_api_key
@require_admin
def get_api_key_details(key):
    """
    Get details about an API key - restricted to admin users.
    
    This endpoint retrieves detailed information about a specific API key.
    Access is restricted to API keys with admin privileges.
    """
    try:
        cursor = get_cursor()
        cursor.execute('SELECT * FROM api_keys WHERE [key] = ?', (key,))
        
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "API key not found"}), 404

        columns = [column[0] for column in cursor.description]
        api_key_details = dict_from_row(row, columns)
        
        return jsonify({
            "key": api_key_details['key'],
            "username": api_key_details['username'],
            "rate_limit": api_key_details['rate_limit'],
            "is_admin": bool(api_key_details['is_admin'])
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@keys_bp.route('/key/update/<key>', methods=['PUT'])
@require_api_key
@require_admin
def update_api_key(key):
    """
    Update an API key's properties - restricted to admin users.
    
    This endpoint updates the properties of an existing API key.
    Access is restricted to API keys with admin privileges.
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No update data provided"}), 400
    
    try:
        cursor = get_cursor()
        cursor.execute('SELECT * FROM api_keys WHERE [key] = ?', (key,))
        
        if not cursor.fetchone():
            return jsonify({"error": "API key not found"}), 404

        update_fields = []
        update_values = []
        
        if 'username' in data:
            update_fields.append("username = ?")
            update_values.append(data['username'])
            
        if 'rate_limit' in data:
            update_fields.append("rate_limit = ?")
            update_values.append(data['rate_limit'])
            
        if 'is_admin' in data:
            update_fields.append("is_admin = ?")
            update_values.append(1 if data['is_admin'] else 0)
        
        if not update_fields:
            return jsonify({"error": "No valid fields to update"}), 400
        
        query = f"UPDATE api_keys SET {', '.join(update_fields)} WHERE [key] = ?"
        update_values.append(key)
        db = get_db()
        cursor = db.cursor()
        cursor.execute(query, update_values)
        db.commit()
        cursor.execute('SELECT * FROM api_keys WHERE [key] = ?', (key,))
        row = cursor.fetchone()
        columns = [column[0] for column in cursor.description]
        updated_key = dict_from_row(row, columns)
        
        return jsonify({
            "message": "API key updated successfully",
            "key": updated_key['key'],
            "username": updated_key['username'],
            "rate_limit": updated_key['rate_limit'],
            "is_admin": bool(updated_key['is_admin'])
        })
    except Exception as e:
        return jsonify({"error": f"Failed to update API key: {str(e)}"}), 500

@keys_bp.route('/key/delete/<key>', methods=['DELETE'])
@require_api_key
@require_admin
def delete_api_key(key):
    """
    Delete an API key - restricted to admin users.
    
    This endpoint permanently deletes an API key from the database.
    Access is restricted to API keys with admin privileges.
    The key being used for authentication cannot be deleted.
    """
    current_key = request.headers.get('X-API-Key')
    if key == current_key:
        return jsonify({"error": "Cannot delete the API key currently in use"}), 400
    
    try:
        cursor = get_cursor()
        cursor.execute('SELECT username FROM api_keys WHERE [key] = ?', (key,))
        
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "API key not found"}), 404
        
        username = row[0]
        db = get_db()
        cursor = db.cursor()
        cursor.execute('DELETE FROM api_keys WHERE [key] = ?', (key,))
        db.commit()
        
        return jsonify({
            "message": "API key deleted successfully",
            "key": key,
            "username": username
        })
    except Exception as e:
        return jsonify({"error": f"Failed to delete API key: {str(e)}"}), 500

@keys_bp.route('/key/list', methods=['GET'])
@require_api_key
@require_admin
def list_api_keys():
    """
    List all API keys - restricted to admin users.
    
    This endpoint retrieves all API keys in the system.
    Access is restricted to API keys with admin privileges.
    """
    try:
        cursor = get_cursor()
        cursor.execute('SELECT * FROM api_keys')
        
        columns = [column[0] for column in cursor.description]
        keys = []
        
        for row in cursor.fetchall():
            key_data = dict_from_row(row, columns)
            keys.append({
                "key": key_data['key'],
                "username": key_data['username'],
                "rate_limit": key_data['rate_limit'],
                "is_admin": bool(key_data['is_admin'])
            })
        
        return jsonify({
            "count": len(keys),
            "keys": keys
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@keys_bp.route('/key/authenticate')
@require_api_key
def authenticate():
    """
    Test endpoint to verify API key is working.
    
    This endpoint serves as a simple way for clients to verify 
    that their API key is valid and to retrieve information about
    their account, including username, rate limit, and admin status.
    """
    return jsonify({
        "authenticated": True,
        "user": request.api_user['user'],
        "rate_limit": request.api_user['rate_limit'],
        "is_admin": request.api_user.get('is_admin', False)
    })