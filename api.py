from flask import Blueprint, jsonify, request, g
from helper import get_location, get_user_records, require_api_key, is_admin, create_new_api_key, get_cursor, dict_from_row, get_db

# Create a Blueprint for API routes
api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/key/generate', methods=['POST'])
@require_api_key
def create_api_key():
    """Generate a new API key - restricted to admin users"""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
        
    # Get info from request
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

@api_bp.route('/key/<key>', methods=['GET'])
@require_api_key
def get_api_key_details(key):
    """Get details about an API key - restricted to admin users"""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    try:
        cursor = get_cursor()
        cursor.execute('SELECT * FROM api_keys WHERE [key] = ?', (key,))
        
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "API key not found"}), 404
        
        # Get column names to create a dictionary
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

@api_bp.route('/key/<key>', methods=['PUT'])
@require_api_key
def update_api_key(key):
    """Update an API key's properties - restricted to admin users"""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    data = request.get_json()
    if not data:
        return jsonify({"error": "No update data provided"}), 400
    
    try:
        # Check if key exists
        cursor = get_cursor()
        cursor.execute('SELECT * FROM api_keys WHERE [key] = ?', (key,))
        
        if not cursor.fetchone():
            return jsonify({"error": "API key not found"}), 404
        
        # Prepare update fields
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
        
        # Build the update query
        query = f"UPDATE api_keys SET {', '.join(update_fields)} WHERE [key] = ?"
        update_values.append(key)
        
        # Execute the update
        db = get_db()
        cursor = db.cursor()
        cursor.execute(query, update_values)
        db.commit()
        
        # Get the updated record
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

@api_bp.route('/key/<key>', methods=['DELETE'])
@require_api_key
def delete_api_key(key):
    """Delete an API key - restricted to admin users"""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    # Don't allow deletion of the key being used
    current_key = request.headers.get('X-API-Key')
    if key == current_key:
        return jsonify({"error": "Cannot delete the API key currently in use"}), 400
    
    try:
        # Check if key exists
        cursor = get_cursor()
        cursor.execute('SELECT username FROM api_keys WHERE [key] = ?', (key,))
        
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "API key not found"}), 404
        
        username = row[0]
        
        # Delete the key
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

@api_bp.route('/keys', methods=['GET'])
@require_api_key
def list_api_keys():
    """List all API keys - restricted to admin users"""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
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

@api_bp.route('/location/<ip>')
@require_api_key
def ip_location(ip):
    """Get location for an IP address"""
    location = get_location(ip)
    return jsonify(location)

@api_bp.route('/user/<username>')
@require_api_key
def user_ips(username):
    """Get IPs for a username"""
    # Find all records for this username
    records = get_user_records(username)
    
    if not records:
        return jsonify({"error": "Username not found"}), 404
    
    # Get location for each IP
    results = []
    for record in records:
        ip = record['ip']
        location = get_location(ip)
        
        results.append({
            "ip": ip,
            "location": location
        })
    
    return jsonify({
        "username": username,
        "ip_count": len(results),
        "locations": results
    })

@api_bp.route('/authenticate')
@require_api_key
def authenticate():
    """Test endpoint to verify API key is working"""
    return jsonify({
        "authenticated": True,
        "user": request.api_user['user'],
        "rate_limit": request.api_user['rate_limit'],
        "is_admin": request.api_user.get('is_admin', False)
    })

@api_bp.route('/users', methods=['GET'])
@require_api_key
def list_users():
    """List all users in the system (admin only)"""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    cursor = get_cursor()
    cursor.execute('SELECT DISTINCT username FROM user_ip')
    
    users = [row[0] for row in cursor.fetchall()]
    
    return jsonify({
        "count": len(users),
        "users": users
    })

@api_bp.route('/stats', methods=['GET'])
@require_api_key
def get_stats():
    """Get statistics about the database (admin only)"""
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
    
    cursor = get_cursor()
    
    # Get user count
    cursor.execute('SELECT COUNT(DISTINCT username) AS user_count FROM user_ip')
    user_count = cursor.fetchone()[0]
    
    # Get IP count
    cursor.execute('SELECT COUNT(DISTINCT ip) AS ip_count FROM user_ip')
    ip_count = cursor.fetchone()[0]
    
    # Get total records
    cursor.execute('SELECT COUNT(*) AS record_count FROM user_ip')
    record_count = cursor.fetchone()[0]
    
    # Get API key count
    cursor.execute('SELECT COUNT(*) AS key_count FROM api_keys')
    key_count = cursor.fetchone()[0]
    
    return jsonify({
        "users": user_count,
        "unique_ips": ip_count,
        "total_records": record_count,
        "api_keys": key_count
    })