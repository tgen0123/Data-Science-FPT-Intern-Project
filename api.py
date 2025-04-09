from flask import Blueprint, jsonify, request, g
from helper import get_location, get_user_records, require_api_key, is_admin, create_new_api_key, get_cursor, dict_from_row

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