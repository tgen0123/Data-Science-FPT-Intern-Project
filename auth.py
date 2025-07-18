import secrets
from flask import request, jsonify
from functools import wraps
from database import get_cursor, dict_from_row

def require_api_key(f):
    """
    Decorator to require API key authentication.
    
    Args:
        f (callable): The function to wrap
        
    Returns:
        callable: Decorated function that requires API key authentication
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        
        if not api_key:
            return {"error": "API key required"}, 401
            
        # Check if API key exists in database
        cursor = get_cursor()
        cursor.execute('SELECT * FROM api_keys WHERE [key] = ?', (api_key,))
        
        row = cursor.fetchone()
        if not row:
            return {"error": "Invalid API key"}, 401
        
        # Get column names to create a dictionary
        columns = [column[0] for column in cursor.description]
        api_user = dict_from_row(row, columns)
        
        # Add the API user info to the request
        request.api_user = {
            'user': api_user['username'],
            'rate_limit': api_user['rate_limit'],
            'is_admin': bool(api_user['is_admin'])
        }
        
        return f(*args, **kwargs)
    return decorated_function

def is_admin(api_user):
    """
    Check if the API user has admin privileges.
    
    Args:
        api_user (dict): API user information dictionary
        
    Returns:
        bool: True if the user has admin privileges, False otherwise
    """
    return api_user and api_user.get('is_admin', False)

def require_admin(f):
    """Decorator that requires an API key with admin privileges."""
    @wraps(f)
    @require_api_key  
    def decorated_function(*args, **kwargs):
        if not is_admin(request.api_user):
            return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
        return f(*args, **kwargs)
    return decorated_function

def generate_api_key():
    """
    Generate a random API key.
    
    Returns:
        str: A secure random API key string
    """
    return secrets.token_urlsafe(32)