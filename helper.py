# helper.py
"""
Helper functions shared across multiple modules in the VPN analysis application.
"""
import secrets
import pyodbc
from flask import g, current_app, request
from functools import wraps

# ---- Database Functions ----

def get_db():
    """
    Get database connection, creating one if needed.
    
    Returns:
        pyodbc.Connection: A connection to the SQL Server database
    """
    db = getattr(g, '_database', None)
    if db is None:
        # Connect to SQL Server using Windows Authentication
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={current_app.config['SQLSERVER_HOST']};"
            f"DATABASE={current_app.config['SQLSERVER_DB']};"
            "Trusted_Connection=yes;"
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

# ---- Authentication Functions ----

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

def generate_api_key():
    """
    Generate a random API key.
    
    Returns:
        str: A secure random API key string
    """
    return secrets.token_urlsafe(32)