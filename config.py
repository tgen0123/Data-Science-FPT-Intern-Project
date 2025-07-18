import os

class Config:
    """Base configuration settings for the Flask application."""
    
    API_HOST = os.environ.get('API_HOST', '127.0.0.1')
    PORT = int(os.environ.get('PORT', 5000))
    DEBUG = os.environ.get('FLASK_DEBUG', 'True').lower() in ('true', '1')

    SQLSERVER_HOST = os.environ.get('SQLSERVER_HOST', 'IT-ONDANGKN\\SQLEXPRESS') # Use double backslash
    SQLSERVER_DB = os.environ.get('SQLSERVER_DB', 'FIS_HCM_DATA')
    # SQLSERVER_USER = os.environ.get('SQLSERVER_USER', 'sa')
    # SQLSERVER_PASS = os.environ.get('SQLSERVER_PASS', '123qwe!!@@##4%%')

    DATA_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')