import os

class Config:
    """Base configuration settings for the Flask application."""
    
    API_HOST = os.environ.get('API_HOST', '172.16.27.68')
    PORT = int(os.environ.get('PORT', 5000))
    DEBUG = os.environ.get('FLASK_DEBUG', 'True').lower() in ('true', '1')
    SQLSERVER_HOST = os.environ.get('SQLSERVER_HOST', '10.86.108.37') 
    SQLSERVER_DB = os.environ.get('SQLSERVER_DB', 'FIS_HCM')
    SQLSERVER_USER = os.environ.get('SQLSERVER_USER', 'sa')
    SQLSERVER_PASS = os.environ.get('SQLSERVER_PASS', 'A@a1234567890(*&^%')
    DATA_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')