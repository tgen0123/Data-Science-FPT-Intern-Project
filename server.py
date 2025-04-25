# server.py
from flask import Flask, jsonify
import pyodbc
import os

# Create the Flask application
app = Flask(__name__)

# SQL Server Configuration
app.config['SQLSERVER_HOST'] = 'IT-ONDANGKN\\SQLEXPRESS' 
app.config['SQLSERVER_DB'] = 'ip_location_api'             

# Global variable for database connection
def get_db_connection():
    """Get a connection to the database"""
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={app.config['SQLSERVER_HOST']};"
        f"DATABASE={app.config['SQLSERVER_DB']};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)

def close_db(e=None):
    """Close the database connection at the end of a request"""
    # Implementation to close DB connection if needed
    pass

# Register database close function
app.teardown_appcontext(close_db)

# Root route for API info
@app.route('/')
def index():
    """Root endpoint that provides API information"""
    return jsonify({
        "name": "VPN Analysis API",
        "version": "2.0",
        "description": "API for analyzing VPN usage patterns with enhanced data preprocessing",
        "endpoints": [
            "",
            "API key management endpoints",
            "/api/authenticate - Test your API key",
            "/api/users - List all users (admin only)",
            "/api/stats - Get database statistics (admin only)",
            "/api/key/generate - Generate new API key (admin only, POST)",
            "/api/key/<key> - Get/Update/Delete API key (admin only)",
            "/api/keys - List all API keys (admin only, GET)",
            "",
            
            "Data management endpoints",
            "/api/data/load - Load CSV file into database with all columns preserved (admin only, POST)",
            "/api/data/preprocess-db - Process data using CSV file, store in processed_data_X table (admin only, POST)",
            "/api/data/processed-tables - Get list of all processed data tables (admin only, GET)",
            "/api/data/processed-table-data/<file_id> - Get details from a processed table (admin only, GET)",
            "/api/data/stats - Get statistics about loaded CSV files (admin only, GET)",
            "/api/data/raw-data/<file_id> - Get details about a specific CSV file (admin only, GET)",
            "",
            
            "VPN endpoints with enhanced capabilities",
            "/api/vpn/stats - Get VPN usage statistics, including time categories",
            "/api/vpn/users/<username> - Get VPN usage details for a user",
            "/api/vpn/anomalies - Detect potential anomalies in VPN usage patterns",
            ""
        ]
    })

def init_db():
    """Initialize the database with required tables"""
    try:
        # Create connection to SQL Server
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={app.config['SQLSERVER_HOST']};"
            "Trusted_Connection=yes;"
        )
        
        # First connect to master to check if our database exists
        conn = pyodbc.connect(connection_string + "DATABASE=master;")
        cursor = conn.cursor()
        
        # Check if database exists, create if not
        cursor.execute(f"SELECT database_id FROM sys.databases WHERE Name = '{app.config['SQLSERVER_DB']}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {app.config['SQLSERVER_DB']}")
            print(f"Database {app.config['SQLSERVER_DB']} created")
        
        conn.close()
        
        # Now connect to our database
        conn = pyodbc.connect(connection_string + f"DATABASE={app.config['SQLSERVER_DB']};")
        cursor = conn.cursor()
        
        # Create api_keys table
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[api_keys]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[api_keys] (
                [key] NVARCHAR(255) PRIMARY KEY,
                [username] NVARCHAR(255) NOT NULL,
                [rate_limit] INT DEFAULT 100,
                [is_admin] BIT DEFAULT 0
            )
        END
        ''')
        
        # Create csv_registry table if it doesn't exist
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[csv_registry]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[csv_registry] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                [file_name] NVARCHAR(255) NOT NULL,
                [file_path] NVARCHAR(1000) NOT NULL,
                [row_count] INT NOT NULL,
                [column_count] INT NOT NULL,
                [loaded_at] DATETIME DEFAULT GETDATE(),
                [is_processed] BIT DEFAULT 0
            )
            
            CREATE INDEX [idx_csv_registry_is_processed] ON [dbo].[csv_registry] ([is_processed])
        END
        ''')
        
        # Check if default API keys exist
        cursor.execute('SELECT COUNT(*) FROM api_keys')
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Add default API keys - one regular user and one admin
            cursor.execute('''
            INSERT INTO api_keys ([key], username, rate_limit, is_admin) VALUES 
            ('demo_key', 'demo_user', 100, 0),
            ('admin_key', 'admin_user', 1000, 1)
            ''')
        
        conn.commit()
        conn.close()
        
        print("Database initialized successfully")
    except Exception as e:
        print(f"Error initializing database: {e}")

# Initialize the application
def init_app():
    """Initialize the application"""
    # Initialize the database
    with app.app_context():
        init_db()
    
    # Register blueprints
    from api_keys import api_bp
    from vpn_analysis import vpn_bp
    from data_loading import data_bp
    
    app.register_blueprint(api_bp)
    app.register_blueprint(vpn_bp)
    app.register_blueprint(data_bp)
    
    return app

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

HOST = '0.0.0.0'
PORT = 5000
DEBUG = True

# Run the application
if __name__ == '__main__':
    # Initialize the app
    app = init_app()
    
    # Start the server
    print(f"Starting server on {HOST}:{PORT}")
    print(f"Debug mode: {'ON' if DEBUG else 'OFF'}")
    print(f"Database: SQL Server ({app.config['SQLSERVER_HOST']} / {app.config['SQLSERVER_DB']})")
    app.run(debug=DEBUG, host=HOST, port=PORT)