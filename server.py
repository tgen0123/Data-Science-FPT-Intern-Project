from flask import Flask, jsonify
from api import api_bp
from helper import load_data, init_db, close_db

# Create the Flask application
app = Flask(__name__)

# SQL Server Configuration - Update the server name to match your SQL Server instance
app.config['SQLSERVER_HOST'] = 'IT-ONDANGKN\\SQLEXPRESS'  # Your SQL Server instance
app.config['SQLSERVER_DB'] = 'ip_location_api'                # Database name

# Register the API blueprint
app.register_blueprint(api_bp)

# Register database close function to ensure connections are closed after each request
app.teardown_appcontext(close_db)

# Add a root route for basic API info
@app.route('/')
def index():
    """
    Root endpoint that provides API information and available endpoints
    
    Returns:
        JSON with API name, version, description, and list of endpoints
    """
    return jsonify({
        "name": "IP Geolocation API",
        "version": "2.1",
        "description": "API for tracking user IPs and getting their geolocation",
        "endpoints": [
            "/api/authenticate - Test your API key",
            "/api/location/<ip> - Get location for an IP",
            "/api/user/<username> - Get IPs for a username",
            "/api/users - List all users (admin only)",
            "/api/stats - Get database statistics (admin only)",
            "/api/key/generate - Generate new API key (admin only, POST)",
            "/api/key/<key> - Get key details (GET), Update key (PUT), Delete key (DELETE) (admin only)",
            "/api/keys - List all API keys (admin only, GET)"
        ]
    })

# Configuration
DATA_FILE = 'sample_data.csv'  # File containing initial data to load
HOST = '0.0.0.0'  # Listen on all interfaces
PORT = 5000       # Port to serve the API
DEBUG = True      # Enable debug mode (set to False in production)

# Initialize the application
def init_app():
    """
    Initialize the application
    
    Sets up the database and loads initial data if needed
    
    Returns:
        Configured Flask application instance
    """
    # Initialize the database (create tables if they don't exist)
    with app.app_context():
        init_db()
        
        # Check if we need to load initial data
        try:
            from helper import get_cursor
            cursor = get_cursor()
            cursor.execute('SELECT COUNT(*) FROM user_ip')
            count = cursor.fetchone()[0]
            
            # If the database is empty, load sample data
            if count == 0:
                print("Database is empty, loading initial data...")
                records = load_data(DATA_FILE)
                print(f"Loaded {records} initial records from CSV")
        except Exception as e:
            print(f"Error checking database: {e}")
    
    return app

# Run the application
if __name__ == '__main__':
    # Initialize the app
    app = init_app()
    
    # Start the server
    print(f"Starting server on {HOST}:{PORT}")
    print(f"Debug mode: {'ON' if DEBUG else 'OFF'}")
    print(f"Database: SQL Server ({app.config['SQLSERVER_HOST']} / {app.config['SQLSERVER_DB']})")
    app.run(debug=DEBUG, host=HOST, port=PORT)