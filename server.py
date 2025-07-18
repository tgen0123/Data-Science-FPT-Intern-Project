import os
import traceback
import pyodbc
from flask import Flask, jsonify, g
from config import Config
import database

def create_app():
    """
    Application factory to create and configure the Flask app.
    This pattern solves the 'app' not defined error.
    """
    app = Flask(__name__)
    app.config.from_object(Config)

    def init_db():
        """
        Initializes the database. Nested to access 'app.config'.
        """
        try:
            # Step 1: Connect to master DB to create the application DB if it doesn't exist.
            connection_string_master = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={app.config['SQLSERVER_HOST']};"
                # f"UID={app.config['SQLSERVER_USER']};"
                # f"PWD={app.config['SQLSERVER_PASS']};"
                "DATABASE=master;"
                f"Trusted_Connection=yes;"  # Use Windows Authentication
                "TrustServerCertificate=yes;"
            )
            with pyodbc.connect(connection_string_master, autocommit=True) as conn_master:
                cursor_master = conn_master.cursor()
                db_name = app.config['SQLSERVER_DB']
                cursor_master.execute(f"SELECT database_id FROM sys.databases WHERE Name = '{db_name}'")
                if not cursor_master.fetchone():
                    cursor_master.execute(f"CREATE DATABASE {db_name}")
                    print(f"Database {db_name} created.")

            # Step 2 & 3 need an application context to work with flask.g
            with app.app_context():
                # Initialize the schema (create tables).
                database.initialize_schema()

                # Insert default data if necessary.
                conn_app = database.get_db()
                cursor_app = conn_app.cursor()
                cursor_app.execute('SELECT COUNT(*) FROM api_keys')
                if cursor_app.fetchone()[0] == 0:
                    cursor_app.execute('''
                    INSERT INTO api_keys ([key], username, rate_limit, is_admin) VALUES 
                    ('demo_key', 'demo_user', 100, 0),
                    ('admin_key', 'admin_user', 1000, 1)
                    ''')
                    conn_app.commit()
                    print("Inserted default API keys.")

            print("Database initialization process completed successfully.")

        except Exception as e:
            print(f"Error during init_db process: {e}")
            traceback.print_exc()

    # --- Error Handlers ---
    @app.errorhandler(404)
    def handle_not_found_error(e):
        return jsonify(error="The requested resource was not found."), 404

    @app.errorhandler(500)
    def handle_internal_server_error(e):
        traceback.print_exc()
        return jsonify(error="An internal server error occurred."), 500

    @app.errorhandler(400)
    def handle_bad_request_error(e):
        description = getattr(e, 'description', "Bad request.")
        return jsonify(error=description), 400

    # --- Database Connection Management ---
    @app.teardown_appcontext
    def close_db(e=None):
        db = g.pop('_database', None)
        if db is not None:
            db.close()

    # --- Application Startup Logic ---
    # Ensure data directory exists
    if not os.path.exists(app.config['DATA_DIRECTORY']):
        os.makedirs(app.config['DATA_DIRECTORY'])
        
    init_db()  # Call the nested init function

    # Register Blueprints
    from api_keys import api_bp
    from data_routes import data_bp
    from webhook_handler import webhook_bp

    app.register_blueprint(api_bp)
    app.register_blueprint(data_bp)
    app.register_blueprint(webhook_bp)

    # --- Root Endpoints ---
    @app.route('/')
    def index():
        port = app.config.get('PORT', 5000)
        hostname_url = f"http://{app.config['API_HOST']}:{port}"
        return jsonify({
            "name": "VPN Analysis API",
            "description": "API for analyzing VPN usage patterns with enhanced data preprocessing and Splunk webhook integration",
            "status": "online",
            "server_hostname": app.config['API_HOST'],
            "access_url": hostname_url,
            "endpoints": [
                "",
                "API KEY MANAGEMENT ENDPOINTS",
                "/api/authenticate - Test your API key",
                "/api/users - List all users (admin only)",
                "/api/stats - Get database statistics (admin only)",
                "/api/key/generate - Generate new API key (admin only, POST)",
                "/api/key/<key> - Get/Update/Delete API key (admin only)",
                "/api/keys - List all API keys (admin only, GET)",
                "",
                "DATA MANAGEMENT ENDPOINTS",
                "/api/data/load - Load and preprocess CSV file directly into the database (admin only, POST)",
                "/api/data/load-to-existing - Load and preprocess CSV file and save to to an existing table in database (admin only, POST)"
                "/api/data/processed-table-data/name/<file_name> - Get data by filename (admin only, GET)",
                "/api/data/stats - Get statistics about loaded CSV files (admin only, GET)",
                "/api/data/file-details/name/<file_name> - Get file details by name (admin only, GET)",
                "",
                "WEBHOOK ENDPOINTS (FOR SPLUNK INTEGRATION)",
                "/api/webhook/splunk-alert - Receive Splunk alerts (POST, NO API KEY REQUIRED)",
                "/api/webhook/splunk-alerts - Get all stored alerts (admin only, GET)",
                "/api/webhook/splunk-alerts/<alert_id> - Get specific alert details (admin only, GET)",
                "/api/webhook/splunk-alerts/<alert_id>/mark-processed - Mark alert as processed (admin only, POST)",
                "/api/webhook/test - Test webhook functionality (POST/GET, NO API KEY REQUIRED)",
                "/api/webhook/health - Webhook health check (GET, NO API KEY REQUIRED)",
            ]
        })

    @app.route('/webhook-info')
    def webhook_info():
        port = app.config.get('PORT', 5000)
        hostname_url = f"http://{app.config['API_HOST']}:{port}"
        return jsonify({
            "webhook_configuration": {
                "splunk_webhook_url": f"{hostname_url}/api/webhook/splunk-alert",
                "method": "POST",
                "content_type": "application/json",
                "authentication": "None required for webhook endpoint",
                "description": "This endpoint receives alerts from Splunk and stores them in the database"
            }
        })
    
    return app

# --- Main Execution Block ---
if __name__ == '__main__':
    app = create_app()
    host = '0.0.0.0'
    port = app.config['PORT']
    debug = app.config['DEBUG']

    print("=" * 80)
    print("STARTING VPN ANALYSIS API SERVER WITH WEBHOOK SUPPORT")
    print("=" * 80)
    
    app.run(debug=debug, host=host, port=port)