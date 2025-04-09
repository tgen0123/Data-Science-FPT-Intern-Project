import re
import secrets
import requests
import pandas as pd
import pyodbc
from functools import wraps
from flask import request, jsonify, g, current_app

# Cache for IP geolocation data
ip_cache = {}

# ---- Database Functions ----

def get_db():
    """Get database connection, creating one if needed"""
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
    """Get a cursor from the database connection"""
    return get_db().cursor()

def dict_from_row(row, columns):
    """Convert a pyodbc row to a dictionary"""
    return {columns[i]: row[i] for i in range(len(columns))}

def init_db():
    """Initialize the database with required tables"""
    try:
        # Create connection to SQL Server
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={current_app.config['SQLSERVER_HOST']};"
            "Trusted_Connection=yes;"
        )
        
        # First connect to master to check if our database exists
        conn = pyodbc.connect(connection_string + "DATABASE=master;")
        cursor = conn.cursor()
        
        # Check if database exists, create if not
        cursor.execute(f"SELECT database_id FROM sys.databases WHERE Name = '{current_app.config['SQLSERVER_DB']}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {current_app.config['SQLSERVER_DB']}")
            print(f"Database {current_app.config['SQLSERVER_DB']} created")
        
        conn.close()
        
        # Now connect to our database
        conn = pyodbc.connect(connection_string + f"DATABASE={current_app.config['SQLSERVER_DB']};")
        cursor = conn.cursor()
        
        # Create user_ip table
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[user_ip]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[user_ip] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                [username] NVARCHAR(255) NOT NULL,
                [ip] NVARCHAR(45) NOT NULL
            )
            
            CREATE UNIQUE INDEX [idx_username_ip] ON [dbo].[user_ip] ([username], [ip])
        END
        ''')
        
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
        
        # Check if default API keys exist
        cursor.execute('SELECT COUNT(*) FROM api_keys')
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Add default API keys
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

def close_db(e=None):
    """Close database connection if it exists"""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

# ---- Authentication Functions ----

def require_api_key(f):
    """Decorator to require API key authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        
        if not api_key:
            return jsonify({"error": "API key required"}), 401
            
        # Check if API key exists in database
        cursor = get_cursor()
        cursor.execute('SELECT * FROM api_keys WHERE [key] = ?', (api_key,))
        
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "Invalid API key"}), 401
        
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

def generate_api_key():
    """Generate a random API key"""
    return secrets.token_urlsafe(32)

def create_new_api_key(username, rate_limit=100, is_admin=False):
    """Create a new API key for a user and store in database"""
    new_key = generate_api_key()
    
    cursor = get_cursor()
    cursor.execute(
        'INSERT INTO api_keys ([key], username, rate_limit, is_admin) VALUES (?, ?, ?, ?)',
        (new_key, username, rate_limit, 1 if is_admin else 0)
    )
    db = get_db()
    db.commit()
    
    return new_key

def is_admin(api_user):
    """Check if the API user has admin privileges"""
    return api_user and api_user.get('is_admin', False)

# ---- Data Processing Functions ----

def extract_data_from_xml(xml_string):
    """Extract username and calling IP from XML data"""
    if not isinstance(xml_string, str):
        return None, None
    
    # Extract username
    username_match = re.search(r"<Data Name='SubjectUserName'>([^<]+)</Data>", xml_string)
    username = username_match.group(1) if username_match else None
    
    # Extract IP address
    ip_match = re.search(r"<Data Name='CallingStationID'>([^<]+)</Data>", xml_string)
    ip = ip_match.group(1) if ip_match else None
    
    if ip == '-' or username == '-':
        return None, None
        
    return username, ip

def get_location(ip_address):
    """Get geolocation for an IP using ipinfo.io API"""
    # Check cache first
    if ip_address in ip_cache:
        return ip_cache[ip_address]
    
    try:
        response = requests.get(f"https://ipinfo.io/{ip_address}/json")
        if response.status_code == 200:
            data = response.json()
            # Save in cache
            ip_cache[ip_address] = data
            return data
        else:
            return {"error": f"API error: {response.status_code}"}
    except Exception as e:
        return {"error": str(e)}

def load_data(file_path):
    """Load and process data from CSV file into database"""
    try:
        df = pd.read_csv(file_path)
        
        # Get database connection
        db = get_db()
        cursor = db.cursor()
        
        # Counter for successfully loaded records
        record_count = 0
        
        for i, row in df.iterrows():
            if pd.isna(row.get('EventData_Xml')):
                continue
                
            username, ip = extract_data_from_xml(row['EventData_Xml'])
            if username and ip:
                try:
                    # Check if this username/IP pair already exists
                    cursor.execute('SELECT COUNT(*) FROM user_ip WHERE username = ? AND ip = ?', (username, ip))
                    if cursor.fetchone()[0] == 0:
                        cursor.execute(
                            'INSERT INTO user_ip (username, ip) VALUES (?, ?)',
                            (username, ip)
                        )
                        record_count += 1
                except Exception as e:
                    print(f"Error inserting record: {e}")
        
        # Commit changes
        db.commit()
        
        print(f"Loaded {record_count} new records into database")
        return record_count
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return 0

def get_user_records(username):
    """Get all records for a specific username from database"""
    cursor = get_cursor()
    cursor.execute('SELECT * FROM user_ip WHERE username = ?', (username,))
    
    columns = [column[0] for column in cursor.description]
    records = []
    
    for row in cursor.fetchall():
        record = dict_from_row(row, columns)
        records.append({
            'username': record['username'],
            'ip': record['ip']
        })
    
    return records