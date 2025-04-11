import re
import secrets
import requests
import pandas as pd
import pyodbc
from functools import wraps
from flask import request, jsonify, g, current_app

# Cache for IP geolocation data - improves performance by avoiding redundant API calls
ip_cache = {}

# ---- Database Functions ----

def get_db() -> pyodbc.Connection:
    """
    Get database connection, creating one if needed.
    
    This function retrieves an existing database connection from Flask's g object
    or creates a new one if none exists. The connection is stored for the lifetime
    of the request to avoid creating multiple connections unnecessarily.
    
    Returns:
        pyodbc.Connection: A connection to the SQL Server database using Windows Authentication
    
    Raises:
        pyodbc.Error: If the connection to the database fails
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

def get_cursor() -> pyodbc.Cursor:
    """
    Get a cursor from the database connection.
    
    Provides a convenient way to get a cursor for executing SQL queries
    without having to explicitly call get_db() first.
    
    Returns:
        pyodbc.Cursor: A cursor object for executing SQL queries
    
    Raises:
        pyodbc.Error: If getting the database connection or cursor fails
    """
    return get_db().cursor()

def dict_from_row(row: pyodbc.Row, columns: list[str]) -> dict:
    """
    Convert a pyodbc row to a dictionary.
    
    Creates a dictionary that maps column names to their corresponding values
    in the database row, making it easier to work with query results.
    
    Args:
        row (pyodbc.Row): A pyodbc row result from a query
        columns (list[str]): List of column names from cursor.description
        
    Returns:
        dict: Dictionary mapping column names to row values
    
    Example:
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        columns = [column[0] for column in cursor.description]
        user_dict = dict_from_row(cursor.fetchone(), columns)
    """
    return {columns[i]: row[i] for i in range(len(columns))}

def init_db() -> None:
    """
    Initialize the database with required tables and default data.
    
    This function performs the following setup operations:
    1. Checks if the configured database exists, creating it if not
    2. Creates the user_ip table if it doesn't exist
    3. Creates the api_keys table if it doesn't exist
    4. Adds default API keys if none exist
    
    The function is idempotent - it can be safely called multiple times
    without creating duplicate tables or data.
    
    Returns:
        None
        
    Raises:
        Exception: If any database operation fails, with the error message printed to console
    """
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

def close_db(e: Exception = None) -> None:
    """
    Close database connection if it exists.
    
    Designed to be registered as Flask's teardown_appcontext function
    to ensure database connections are closed after each request.
    
    Args:
        e (Exception, optional): Exception that caused the context to be torn down.
            Flask passes this automatically. Defaults to None.
    
    Returns:
        None
    """
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()





# ---- Authentication Functions ----

def require_api_key(f: callable) -> callable:
    """
    Decorator to require API key authentication.
    
    This decorator performs the following:
    1. Checks if a valid API key is provided in the X-API-Key header
    2. Verifies the key exists in the database
    3. Attaches API user information to the request object
    4. Returns 401 error if the key is missing or invalid
    
    The decorated function can access the authenticated user via request.api_user
    
    Args:
        f (callable): The function to wrap
        
    Returns:
        callable: Decorated function that requires API key authentication
        
    Example:
        @app.route('/protected')
        @require_api_key
        def protected_route():
            # Access authenticated user
            user = request.api_user
            return f"Hello {user['user']}!"
    """
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

def generate_api_key() -> str:
    """
    Generate a random API key.
    
    Creates a cryptographically strong, URL-safe token using the secrets module.
    The token has 32 bytes of randomness (approximately 43 characters when encoded).
    
    Returns:
        str: A secure random API key string
    """
    return secrets.token_urlsafe(32)

def create_new_api_key(username: str, rate_limit: int = 100, is_admin: bool = False) -> str:
    """
    Create a new API key for a user and store in database.
    
    Generates a unique API key and associates it with the provided username
    in the database with the specified rate limit and admin status.
    
    Args:
        username (str): Username to associate with the key
        rate_limit (int, optional): Maximum requests per timeframe. Defaults to 100.
        is_admin (bool, optional): Whether the key has admin privileges. Defaults to False.
        
    Returns:
        str: The newly generated API key
        
    Raises:
        pyodbc.Error: If database operations fail
    """
    new_key = generate_api_key()
    
    cursor = get_cursor()
    cursor.execute(
        'INSERT INTO api_keys ([key], username, rate_limit, is_admin) VALUES (?, ?, ?, ?)',
        (new_key, username, rate_limit, 1 if is_admin else 0)
    )
    db = get_db()
    db.commit()
    
    return new_key

def is_admin(api_user: dict) -> bool:
    """
    Check if the API user has admin privileges.
    
    Args:
        api_user (dict): API user information dictionary containing user details
            from the database, or None if no user is authenticated
        
    Returns:
        bool: True if the user has admin privileges, False otherwise
        
    Note:
        Returns False if api_user is None or doesn't have an is_admin attribute
    """
    return api_user and api_user.get('is_admin', False)






# ---- Data Processing Functions ----

def extract_data_from_xml(xml_string: str) -> tuple[str | None, str | None]:
    """
    Extract username and calling IP from XML data.
    
    Parses XML data with regex to extract SubjectUserName and CallingStationID
    fields from event log XML data. This function is designed to work with
    a specific XML format from Windows event logs.
    
    Args:
        xml_string (str): XML data containing user login information
        
    Returns:
        tuple[str | None, str | None]: A tuple containing:
            - username (str or None): The extracted username or None if not found
            - ip (str or None): The extracted IP address or None if not found
            
    Note:
        Returns (None, None) if:
        - Input is not a string
        - Required XML tags aren't found
        - Values are placeholder dashes ('-')
    """
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

def get_location(ip_address: str) -> dict:
    """
    Get geolocation for an IP using ipinfo.io API.
    
    Retrieves geographic information for the provided IP address using the
    ipinfo.io service. Results are cached to avoid redundant API calls for
    the same IP address during the application's lifetime.
    
    Args:
        ip_address (str): Valid IPv4 or IPv6 address to geolocate
        
    Returns:
        dict: Dictionary with geolocation data containing some or all of:
            - ip: The IP address
            - hostname: Hostname if available
            - city: City name
            - region: Region/state
            - country: Country code
            - loc: Latitude,longitude
            - org: Organization/ISP
            - postal: Postal code
            - timezone: Timezone
            
            Or in case of an error:
            - error: Error message
            
    Note:
        This function depends on the external ipinfo.io service.
        Free tier usage is limited to 50,000 requests per month.
    """
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

def load_data(file_path: str) -> int:
    """
    Load and process data from CSV file into database.
    
    Reads a CSV file containing event log data, extracts username and IP
    information from XML data in the 'EventData_Xml' column, and stores
    unique username-IP pairs in the database.
    
    Args:
        file_path (str): Path to the CSV file containing the event log data
        
    Returns:
        int: Number of new records successfully loaded into the database
        
    Raises:
        FileNotFoundError: If the specified file doesn't exist
        pandas.errors.EmptyDataError: If the CSV file is empty
        pandas.errors.ParserError: If the CSV file is malformed
        pyodbc.Error: If database operations fail
        
    Note:
        - The function skips rows where the XML data is missing or invalid
        - Only unique username-IP pairs are inserted (duplicates are ignored)
        - The CSV file must have a column named 'EventData_Xml'
    """
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

def get_user_records(username: str) -> list[dict]:
    """
    Get all records for a specific username from database.
    
    Retrieves all IP addresses associated with the given username
    from the user_ip table in the database.
    
    Args:
        username (str): Username to query
        
    Returns:
        list[dict]: List of dictionaries, each containing:
            - username (str): The username (same as input)
            - ip (str): An IP address associated with this username
            
        Returns an empty list if no records are found.
        
    Raises:
        pyodbc.Error: If database query fails
    """
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