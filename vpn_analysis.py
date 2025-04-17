# vpn_analysis.py
import pandas as pd
import requests
from flask import Blueprint, jsonify, request, g, current_app
from functools import wraps
import pyodbc

# Import preprocessing module
from data_preprocessor import preprocess_data

# Create a Blueprint for VPN analysis routes
vpn_bp = Blueprint('vpn', __name__, url_prefix='/api/vpn')

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

def is_admin(api_user):
    """
    Check if the API user has admin privileges.
    
    Args:
        api_user (dict): API user information dictionary
        
    Returns:
        bool: True if the user has admin privileges, False otherwise
    """
    return api_user and api_user.get('is_admin', False)

# ---- External IP Location API ----

def get_ip_location(ip):
    """
    Get location information for an IP using external ipinfo.io API.
    
    Args:
        ip (str): IP address to look up
        
    Returns:
        dict: Location information or error message
    """
    try:
        response = requests.get(f"https://ipinfo.io/{ip}/json")
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"API error: {response.status_code}"}
    except Exception as e:
        return {"error": str(e)}

# ---- VPN Data Processing Functions ----

def extract_vpn_features(df):
    """
    Extract VPN-relevant features from a dataframe.
    This assumes the dataframe has already been preprocessed.
    
    Args:
        df (DataFrame): Preprocessed dataframe
    
    Returns:
        DataFrame: DataFrame with extracted VPN features
    """
    try:
        # Create a new dataframe for VPN features
        vpn_df = pd.DataFrame()
        
        # Extract timestamp - using _time which appears in your sample data
        if '_time' in df.columns:
            vpn_df['timestamp'] = pd.to_datetime(df['_time'])
        
        # Extract username - use SubjectUserName directly
        if 'SubjectUserName' in df.columns:
            vpn_df['username'] = df['SubjectUserName']
        
        # Extract source IP - use CallingStationID directly
        if 'CallingStationID' in df.columns:
            vpn_df['source_ip'] = df['CallingStationID']
        
        # Extract department from organizational path
        if 'FullyQualifiedSubjectUserName' in df.columns:
            # Based on the sample data, this pattern should work better
            vpn_df['department'] = df['FullyQualifiedSubjectUserName'].str.extract(
                r'fsoft\.fpt\.vn/FPT/FIS/([^/]+)'
            )
        
        # Extract VPN gateway
        if 'NASIdentifier' in df.columns:
            vpn_df['vpn_gateway'] = df['NASIdentifier']
        
        # Extract session ID
        if 'AccountSessionIdentifier' in df.columns:
            vpn_df['session_id'] = df['AccountSessionIdentifier']
        
        # Ensure we have the essential columns
        if 'username' not in vpn_df.columns or 'source_ip' not in vpn_df.columns:
            missing = []
            if 'username' not in vpn_df.columns:
                missing.append('username')
            if 'source_ip' not in vpn_df.columns:
                missing.append('source_ip')
            raise ValueError(f"Essential columns missing: {missing}")
        
        print(f"Extracted VPN features: {vpn_df.shape[0]} rows, {vpn_df.shape[1]} columns")
        return vpn_df
    
    except Exception as e:
        print(f"Error extracting VPN features: {e}")
        return None

def load_vpn_data_to_db(df):
    """
    Load VPN data into the database.
    
    Args:
        df (DataFrame): DataFrame with VPN features
        
    Returns:
        int: Number of records loaded
    """
    count = 0
    skipped = 0
    db = get_db()
    cursor = db.cursor()
    
    try:
        # Create vpn_logs table if it doesn't exist - modified data types
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vpn_logs]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [dbo].[vpn_logs] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                [timestamp] DATETIME,
                [username] NVARCHAR(255) NOT NULL,
                [source_ip] NVARCHAR(45) NOT NULL,
                [department] NVARCHAR(255),
                [vpn_gateway] NVARCHAR(100),
                [session_id] NVARCHAR(100)
            )
            
            CREATE INDEX [idx_vpn_username] ON [dbo].[vpn_logs] ([username])
            CREATE INDEX [idx_vpn_source_ip] ON [dbo].[vpn_logs] ([source_ip])
        END
        ''')
        db.commit()
        
        # Debug info
        print(f"Starting to load {len(df)} records into the database")
        
        # Insert data
        for i, row in df.iterrows():
            try:
                # Extract values with safe fallbacks
                timestamp = row.get('timestamp', None)
                username = str(row.get('username', ''))
                source_ip = str(row.get('source_ip', ''))
                department = str(row.get('department', '')) if not pd.isna(row.get('department')) else None
                vpn_gateway = str(row.get('vpn_gateway', '')) if not pd.isna(row.get('vpn_gateway')) else None
                session_id = str(row.get('session_id', '')) if not pd.isna(row.get('session_id')) else None
                
                # Skip rows with missing essential data
                if not username or not source_ip:
                    skipped += 1
                    continue
                
                # Check for duplicate records
                if timestamp is not None:
                    cursor.execute('''
                    SELECT COUNT(*) FROM vpn_logs 
                    WHERE username = ? AND source_ip = ? AND timestamp = ?
                    ''', (username, source_ip, timestamp))
                else:
                    cursor.execute('''
                    SELECT COUNT(*) FROM vpn_logs 
                    WHERE username = ? AND source_ip = ? AND timestamp IS NULL
                    ''', (username, source_ip))
                
                if cursor.fetchone()[0] > 0:
                    skipped += 1
                    continue
                
                # Insert the record - explicitly convert all values to appropriate types
                cursor.execute('''
                INSERT INTO vpn_logs 
                (timestamp, username, source_ip, department, vpn_gateway, session_id)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp,
                    username, 
                    source_ip, 
                    department, 
                    vpn_gateway, 
                    session_id
                ))
                count += 1
                
            except Exception as e:
                print(f"Error inserting row {i}: {e}")
                skipped += 1
                
        db.commit()
        print(f"Loaded {count} records, skipped {skipped} records")
        return count
        
    except Exception as e:
        print(f"Error in load_vpn_data_to_db: {e}")
        return 0

# ---- API Endpoints ----

@vpn_bp.route('/load', methods=['POST'])
@require_api_key
def load_vpn_data_endpoint():
    """
    Load VPN data from a CSV file into the database.
    
    This endpoint:
    1. Calls the data preprocessor to clean the data
    2. Extracts VPN features
    3. Loads data into the database
    
    Requires an admin API key.
    """
    if not is_admin(request.api_user):
        return jsonify({"error": "Unauthorized - Admin privileges required"}), 403
        
    data = request.get_json()
    if not data or 'file_path' not in data:
        return jsonify({"error": "file_path required"}), 400
        
    try:
        # Step 1: Preprocess data using the separate module
        preprocessed_df = preprocess_data(data['file_path'])
        if preprocessed_df is None:
            return jsonify({"error": "Failed to preprocess file"}), 500
            
        # Step 2: Extract VPN features
        vpn_df = extract_vpn_features(preprocessed_df)
        if vpn_df is None:
            return jsonify({"error": "Failed to extract VPN features"}), 500
            
        # Step 3: Load data into the vpn_logs table
        vpn_record_count = load_vpn_data_to_db(vpn_df)
        
        return jsonify({
            "success": True,
            "vpn_records_loaded": vpn_record_count,
            "message": f"Successfully loaded {vpn_record_count} VPN records"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@vpn_bp.route('/stats', methods=['GET'])
@require_api_key
def vpn_stats():
    """Get VPN usage statistics."""
    try:
        cursor = get_cursor()
        
        # Get total VPN connection count
        cursor.execute('SELECT COUNT(*) FROM vpn_logs')
        total_connections = cursor.fetchone()[0]
        
        # Get unique user count
        cursor.execute('SELECT COUNT(DISTINCT username) FROM vpn_logs')
        unique_users = cursor.fetchone()[0]
        
        # Get unique source IPs
        cursor.execute('SELECT COUNT(DISTINCT source_ip) FROM vpn_logs')
        unique_ips = cursor.fetchone()[0]
        
        # Get connections by department - handle NULL more explicitly
        cursor.execute('''
        SELECT 
            CASE WHEN department IS NULL OR RTRIM(LTRIM(department)) = '' 
                 THEN 'Unknown' 
                 ELSE department 
            END as dept, 
            COUNT(*) as count 
        FROM vpn_logs 
        GROUP BY 
            CASE WHEN department IS NULL OR RTRIM(LTRIM(department)) = '' 
                 THEN 'Unknown' 
                 ELSE department 
            END
        ORDER BY count DESC
        ''')
        
        departments = {}
        for row in cursor.fetchall():
            departments[row[0]] = row[1]
        
        # Get connections by VPN gateway
        cursor.execute('''
        SELECT 
            CASE WHEN vpn_gateway IS NULL OR RTRIM(LTRIM(vpn_gateway)) = '' 
                 THEN 'Unknown' 
                 ELSE vpn_gateway 
            END as gateway, 
            COUNT(*) as count 
        FROM vpn_logs 
        GROUP BY 
            CASE WHEN vpn_gateway IS NULL OR RTRIM(LTRIM(vpn_gateway)) = '' 
                 THEN 'Unknown' 
                 ELSE vpn_gateway 
            END
        ORDER BY count DESC
        ''')
        
        gateways = {}
        for row in cursor.fetchall():
            gateways[row[0]] = row[1]
        
        # Get connections by hour - safely handle NULL timestamps
        # Fixed query to not use 'hour' as a column name
        cursor.execute('''
        SELECT 
            CASE WHEN timestamp IS NULL 
                 THEN 'Unknown' 
                 ELSE CAST(DATEPART(hour, timestamp) AS VARCHAR) 
            END as hour_of_day, 
            COUNT(*) as count 
        FROM vpn_logs 
        GROUP BY 
            CASE WHEN timestamp IS NULL 
                 THEN 'Unknown' 
                 ELSE CAST(DATEPART(hour, timestamp) AS VARCHAR) 
            END
        ORDER BY 
            CASE WHEN hour_of_day = 'Unknown' THEN 1 ELSE 0 END,
            hour_of_day
        ''')
        
        hourly = {}
        for row in cursor.fetchall():
            hourly[row[0]] = row[1]
        
        return jsonify({
            "total_connections": total_connections,
            "unique_users": unique_users,
            "unique_source_ips": unique_ips,
            "connections_by_department": departments,
            "connections_by_gateway": gateways,
            "connections_by_hour": hourly
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@vpn_bp.route('/users/<username>', methods=['GET'])
@require_api_key
def vpn_user_details(username):
    """Get VPN usage details for a specific user."""
    try:
        cursor = get_cursor()
        
        # Get user's connection count
        cursor.execute('SELECT COUNT(*) FROM vpn_logs WHERE username = ?', (username,))
        connection_count = cursor.fetchone()[0]
        
        if connection_count == 0:
            return jsonify({"error": "User not found in VPN logs"}), 404
        
        # Get user's distinct IPs
        cursor.execute('''
        SELECT source_ip, COUNT(*) as count 
        FROM vpn_logs 
        WHERE username = ? 
        GROUP BY source_ip 
        ORDER BY count DESC
        ''', (username,))
        
        ips = []
        for row in cursor.fetchall():
            ips.append({
                "ip": row[0],
                "connection_count": row[1]
            })
        
        # Get user's connection history
        cursor.execute('''
        SELECT timestamp, source_ip, vpn_gateway, department, session_id
        FROM vpn_logs 
        WHERE username = ? 
        ORDER BY timestamp DESC
        ''', (username,))
        
        history = []
        for row in cursor.fetchall():
            history.append({
                "timestamp": row[0].isoformat() if row[0] else None,
                "source_ip": row[1],
                "vpn_gateway": row[2],
                "department": row[3],
                "session_id": row[4]
            })
        
        # Get location info for each IP
        for ip_entry in ips:
            ip = ip_entry["ip"]
            ip_entry["location"] = get_ip_location(ip)
        
        return jsonify({
            "username": username,
            "connection_count": connection_count,
            "source_ips": ips,
            "connection_history": history
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@vpn_bp.route('/location/<ip>', methods=['GET'])
@require_api_key
def ip_location(ip):
    """
    Get location information for an IP address.
    
    Args:
        ip (str): IP address to look up
        
    Returns:
        JSON with location information
    """
    location = get_ip_location(ip)
    return jsonify(location)

@vpn_bp.route('/anomalies', methods=['GET'])
@require_api_key
def vpn_anomalies():
    """Detect potential anomalies in VPN usage."""
    try:
        cursor = get_cursor()
        
        # Find users connecting from multiple IPs within a short time window
        cursor.execute('''
        WITH UserIpTimeDiff AS (
            SELECT 
                username,
                timestamp,
                source_ip,
                LAG(source_ip) OVER (PARTITION BY username ORDER BY timestamp) AS prev_ip,
                LAG(timestamp) OVER (PARTITION BY username ORDER BY timestamp) AS prev_timestamp,
                DATEDIFF(minute, LAG(timestamp) OVER (PARTITION BY username ORDER BY timestamp), timestamp) AS time_diff_minutes
            FROM vpn_logs
            WHERE timestamp IS NOT NULL
        )
        SELECT 
            username, 
            timestamp, 
            source_ip, 
            prev_ip, 
            prev_timestamp,
            time_diff_minutes
        FROM UserIpTimeDiff
        WHERE 
            prev_ip IS NOT NULL AND 
            source_ip <> prev_ip AND 
            time_diff_minutes < 60  -- Less than 60 minutes between different IPs
        ORDER BY username, timestamp
        ''')
        
        rapid_ip_changes = []
        for row in cursor.fetchall():
            # Create the basic entry
            entry = {
                "username": row[0],
                "timestamp": row[1].isoformat() if row[1] else None,
                "current_ip": row[2],
                "previous_ip": row[3],
                "previous_timestamp": row[4].isoformat() if row[4] else None,
                "minutes_since_previous": row[5]
            }
            
            # Add location information for both IPs
            entry["current_ip_location"] = get_ip_location(row[2])
            entry["previous_ip_location"] = get_ip_location(row[3])
            
            rapid_ip_changes.append(entry)
        
        # Find unusual access times - ensure time is available
        # Fixed query to use hour_of_day instead of hour
        cursor.execute('''
        SELECT 
            username, 
            timestamp, 
            source_ip,
            DATEPART(hour, timestamp) as hour_of_day
        FROM vpn_logs
        WHERE 
            timestamp IS NOT NULL AND
            (DATEPART(hour, timestamp) < 7 OR DATEPART(hour, timestamp) > 19)  -- Outside 7am-7pm
        ORDER BY timestamp
        ''')
        
        unusual_hours = []
        for row in cursor.fetchall():
            entry = {
                "username": row[0],
                "timestamp": row[1].isoformat() if row[1] else None,
                "source_ip": row[2],
                "hour": row[3]
            }
            
            # Add location information
            entry["location"] = get_ip_location(row[2])
            
            unusual_hours.append(entry)
        
        return jsonify({
            "rapid_ip_changes": rapid_ip_changes,
            "unusual_hours": unusual_hours
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500