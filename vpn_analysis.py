# vpn_analysis.py
import pandas as pd
import requests
from flask import Blueprint, jsonify, request, g, current_app
from functools import wraps
import pyodbc
import numpy as np
from datetime import datetime

# Import preprocessing module
from data_preprocessor import preprocess_data, clean_username, extract_department
from helper import get_db, get_cursor, dict_from_row, require_api_key, is_admin

# Create a Blueprint for VPN analysis routes
vpn_bp = Blueprint('vpn', __name__, url_prefix='/api/vpn')

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
        
        # Extract timestamp - using timestamp from cleaned data
        if 'timestamp' in df.columns:
            # Convert timestamp string to datetime for database storage
            vpn_df['timestamp'] = pd.to_datetime(
                df['timestamp'].str.replace('\\+07:00', ''), 
                errors='coerce'
            )
        elif '_time' in df.columns:
            # Fallback to _time column if timestamp doesn't exist
            vpn_df['timestamp'] = pd.to_datetime(df['_time'], errors='coerce')
        
        # Extract username - using cleaned username
        if 'subjectusername' in df.columns:
            vpn_df['username'] = df['subjectusername']
        
        # Extract source IP - use CallingStationID directly
        if 'callingstationid' in df.columns:
            vpn_df['source_ip'] = df['callingstationid']
        
        # Extract department from already processed field if available
        if 'department' in df.columns:
            vpn_df['department'] = df['department']
        # Fall back to extracting it ourselves
        elif 'fullyqualifiedsubjectusername' in df.columns:
            vpn_df['department'] = df['fullyqualifiedsubjectusername'].apply(extract_department)
        
        # Extract VPN gateway
        if 'nasidentifier' in df.columns:
            vpn_df['vpn_gateway'] = df['nasidentifier']
        
        # Extract session ID
        if 'accountsessionidentifier' in df.columns:
            vpn_df['session_id'] = df['accountsessionidentifier']
        
        # Extract hour_of_day if available (this was missing before)
        if 'hour_of_day' in df.columns:
            vpn_df['hour_of_day'] = df['hour_of_day']
        
        # Add time category if available
        if 'time_category' in df.columns:
            vpn_df['time_category'] = df['time_category']
        # Otherwise calculate it if we have hour_of_day
        elif 'hour_of_day' in df.columns:
            vpn_df['time_category'] = pd.cut(
                df['hour_of_day'],
                bins=[0, 6, 12, 18, 24],
                labels=['night_(0-6)', 'morning_(6-12)', 'afternoon_(12-18)', 'evening_(18-24)'],
                include_lowest=True
            )
        # Otherwise calculate from timestamp
        elif 'timestamp' in vpn_df.columns:
            # Only calculate hour_of_day if it's not already set
            if 'hour_of_day' not in vpn_df.columns:
                vpn_df['hour_of_day'] = vpn_df['timestamp'].dt.hour
            vpn_df['time_category'] = pd.cut(
                vpn_df['hour_of_day'],
                bins=[0, 6, 12, 18, 24],
                labels=['night_(0-6)', 'morning_(6-12)', 'afternoon_(12-18)', 'evening_(18-24)'],
                include_lowest=True
            )
        
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
        # Create vpn_logs table if it doesn't exist - modified to include time_category
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
                [session_id] NVARCHAR(100),
                [hour_of_day] INT,
                [time_category] NVARCHAR(50)
            )
            
            CREATE INDEX [idx_vpn_username] ON [dbo].[vpn_logs] ([username])
            CREATE INDEX [idx_vpn_source_ip] ON [dbo].[vpn_logs] ([source_ip])
            CREATE INDEX [idx_vpn_time_category] ON [dbo].[vpn_logs] ([time_category])
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
                
                # Get time category if available
                hour_of_day = row.get('hour_of_day', None)
                if hour_of_day is not None:
                    hour_of_day = int(hour_of_day)
                
                time_category = str(row.get('time_category', '')) if not pd.isna(row.get('time_category')) else None
                
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
                (timestamp, username, source_ip, department, vpn_gateway, session_id, hour_of_day, time_category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp,
                    username, 
                    source_ip, 
                    department, 
                    vpn_gateway, 
                    session_id,
                    hour_of_day,
                    time_category
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
        
        # Get connections by department
        cursor.execute('''
        SELECT 
            department,
            COUNT(*) as count 
        FROM vpn_logs 
        GROUP BY department
        ORDER BY count DESC
        ''')
        
        departments = {}
        for row in cursor.fetchall():
            dept_name = row[0] if row[0] else 'Unknown'
            departments[dept_name] = row[1]
        
        # Get connections by VPN gateway
        cursor.execute('''
        SELECT 
            vpn_gateway,
            COUNT(*) as count 
        FROM vpn_logs 
        GROUP BY vpn_gateway
        ORDER BY count DESC
        ''')
        
        gateways = {}
        for row in cursor.fetchall():
            gateway_name = row[0] if row[0] else 'Unknown'
            gateways[gateway_name] = row[1]
        
        # Get connections by time category (new analysis based on notebook)
        cursor.execute('''
        SELECT 
            time_category, 
            COUNT(*) as count 
        FROM vpn_logs 
        WHERE time_category IS NOT NULL
        GROUP BY time_category
        ORDER BY count DESC
        ''')
        
        time_categories = {}
        for row in cursor.fetchall():
            category = row[0] if row[0] else 'Unknown'
            time_categories[category] = row[1]
        
        # Get connections by hour - extract hour from timestamp
        cursor.execute('''
        SELECT 
            hour_of_day, 
            COUNT(*) as count 
        FROM vpn_logs 
        WHERE hour_of_day IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        ''')
        
        hourly = {}
        for row in cursor.fetchall():
            hourly[str(row[0])] = row[1]
        
        return jsonify({
            "total_connections": total_connections,
            "unique_users": unique_users,
            "unique_source_ips": unique_ips,
            "connections_by_department": departments,
            "connections_by_gateway": gateways,
            "connections_by_time_category": time_categories,
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
        
        # Get user's connection history with time categories
        cursor.execute('''
        SELECT timestamp, source_ip, vpn_gateway, department, session_id, time_category
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
                "session_id": row[4],
                "time_category": row[5]
            })
        
        # Get time category distribution
        cursor.execute('''
        SELECT 
            time_category, 
            COUNT(*) as count 
        FROM vpn_logs 
        WHERE username = ? AND time_category IS NOT NULL
        GROUP BY time_category
        ''', (username,))
        
        time_categories = {}
        for row in cursor.fetchall():
            category = row[0] if row[0] else 'Unknown'
            time_categories[category] = row[1]
        
        # Get location info for each IP
        for ip_entry in ips:
            ip = ip_entry["ip"]
            ip_entry["location"] = get_ip_location(ip)
        
        return jsonify({
            "username": username,
            "connection_count": connection_count,
            "source_ips": ips,
            "time_category_distribution": time_categories,
            "connection_history": history
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
            UserIpTimeDiff.username, 
            UserIpTimeDiff.timestamp, 
            UserIpTimeDiff.source_ip, 
            UserIpTimeDiff.prev_ip, 
            UserIpTimeDiff.prev_timestamp,
            UserIpTimeDiff.time_diff_minutes,
            vpn_logs.time_category
        FROM UserIpTimeDiff
        LEFT JOIN vpn_logs ON UserIpTimeDiff.username = vpn_logs.username AND UserIpTimeDiff.timestamp = vpn_logs.timestamp
        WHERE 
            UserIpTimeDiff.prev_ip IS NOT NULL AND 
            UserIpTimeDiff.source_ip <> UserIpTimeDiff.prev_ip AND 
            UserIpTimeDiff.time_diff_minutes < 60  -- Less than 60 minutes between different IPs
        ORDER BY UserIpTimeDiff.username, UserIpTimeDiff.timestamp
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
                "minutes_since_previous": row[5],
                "time_category": row[6]
            }
            
            # Add location information for both IPs
            entry["current_ip_location"] = get_ip_location(row[2])
            entry["previous_ip_location"] = get_ip_location(row[3])
            
            rapid_ip_changes.append(entry)
        
        # Find unusual access times - we'll use the time categories now
        cursor.execute('''
        SELECT 
            username, 
            timestamp, 
            source_ip,
            hour_of_day,
            time_category
        FROM vpn_logs
        WHERE 
            time_category = 'night_(0-6)'  -- Access during night hours
        ORDER BY timestamp
        ''')
        
        unusual_hours = []
        for row in cursor.fetchall():
            entry = {
                "username": row[0],
                "timestamp": row[1].isoformat() if row[1] else None,
                "source_ip": row[2],
                "hour": row[3],
                "time_category": row[4]
            }
            
            # Add location information
            entry["location"] = get_ip_location(row[2])
            
            unusual_hours.append(entry)
        
        # Find users with connections from unusual departments
        cursor.execute('''
        WITH UserDeptCounts AS (
            SELECT 
                username,
                department,
                COUNT(*) as dept_count,
                SUM(COUNT(*)) OVER (PARTITION BY username) as total_user_connections
            FROM vpn_logs
            WHERE department IS NOT NULL
            GROUP BY username, department
        )
        SELECT
            username,
            department,
            dept_count,
            total_user_connections,
            CAST(dept_count AS FLOAT) / total_user_connections as dept_ratio
        FROM UserDeptCounts
        WHERE 
            total_user_connections > 5 AND  -- User has sufficient history
            dept_count = 1 AND              -- Only connected once from this department
            CAST(dept_count AS FLOAT) / total_user_connections < 0.1  -- Represents less than 10% of connections
        ORDER BY username, dept_ratio
        ''')
        
        unusual_departments = []
        for row in cursor.fetchall():
            unusual_departments.append({
                "username": row[0],
                "unusual_department": row[1],
                "connection_count": row[2],
                "total_connections": row[3],
                "percent_of_total": round(row[4] * 100, 2)
            })
        
        return jsonify({
            "rapid_ip_changes": rapid_ip_changes,
            "unusual_hours": unusual_hours,
            "unusual_departments": unusual_departments
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500