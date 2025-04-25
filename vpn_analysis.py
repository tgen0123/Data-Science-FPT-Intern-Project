# vpn_analysis.py
import pandas as pd
import requests
from flask import Blueprint, jsonify, request, g, current_app
from functools import wraps
import pyodbc
import numpy as np
from datetime import datetime

# Import preprocessing module
from data_preprocessor import clean_username, extract_department
from helper import get_db, get_cursor, dict_from_row, require_api_key, is_admin

# Create a Blueprint for VPN analysis routes
vpn_bp = Blueprint('vpn', __name__, url_prefix='/api/vpn')

# ---- Helper Functions ----

def get_processed_tables():
    """
    Get a list of all processed data tables.
    
    Returns:
        list: List of table names
    """
    try:
        cursor = get_cursor()
        cursor.execute('''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE 'processed_data_%'
        ORDER BY table_name
        ''')
        
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error getting processed tables: {e}")
        return []

def get_default_table():
    """
    Get the most recent processed data table.
    If multiple tables exist, returns the one with the highest file_id.
    
    Returns:
        str: Table name or None if no tables exist
    """
    tables = get_processed_tables()
    if not tables:
        return None
    
    # Return the table with the highest ID (likely the most recent)
    # Table names are in format 'processed_data_X' where X is the file_id
    return max(tables, key=lambda t: int(t.split('_')[-1]))

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

# ---- API Endpoints ----

@vpn_bp.route('/stats', methods=['GET'])
@require_api_key
def vpn_stats():
    """Get VPN usage statistics."""
    try:
        # Get table to analyze
        table_name = request.args.get('table', get_default_table())
        if not table_name:
            return jsonify({"error": "No processed data tables found"}), 404
        
        # If a specific file_id is provided, use its table
        file_id = request.args.get('file_id')
        if file_id:
            table_name = f"processed_data_{file_id}"
        
        # Validate that the table exists
        cursor = get_cursor()
        cursor.execute(f'''
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        ''')
        
        if cursor.fetchone()[0] == 0:
            return jsonify({"error": f"Table {table_name} not found"}), 404
        
        # Get total VPN connection count
        cursor.execute(f'SELECT COUNT(*) FROM [{table_name}]')
        total_connections = cursor.fetchone()[0]
        
        # Get unique user count - using subjectusername
        cursor.execute(f'SELECT COUNT(DISTINCT subjectusername) FROM [{table_name}]')
        unique_users = cursor.fetchone()[0]
        
        # Get unique source IPs - using callingstationid
        cursor.execute(f'SELECT COUNT(DISTINCT callingstationid) FROM [{table_name}]')
        unique_ips = cursor.fetchone()[0]
        
        # Get connections by department
        cursor.execute(f'''
        SELECT 
            department,
            COUNT(*) as count 
        FROM [{table_name}]
        GROUP BY department
        ORDER BY count DESC
        ''')
        
        departments = {}
        for row in cursor.fetchall():
            dept_name = row[0] if row[0] else 'Unknown'
            departments[dept_name] = row[1]
        
        # Get connections by VPN gateway - using nasidentifier
        cursor.execute(f'''
        SELECT 
            nasidentifier,
            COUNT(*) as count 
        FROM [{table_name}]
        GROUP BY nasidentifier
        ORDER BY count DESC
        ''')
        
        gateways = {}
        for row in cursor.fetchall():
            gateway_name = row[0] if row[0] else 'Unknown'
            gateways[gateway_name] = row[1]
        
        # Get connections by time category
        cursor.execute(f'''
        SELECT 
            time_category, 
            COUNT(*) as count 
        FROM [{table_name}]
        WHERE time_category IS NOT NULL
        GROUP BY time_category
        ORDER BY count DESC
        ''')
        
        time_categories = {}
        for row in cursor.fetchall():
            category = row[0] if row[0] else 'Unknown'
            time_categories[category] = row[1]
        
        # Get connections by hour
        cursor.execute(f'''
        SELECT 
            hour_of_day, 
            COUNT(*) as count 
        FROM [{table_name}]
        WHERE hour_of_day IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        ''')
        
        hourly = {}
        for row in cursor.fetchall():
            hourly[str(row[0])] = row[1]
        
        return jsonify({
            "table_name": table_name,
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
        # Get table to analyze
        table_name = request.args.get('table', get_default_table())
        if not table_name:
            return jsonify({"error": "No processed data tables found"}), 404
        
        # If a specific file_id is provided, use its table
        file_id = request.args.get('file_id')
        if file_id:
            table_name = f"processed_data_{file_id}"
        
        # Validate that the table exists
        cursor = get_cursor()
        cursor.execute(f'''
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        ''')
        
        if cursor.fetchone()[0] == 0:
            return jsonify({"error": f"Table {table_name} not found"}), 404
        
        # Get user's connection count - using subjectusername
        cursor.execute(f'SELECT COUNT(*) FROM [{table_name}] WHERE subjectusername = ?', (username,))
        connection_count = cursor.fetchone()[0]
        
        if connection_count == 0:
            return jsonify({"error": "User not found in VPN logs"}), 404
        
        # Get user's distinct IPs - using callingstationid
        cursor.execute(f'''
        SELECT callingstationid, COUNT(*) as count 
        FROM [{table_name}]
        WHERE subjectusername = ? 
        GROUP BY callingstationid 
        ORDER BY count DESC
        ''', (username,))
        
        ips = []
        for row in cursor.fetchall():
            ips.append({
                "ip": row[0],
                "connection_count": row[1]
            })
        
        # Get user's connection history with time categories
        cursor.execute(f'''
        SELECT timestamp, callingstationid, nasidentifier, department, accountsessionidentifier, time_category
        FROM [{table_name}]
        WHERE subjectusername = ? 
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
        cursor.execute(f'''
        SELECT 
            time_category, 
            COUNT(*) as count 
        FROM [{table_name}]
        WHERE subjectusername = ? AND time_category IS NOT NULL
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
            "table_name": table_name,
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
        # Get table to analyze
        table_name = request.args.get('table', get_default_table())
        if not table_name:
            return jsonify({"error": "No processed data tables found"}), 404
        
        # If a specific file_id is provided, use its table
        file_id = request.args.get('file_id')
        if file_id:
            table_name = f"processed_data_{file_id}"
        
        # Validate that the table exists
        cursor = get_cursor()
        cursor.execute(f'''
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        ''')
        
        if cursor.fetchone()[0] == 0:
            return jsonify({"error": f"Table {table_name} not found"}), 404
        
        # Find users connecting from multiple IPs within a short time window
        cursor.execute(f'''
        WITH UserIpTimeDiff AS (
            SELECT 
                subjectusername as username,
                timestamp,
                callingstationid as source_ip,
                LAG(callingstationid) OVER (PARTITION BY subjectusername ORDER BY timestamp) AS prev_ip,
                LAG(timestamp) OVER (PARTITION BY subjectusername ORDER BY timestamp) AS prev_timestamp,
                DATEDIFF(minute, LAG(timestamp) OVER (PARTITION BY subjectusername ORDER BY timestamp), timestamp) AS time_diff_minutes
            FROM [{table_name}]
            WHERE timestamp IS NOT NULL
        )
        SELECT 
            UserIpTimeDiff.username, 
            UserIpTimeDiff.timestamp, 
            UserIpTimeDiff.source_ip, 
            UserIpTimeDiff.prev_ip, 
            UserIpTimeDiff.prev_timestamp,
            UserIpTimeDiff.time_diff_minutes,
            t.time_category
        FROM UserIpTimeDiff
        LEFT JOIN [{table_name}] t ON UserIpTimeDiff.username = t.subjectusername AND UserIpTimeDiff.timestamp = t.timestamp
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
        cursor.execute(f'''
        SELECT 
            subjectusername as username, 
            timestamp, 
            callingstationid as source_ip,
            hour_of_day,
            time_category
        FROM [{table_name}]
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
        cursor.execute(f'''
        WITH UserDeptCounts AS (
            SELECT 
                subjectusername as username,
                department,
                COUNT(*) as dept_count,
                SUM(COUNT(*)) OVER (PARTITION BY subjectusername) as total_user_connections
            FROM [{table_name}]
            WHERE department IS NOT NULL
            GROUP BY subjectusername, department
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
            "table_name": table_name,
            "rapid_ip_changes": rapid_ip_changes,
            "unusual_hours": unusual_hours,
            "unusual_departments": unusual_departments
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@vpn_bp.route('/tables', methods=['GET'])
@require_api_key
def list_available_tables():
    """List all available processed data tables for VPN analysis."""
    try:
        tables = get_processed_tables()
        if not tables:
            return jsonify({"error": "No processed data tables found"}), 404
        
        cursor = get_cursor()
        
        # Get record counts for each table
        table_stats = []
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
            count = cursor.fetchone()[0]
            
            # Extract file_id from table name
            file_id = int(table.replace('processed_data_', ''))
            
            # Get file info
            cursor.execute('''
            SELECT file_name, file_path
            FROM csv_registry
            WHERE id = ?
            ''', (file_id,))
            
            file_info = cursor.fetchone()
            if file_info:
                file_name, file_path = file_info
            else:
                file_name = None
                file_path = None
            
            table_stats.append({
                "table_name": table,
                "file_id": file_id,
                "file_name": file_name,
                "file_path": file_path,
                "record_count": count
            })
        
        return jsonify({
            "default_table": get_default_table(),
            "available_tables": table_stats
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500