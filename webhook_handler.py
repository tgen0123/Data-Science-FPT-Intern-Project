import json
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Union
from flask import Blueprint, request, jsonify, Response
from auth import get_cursor, require_api_key
from database import get_db

# A type alias for JSON-like dictionaries for clearer type hinting.
JsonDict = Dict[str, Any]

webhook_bp = Blueprint('webhook', __name__, url_prefix='/api/webhook')

def parse_splunk_trigger_time(time_str: str) -> datetime:
    """
    Parses a time string from a Splunk webhook into a datetime object.

    This function iterates through a list of common time formats that Splunk uses.
    It handles various precisions, timezones, and the 'Z' (Zulu/UTC) suffix.
    If the provided string is empty or none of the formats match, it defaults
    to the current time.

    Args:
        time_str: The time string received from Splunk.

    Returns:
        A datetime object representing the parsed time, or the
        current time if parsing fails.
    """
    if not time_str:
        return datetime.now()
    
    # Common Splunk time formats
    time_formats: List[str] = [
        '%Y-%m-%d %H:%M:%S',           # 2024-07-02 10:30:00
        '%Y-%m-%dT%H:%M:%S',           # 2024-07-02T10:30:00
        '%Y-%m-%dT%H:%M:%SZ',          # 2024-07-02T10:30:00Z
        '%Y-%m-%dT%H:%M:%S.%f',        # 2024-07-02T10:30:00.123456
        '%Y-%m-%dT%H:%M:%S.%fZ',       # 2024-07-02T10:30:00.123456Z
        '%Y-%m-%dT%H:%M:%S%z',         # 2024-07-02T10:30:00+0700
        '%Y-%m-%dT%H:%M:%S.%f%z',      # 2024-07-02T10:30:00.123456+0700
    ]
    
    for fmt in time_formats:
        try:
            if time_str.endswith('Z'):
                time_str_clean: str = time_str[:-1]
                return datetime.strptime(time_str_clean, fmt.replace('Z', ''))
            else:
                return datetime.strptime(time_str, fmt)
        except ValueError:
            continue
    
    try:
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except:
        print(f"Could not parse time string: {time_str}, using current time")
        return datetime.now()

@webhook_bp.route('/splunk-alert', methods=['POST'])
def receive_splunk_alert() -> Tuple[Response, int]:
    """
    Receives and stores webhook alerts from Splunk in the database.

    This endpoint is designed to be called by Splunk when an alert is triggered.
    It extracts alert details from the incoming request (JSON, form, or raw text),
    parses them, and inserts them into the `splunk_alerts` table for later processing.
    In case of an error during processing, it attempts to log the failure itself
    as a special type of alert.

    Expected Splunk webhook payload may include:
    - 'search_name': Name of the saved search/alert.
    - 'severity': Alert severity (e.g., 'low', 'medium', 'high').
    - 'trigger_time': The timestamp when the alert was triggered.
    - 'result.count': Number of results that triggered the alert.
    - 'search': The search query that was executed.

    Returns:
        A tuple containing a Flask JSON response and an HTTP status code.
        - On success, returns a success message and the new alert's ID with a 200 status.
        - On failure, returns an error message with a 500 status.
    """
    try:

        source_ip: str = request.headers.get('X-Forwarded-For', request.remote_addr or 'unknown')
        user_agent: str = request.headers.get('User-Agent', '')
        alert_data: JsonDict = {}
        
        if request.is_json:
            alert_data = request.get_json() or {}
            print("Received JSON webhook data from Splunk")
        elif request.form:
            alert_data = request.form.to_dict()
            print("Received form data webhook from Splunk")
        else:
            raw_data: str = request.get_data(as_text=True)
            if raw_data:
                try:
                    alert_data = json.loads(raw_data)
                    print("Received raw JSON webhook data from Splunk")
                except:
                    alert_data = {'raw_data': raw_data}
                    print("Received raw text webhook data from Splunk")
        
        print(f"Alert data received: {alert_data}")
        print(f"Source IP: {source_ip}")
        print(f"User Agent: {user_agent}")

        alert_name: str = alert_data.get('search_name', alert_data.get('alert_name', 'Unknown Alert'))
        search_name: str = alert_data.get('search_name', '')
        severity: str = alert_data.get('severity', alert_data.get('alert_severity', 'medium')).lower()
        trigger_time_str: str = alert_data.get('trigger_time', alert_data.get('_time', ''))
        trigger_time: datetime = parse_splunk_trigger_time(trigger_time_str)
        result_count: int = 0
        result_count_fields: List[str] = ['result.count', 'result_count', 'count', 'results_count']
        for field in result_count_fields:
            if field in alert_data:
                try:
                    result_count = int(alert_data[field])
                    break
                except (ValueError, TypeError):
                    continue
        
        search_query: str = alert_data.get('search', alert_data.get('query', ''))
        alert_data_json: str = json.dumps(alert_data, default=str)

        db = get_db()
        cursor = db.cursor()
        
        cursor.execute('''
        INSERT INTO splunk_alerts 
        (alert_name, search_name, severity, trigger_time, result_count, search_query, 
         alert_data, source_ip, user_agent, processed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        ''', (alert_name, search_name, severity, trigger_time, result_count, 
              search_query, alert_data_json, source_ip, user_agent))
        
        cursor.execute('SELECT @@IDENTITY')
        alert_id_result = cursor.fetchone()
        alert_id: Optional[int] = alert_id_result[0] if alert_id_result else None
        
        db.commit()
        
        print(f"Stored Splunk alert with ID: {alert_id}")
        
        response_data: JsonDict = {
            "status": "success",
            "message": "Alert received and stored successfully",
            "alert_id": alert_id,
        }
        
        return jsonify(response_data), 200
        
    except Exception as e:
        error_msg: str = f"Error processing Splunk alert: {str(e)}"
        print(f"{error_msg}")
        traceback.print_exc()

        try:
            db = get_db()
            cursor = db.cursor()
            cursor.execute('''
            INSERT INTO splunk_alerts 
            (alert_name, search_name, severity, alert_data, source_ip, user_agent, 
             processed, error_message)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            ''', ('ERROR_PROCESSING_ALERT', 'webhook_error', 'high', 
                  json.dumps({"error": str(e), "request_data": str(request.get_data())}),
                  request.headers.get('X-Forwarded-For', request.remote_addr or 'unknown'), 
                  request.headers.get('User-Agent', ''), error_msg))
            db.commit()
        except:
            pass  
        
        return jsonify({
            "status": "error",
            "message": error_msg,
        }), 500

@webhook_bp.route('/splunk-alerts', methods=['GET'])
@require_api_key
def get_splunk_alerts() -> Tuple[Response, int]:
    """
    Retrieves stored Splunk alerts from the database with filtering and pagination.
    
    This endpoint requires API key authentication. It allows clients to query the
    collected alerts based on various criteria.

    Query Parameters:
        limit (int): The maximum number of alerts to return. Defaults to 50.
        offset (int): The number of alerts to skip, for pagination. Defaults to 0.
        processed (str): Filters by processed status ('true' or 'false').
        severity (str): Filters by a specific severity level (e.g., 'high').
        from_date (str): Filters for alerts received on or after this date (YYYY-MM-DD).
        to_date (str): Filters for alerts received on or before this date (YYYY-MM-DD).
        search (str): A search term to match against the alert name or search name.

    Returns:
        A tuple containing a Flask JSON response and an HTTP status code.
        The JSON object includes a list of alerts, total count, pagination details,
        applied filters, and summary statistics. Returns 200 on success, 500 on error.
    """
    try:
        cursor = get_cursor()
        
        # Get query parameters with defaults
        limit: int = request.args.get('limit', 50, type=int)
        offset: int = request.args.get('offset', 0, type=int)
        processed: Optional[str] = request.args.get('processed', None)
        severity: Optional[str] = request.args.get('severity', None)
        from_date: Optional[str] = request.args.get('from_date', None)
        to_date: Optional[str] = request.args.get('to_date', None)
        search_term: Optional[str] = request.args.get('search', None)
        
        # Build WHERE clause and parameters
        where_conditions: List[str] = []
        params: List[Any] = []
        
        if processed is not None:
            where_conditions.append("processed = ?")
            params.append(1 if processed.lower() == 'true' else 0)
        
        if severity:
            where_conditions.append("severity = ?")
            params.append(severity.lower())
        
        if from_date:
            where_conditions.append("CAST(received_at AS DATE) >= ?")
            params.append(from_date)
        
        if to_date:
            where_conditions.append("CAST(received_at AS DATE) <= ?")
            params.append(to_date)
        
        if search_term:
            where_conditions.append("(alert_name LIKE ? OR search_name LIKE ?)")
            search_pattern: str = f"%{search_term}%"
            params.extend([search_pattern, search_pattern])
        
        where_clause: str = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Get alerts with pagination
        query: str = f'''
        SELECT id, alert_name, search_name, severity, trigger_time, result_count, 
               search_query, alert_data, source_ip, user_agent, received_at, 
               processed, processed_at, error_message
        FROM splunk_alerts
        {where_clause}
        ORDER BY received_at DESC
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
        '''
        
        cursor.execute(query, params)
        alerts: List[JsonDict] = []
        
        for row in cursor.fetchall():
            # Parse alert_data JSON
            alert_data_dict: JsonDict = {}
            try:
                alert_data_dict = json.loads(row[7]) if row[7] else {}
            except:
                alert_data_dict = {"raw_data": row[7]}
            
            alerts.append({
                "id": row[0],
                "alert_name": row[1],
                "search_name": row[2],
                "severity": row[3],
                "trigger_time": row[4].isoformat() if row[4] else None,
                "result_count": row[5],
                "search_query": row[6],
                "alert_data": alert_data_dict,
                "source_ip": row[8],
                "user_agent": row[9],
                "received_at": row[10].isoformat() if row[10] else None,
                "processed": bool(row[11]),
                "processed_at": row[12].isoformat() if row[12] else None,
                "error_message": row[13]
            })
        
        # Get total count
        count_query: str = f"SELECT COUNT(*) FROM splunk_alerts {where_clause}"
        cursor.execute(count_query, params)
        total_count_result = cursor.fetchone()
        total_count: int = total_count_result[0] if total_count_result else 0
        
        # Get summary statistics
        stats_query: str = '''
        SELECT 
            severity,
            COUNT(*) as count,
            COUNT(CASE WHEN processed = 1 THEN 1 END) as processed_count
        FROM splunk_alerts
        GROUP BY severity
        '''
        cursor.execute(stats_query)
        severity_stats: JsonDict = {}
        for row in cursor.fetchall():
            severity_stats[row[0]] = {
                "total": row[1],
                "processed": row[2],
                "unprocessed": row[1] - row[2]
            }
        
        return jsonify({
            "alerts": alerts,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
            "filters": {
                "processed": processed,
                "severity": severity,
                "from_date": from_date,
                "to_date": to_date,
                "search": search_term
            },
            "statistics": {
                "by_severity": severity_stats,
                "total_alerts": total_count
            }
        })
        
    except Exception as e:
        print(f"Error getting Splunk alerts: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@webhook_bp.route('/splunk-alerts/<int:alert_id>', methods=['GET'])
@require_api_key
def get_splunk_alert_details(alert_id: int) -> Tuple[Response, int]:
    """
    Retrieves detailed information about a specific Splunk alert by its ID.

    This endpoint requires API key authentication and provides a way to fetch all
    stored data for a single alert.

    Args:
        alert_id: The unique identifier of the alert to retrieve, passed
                  in the URL path.

    Returns:
        A tuple containing a Flask JSON response and an HTTP status code.
        - On success, returns the alert's details with a 200 status.
        - If the alert is not found, returns an error message with a 404 status.
        - On database or other errors, returns an error message with a 500 status.
    """
    try:
        cursor = get_cursor()
        
        cursor.execute('''
        SELECT id, alert_name, search_name, severity, trigger_time, result_count, 
               search_query, alert_data, source_ip, user_agent, received_at, 
               processed, processed_at, error_message
        FROM splunk_alerts
        WHERE id = ?
        ''', (alert_id,))
        
        row = cursor.fetchone()
        
        if not row:
            return jsonify({"error": "Alert not found"}), 404
        
        # Parse alert_data JSON
        alert_data_dict: JsonDict = {}
        try:
            alert_data_dict = json.loads(row[7]) if row[7] else {}
        except:
            alert_data_dict = {"raw_data": row[7]}
        
        alert_details: JsonDict = {
            "id": row[0],
            "alert_name": row[1],
            "search_name": row[2],
            "severity": row[3],
            "trigger_time": row[4].isoformat() if row[4] else None,
            "result_count": row[5],
            "search_query": row[6],
            "alert_data": alert_data_dict,
            "source_ip": row[8],
            "user_agent": row[9],
            "received_at": row[10].isoformat() if row[10] else None,
            "processed": bool(row[11]),
            "processed_at": row[12].isoformat() if row[12] else None,
            "error_message": row[13]
        }
        
        return jsonify(alert_details)
        
    except Exception as e:
        print(f"Error getting alert details: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@webhook_bp.route('/splunk-alerts/<int:alert_id>/mark-processed', methods=['POST'])
@require_api_key
def mark_alert_processed(alert_id: int) -> Tuple[Response, int]:
    """
    Marks a specific Splunk alert as processed.

    This endpoint requires API key authentication. It updates the alert's `processed`
    status to true and sets the `processed_at` timestamp.

    Args:
        alert_id: The unique identifier for the alert to be marked as processed.

    Returns:
        A tuple containing a Flask JSON response and an HTTP status code.
        - On success, returns a success message with a 200 status.
        - If the alert is not found, returns an error message with a 404 status.
        - On database or other errors, returns an error message with a 500 status.
    """
    try:
        db = get_db()
        cursor = db.cursor()
        
        cursor.execute('''
        UPDATE splunk_alerts 
        SET processed = 1, processed_at = GETDATE()
        WHERE id = ?
        ''', (alert_id,))
        
        if cursor.rowcount == 0:
            return jsonify({"error": "Alert not found"}), 404
        
        db.commit()
        
        return jsonify({
            "status": "success",
            "message": f"Alert {alert_id} marked as processed",
            "processed_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Error marking alert as processed: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@webhook_bp.route('/test', methods=['POST', 'GET'])
def test_webhook() -> Tuple[Response, int]:
    """
    A simple test endpoint to verify that the webhook blueprint is working.

    It accepts both GET and POST requests and echoes back information about the
    request it received, such as the method, headers, and any data payload.

    Returns:
        A tuple containing a JSON confirmation and a 200 HTTP status code.
    """
    return jsonify({
        "status": "success",
        "message": "Webhook endpoint is working",
        "timestamp": datetime.now().isoformat(),
        "method": request.method,
        "headers": dict(request.headers),
        "data": request.get_json() if request.is_json else request.form.to_dict()
    })

# Health check endpoint
@webhook_bp.route('/health', methods=['GET'])
def health_check() -> Tuple[Response, int]:
    """
    Provides a health check endpoint for monitoring purposes.

    This endpoint can be used by automated systems to verify the service's status.
    It checks for basic functionality, such as database connectivity.

    Returns:
        A tuple containing a JSON response with the system health status and a 200
        HTTP status code. The status of dependencies like the database is included.
    """
    try:
        # Test database connectivity
        cursor = get_cursor()
        cursor.execute('SELECT 1')
        db_status: str = "healthy"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database": db_status,
        "webhook_endpoint": "/api/webhook/splunk-alert"
    })