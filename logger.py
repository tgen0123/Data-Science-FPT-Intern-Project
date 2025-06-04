import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

# Create logs directory if it doesn't exist
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

def setup_logger(name, log_file, level=logging.INFO):
    """Function to setup a logger with both file and console handlers"""
    
    # Create formatter
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create and set up the file handler with rotation
    file_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, log_file),
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # Create and set up the console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Get or create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers if any
    logger.handlers = []
    
    # Add the handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Create different loggers for different components
app_logger = setup_logger('app', 'app.log')
# db_logger = setup_logger('database', 'database.log')
api_logger = setup_logger('api', 'api.log')
data_logger = setup_logger('data', 'data.log')
security_logger = setup_logger('security', 'security.log')
error_logger = setup_logger('error', 'error.log', level=logging.ERROR)  # Dedicated error logger
query_logger = setup_logger('query', 'database.log')  # Dedicated query logger

def log_error(logger, error, additional_info=None):
    """Utility function to log errors with stack trace"""
    import traceback
    error_msg = f"Error: {str(error)}"
    if additional_info:
        error_msg += f" | Additional Info: {additional_info}"
    # Log to both the component logger and the dedicated error logger
    logger.error(error_msg)
    error_logger.error(error_msg)
    stack_trace = f"Stack Trace: {''.join(traceback.format_tb(error.__traceback__))}"
    logger.error(stack_trace)
    error_logger.error(stack_trace)

def log_request(logger, request, response=None):
    """Utility function to log HTTP requests and responses"""
    try:
        request_info = {
            'method': request.method,
            'url': request.url,
            'headers': dict(request.headers),
            'data': request.get_data(as_text=True)
        }
        logger.info(f"Request: {request_info}")
        
        if response:
            response_info = {
                'status': response.status_code,
                'headers': dict(response.headers),
                'data': response.get_data(as_text=True)
            }
            logger.info(f"Response: {response_info}")
            
    except Exception as e:
        log_error(logger, e, "Error logging request/response")

def log_db_query(logger, query, params=None, duration=None):
    """Utility function to log database queries"""
    try:
        log_entry = f"SQL Query: {query}"
        if params:
            log_entry += f" | Params: {params}"
        if duration:
            log_entry += f" | Duration: {duration:.2f}s"
        # Log to both the component logger and the dedicated query logger
        logger.debug(log_entry)
        
    except Exception as e:
        log_error(error_logger, e, "Error logging database query")
