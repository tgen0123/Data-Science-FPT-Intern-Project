import os
import re
import numpy as np
import json

def sanitize_for_table_name(name):
    """
    Sanitize a string to be used as a safe SQL table name.
    """
    name = os.path.splitext(name)[0]
    name = re.sub(r'[\s\.\-]+', '_', name)
    name = re.sub(r'[^\w]', '', name)

    if name[0].isdigit():
        name = '_' + name
        
    return name

def convert_numpy_types(obj):
    """
    Recursively convert numpy types to Python native types for JSON serialization.
    
    Args:
        obj: Object that may contain numpy types
        
    Returns:
        Object with numpy types converted to Python types
    """
    if isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_types(item) for item in obj)
    elif isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif hasattr(obj, 'item'):  
        return obj.item()
    else:
        return obj

def safe_json_dumps(obj):
    """
    Safely serialize object to JSON, converting numpy types first.
    
    Args:
        obj: Object to serialize
        
    Returns:
        JSON string
    """
    try:
        return json.dumps(obj)
    except TypeError as e:
        if "not JSON serializable" in str(e):
            converted_obj = convert_numpy_types(obj)
            return json.dumps(converted_obj)
        else:
            raise e
