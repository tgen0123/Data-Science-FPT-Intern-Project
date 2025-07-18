import pandas as pd
import numpy as np
import re
import os
import collections
import traceback

def separate_datetime_columns(df):
    """
    NEW FUNCTION: Separate datetime columns into separate date and time columns while keeping the original.
    
    Args:
        df: DataFrame to process
        
    Returns:
        tuple: (DataFrame with additional date/time columns, dict with separation info)
    """
    datetime_columns = []
    separation_info = {}
    
    print("\n=== SEPARATING DATETIME COLUMNS ===")
    
    # Find all datetime columns
    for col in df.columns:
        col_dtype_str = str(df[col].dtype)
        if 'datetime64' in col_dtype_str:
            datetime_columns.append(col)
    
    if not datetime_columns:
        print("No datetime columns found to separate")
        return df, {}
    
    print(f"Found {len(datetime_columns)} datetime columns to separate: {datetime_columns}")
    
    for col in datetime_columns:
        try:
            print(f"\nProcessing datetime column: '{col}'")
            
            # Check if we have any valid datetime values
            valid_datetimes = df[col].notna()
            valid_count = valid_datetimes.sum()
            
            if valid_count == 0:
                print(f"   Column '{col}' has no valid datetime values, skipping separation")
                continue
            
            print(f"   Valid datetime values: {valid_count}/{len(df)} ({valid_count/len(df):.1%})")
            
            # Create new column names
            date_col_name = f"{col}_date"
            hour_col_name = f"{col}_hour"
            
            # Ensure new column names don't conflict with existing ones
            counter = 1
            while date_col_name in df.columns:
                date_col_name = f"{col}_date_{counter}"
                counter += 1
            
            counter = 1
            while hour_col_name in df.columns:
                hour_col_name = f"{col}_time_{counter}"
                counter += 1
            
            print(f"   Creating columns: '{date_col_name}' and '{hour_col_name}'")
            
            # Extract date component (removes time, keeps date only)
            df[date_col_name] = df[col].dt.date
            
            # Extract time component (removes date, keeps time only)
            df[hour_col_name] = df[col].dt.hour
            
            # Count successful extractions
            date_count = df[date_col_name].notna().sum()
            hour_count = df[hour_col_name].notna().sum()
            
            print(f"   Successfully separated '{col}':")
            print(f"     - Date column '{date_col_name}': {date_count} values")
            print(f"     - Hour column '{hour_col_name}': {hour_count} values")
            
            # Store separation info
            separation_info[col] = {
                'original_column': col,
                'date_column': date_col_name,
                'hour_column': hour_col_name,
                'original_valid_count': valid_count,
                'date_extracted_count': date_count,
                'hour_extracted_count': hour_col_name
            }
            
        except Exception as e:
            print(f"   Error separating datetime column '{col}': {e}")
            traceback.print_exc()
    
    if separation_info:
        print(f"\nSuccessfully separated {len(separation_info)} datetime columns:")
        for original_col, info in separation_info.items():
            print(f"  {original_col} → {info['date_column']} + {info['hour_column']}")
    else:
        print("\nNo datetime columns were separated")
    
    return df, separation_info

def deduplicate_columns(columns):
    """Deduplicate column names for compatibility - PRESERVES underscores."""
    counter = collections.Counter()
    new_cols = []
    seen = set()
    
    print("=== COLUMN NAME PROCESSING ===")
    
    for col in columns:
        original_col = col
        col = str(col).strip().lower()
            
        # Replace spaces, dots, hyphens with underscores, but keep existing underscores
        col = re.sub(r'[\s\.\-]+', '_', col)      # Replace whitespace, dots, hyphens
        col = re.sub(r'[^\w]', '_', col)          # Replace non-word chars (\w includes letters, digits, _)
        col = re.sub(r'_+', '_', col)             # Replace multiple underscores with single
        
        # Don't strip leading underscores - they might be meaningful!
        # Only strip trailing underscores if they were added
        col = col.rstrip('_') if col.endswith('_') and not original_col.strip().endswith('_') else col
        
        print(f"  '{original_col}' → '{col}'")
        
        if not col:
            col = 'unnamed'
        
        base_col = col
        suffix = counter[base_col]
        while col in seen:
            suffix += 1
            col = f"{base_col}_{suffix}"
        counter[base_col] += 1
        seen.add(col)
        new_cols.append(col)
    
    print(f"Final columns: {new_cols}")
    return new_cols

def detect_ip_columns(df):
    """
    Detect columns that likely contain IP addresses based on column names and content.
    
    Args:
        df: DataFrame to analyze
    Returns:
        dict: Dictionary containing detected IP columns and their analysis
    """
    ip_columns = []
    ip_analysis = {}
    ip_name_patterns = [

        r'\bip\b',                    # exact word "ip"
        r'^ip$',                      # exactly "ip" (redundant but explicit)
        r'^ip[_\-\.\w]*',            # starts with "ip" followed by any separator or word chars
        r'^ip\w+',                   # starts with "ip" followed directly by word chars (ipport, ipaddr, etc)
        r'\w*[_\-\.]ip$',            # ends with "ip" preceded by any chars and separator
        r'\w+ip$',                   # ends with "ip" preceded directly by word chars (portip, hostip, etc)
        r'[_\-\.]ip[_\-\.]',         # "ip" in the middle with separators
        r'\w+ip\w+',                 # "ip" in the middle without separators (clientipaddress, etc)
        r'_ip_',                     # _ip_ anywhere in name
        r'^_ip\w*',                  # starts with _ip
        r'\w*ip_',                   # ends with ip_
        r'_ip$',                     # ends with _ip
        r'\bsrc_?ip\b',              # source IP variations
        r'\bdst_?ip\b',              # destination IP variations
        r'\bclient_?ip\b',           # client IP variations
        r'\bserver_?ip\b',           # server IP variations
        r'\bremote_?ip\b',           # remote IP variations
        r'\blocal_?ip\b',            # local IP variations
        r'\bpublic_?ip\b',           # public IP variations
        r'\bprivate_?ip\b',          # private IP variations
        r'\bexternal_?ip\b',         # external IP variations
        r'\binternal_?ip\b',         # internal IP variations
        r'\bip_?addr(ess)?\b',       # IP address variations
        r'\baddr(ess)?_?ip\b',       # address IP variations
        r'ipaddr\w*',                # ipaddress, ipaddr, etc.
        r'\w*ipaddr',                # srcipaddr, clientipaddr, etc.
        r'\borigin_?ip\b',           # origin IP
        r'\btarget_?ip\b',           # target IP
        r'\bhost_?ip\b',             # host IP
        r'\bnode_?ip\b',             # node IP
        r'\bendpoint_?ip\b',         # endpoint IP
        r'hostip\w*',                # hostip, hostipaddr, etc.
        r'nodeip\w*',                # nodeip variations
        r'ipport\w*',                # ipport, ipportnum, etc.
        r'portip\w*',                # portip, portipaddr, etc.
        r'\w*ipport',                # srcipport, clientipport, etc.
        r'\w*portip',                # srcportip, clientportip, etc.
        r'\bip[/\\]',                # ip/ or ip\
        r'[/\\]ip\b',                # /ip or \ip
        r'\bip[-_.]',                # ip followed by dash, underscore, or dot
        r'[-_.]ip\b',                # ip preceded by dash, underscore, or dot
        r'ip\d+',                    # ip followed by numbers (ip1, ip2, etc.)
        r'\d+ip',                    # numbers followed by ip (1ip, 2ip, etc.)
        r'wan_?ip',                  # WAN IP
        r'lan_?ip',                  # LAN IP
        r'nat_?ip',                  # NAT IP
        r'vpn_?ip',                  # VPN IP
        r'proxy_?ip',                # Proxy IP
        r'gateway_?ip',              # Gateway IP
        r'router_?ip',               # Router IP
        r'firewall_?ip',             # Firewall IP
        r'real_?ip',                 # Real IP (behind proxy)
        r'peer_?ip',                 # Peer IP
        r'next_?ip',                 # Next hop IP
        r'prev_?ip',                 # Previous IP
        r'orig_?ip',                 # Original IP
        r'final_?ip',                # Final IP
        r'tcp_?ip',                  # TCP IP
        r'udp_?ip',                  # UDP IP
        r'icmp_?ip',                 # ICMP IP
        r'dhcp_?ip',                 # DHCP IP
        r'dns_?ip',                  # DNS IP
        r'wanip\w*',                 # wanip, wanipaddr, etc.
        r'lanip\w*',                 # lanip, lanipaddr, etc.
        r'natip\w*',                 # natip variations
        r'vpnip\w*',                 # vpnip variations
        r'tcpip\w*',                 # tcpip variations
        r'udpip\w*',                 # udpip variations
        r'(?<![a-z])ip(?![a-z])|ip\w+|\w+ip(?!\w)|(?<!\w)\w*ip\w*(?!\w)',  # Complex pattern to catch most IP variations while avoiding words like "ship", "zip"
    ]
    
    # Compile patterns for efficiency
    compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in ip_name_patterns]
    
    print("=== IP COLUMN DETECTION ===")
    
    for col in df.columns:
        col_lower = col.lower()
        is_ip_column = False
        matched_patterns = []
        
        # Check if column name matches IP patterns
        for i, pattern in enumerate(compiled_patterns):
            if pattern.search(col_lower):
                is_ip_column = True
                matched_patterns.append(ip_name_patterns[i])
        
        content_suggests_ip = False
        ip_ratio = 0.0
        
        if df[col].dtype == 'object':  # Only check string/object columns
            # Get sample of non-null values for testing
            sample_values = df[col].dropna().astype(str).head(100)
            
            if len(sample_values) > 0:
                # Enhanced IP address patterns
                ipv4_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
                ipv6_pattern = r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::1$|^::$'
                
                # Count valid IP addresses
                ipv4_matches = sample_values.str.match(ipv4_pattern).sum()
                ipv6_matches = sample_values.str.match(ipv6_pattern).sum()
                total_ip_matches = ipv4_matches + ipv6_matches
                
                ip_ratio = total_ip_matches / len(sample_values)
                
                # Consider it an IP column if >50% of values are valid IPs
                if ip_ratio > 0.5:
                    content_suggests_ip = True
        
        # Combine name-based and content-based detection
        if is_ip_column or content_suggests_ip:
            ip_columns.append(col)
            
            # Store analysis details
            ip_analysis[col] = {
                'detected_by_name': is_ip_column,
                'detected_by_content': content_suggests_ip,
                'matched_patterns': matched_patterns,
                'ip_content_ratio': ip_ratio,
                'column_original': col,
                'column_lower': col_lower
            }
            
            print(f"IP Column detected: '{col}'")
            if is_ip_column:
                print(f"   - Name-based detection: {matched_patterns}")
            if content_suggests_ip:
                print(f"   - Content-based detection: {ip_ratio:.1%} IP addresses")
    
    if ip_columns:
        print(f"\nTotal IP columns detected: {len(ip_columns)}")
        for col in ip_columns:
            analysis = ip_analysis[col]
            detection_method = []
            if analysis['detected_by_name']:
                detection_method.append("name")
            if analysis['detected_by_content']:
                detection_method.append("content")
            print(f"   - {col} (detected by: {', '.join(detection_method)})")
    else:
        print("\nNo IP columns detected")
    
    return {
        'ip_columns': ip_columns,
        'analysis': ip_analysis,
        'count': len(ip_columns)
    }

def process_ip_columns(df, ip_detection_result):
    """
    Process detected IP columns with appropriate data types and validation.
    
    Args:
        df: DataFrame to process
        ip_detection_result: Result from detect_ip_columns function
    
    Returns:
        DataFrame: DataFrame with processed IP columns
    """
    if not ip_detection_result['ip_columns']:
        print("No IP columns to process")
        return df
    
    print("\n=== PROCESSING IP COLUMNS ===")
    
    for col in ip_detection_result['ip_columns']:
        analysis = ip_detection_result['analysis'][col]
        
        try:
            # Clean and validate IP addresses
            print(f"Processing IP column: '{col}'")
            
            # Convert to string and clean whitespace
            df[col] = df[col].astype(str).str.strip()
            
            # Replace common invalid values with NaN
            invalid_values = ['', 'null', 'none', 'n/a', 'unknown', '0.0.0.0', 'nan']
            df[col] = df[col].replace(invalid_values, pd.NA, regex=False)
            
            # Validate IP addresses and mark invalid ones as NaN
            ipv4_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
            ipv6_pattern = r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::1$|^::$'
            
            # Create boolean masks for valid IPs
            valid_ipv4 = df[col].str.match(ipv4_pattern, na=False)
            valid_ipv6 = df[col].str.match(ipv6_pattern, na=False)
            valid_ip = valid_ipv4 | valid_ipv6
            
            # Count validation results
            total_non_null = df[col].notna().sum()
            valid_count = valid_ip.sum()
            invalid_count = total_non_null - valid_count
            
            print(f"   - Total non-null values: {total_non_null}")
            print(f"   - Valid IP addresses: {valid_count}")
            print(f"   - Invalid IP addresses: {invalid_count}")
            
            if invalid_count > 0:
                invalid_ips = df.loc[~valid_ip & df[col].notna(), col].unique()[:5]  # Show first 5 invalid IPs
                print(f"   - Sample invalid IPs: {list(invalid_ips)}")
            
            # Set appropriate data type (keep as string/object for IP addresses)
            # IP addresses are typically stored as strings in databases
            df[col] = df[col].astype('string')
            
            print(f"   Processed '{col}' as IP address column")
            
        except Exception as e:
            print(f"   Error processing IP column '{col}': {e}")
    
    return df

def detect_and_parse_dates(df, threshold=0.7):
    """
    Detect and parse potential date columns in the DataFrame.
    
    Args:
        df: DataFrame to analyze and modify
        threshold: Minimum ratio of valid dates required to convert column (default 0.7 = 70%)
    Returns:
        DataFrame: DataFrame with date columns converted to datetime
    """
    date_columns_converted = []
    
    for col in df.columns:
        if df[col].dtype == 'object':  # Only check string/object columns
            # Skip if column is mostly empty
            non_null_count = df[col].notna().sum()
            if non_null_count < len(df) * 0.1:  # Less than 10% non-null
                continue
                
            # Get sample of non-null values for testing
            sample_values = df[col].dropna().astype(str)
            if len(sample_values) == 0:
                continue
                
            # Check if column name suggests it's a date
            col_lower = col.lower()
            date_indicators = ['date', 'time', 'timestamp', 'created', 'updated', 'modified', 
                             'start', 'end', 'birth', 'dob', 'expire', 'due', 'schedule', '_time']
            
            # Enhanced date detection for column names
            name_suggests_date = (
                any(indicator in col_lower for indicator in date_indicators) or
                col_lower.startswith('_time') or 
                col_lower.endswith('_time') or
                col_lower == '_time'
            )
            
            # DEBUG: Print sample values to see what we're working with
            print(f"\n=== Analyzing column '{col}' ===")
            print(f"Sample values: {sample_values.head(3).tolist()}")
            print(f"Column name suggests date: {name_suggests_date}")
            
            # Try to parse as dates
            try:
                # Test with a sample first to avoid processing large columns unnecessarily
                test_sample = sample_values.head(min(1000, len(sample_values)))
                
                # Try multiple date parsing approaches
                parsed_sample = None
                successful_format = None
                
                # Approach 1: pandas infer_datetime_format
                try:
                    parsed_sample = pd.to_datetime(test_sample, errors='coerce', infer_datetime_format=True)
                except:
                    pass
                
                # Approach 2: Common date formats if first approach failed
                if parsed_sample is None or parsed_sample.notna().sum() < len(test_sample) * threshold:
                    common_formats = [
                        # Standard date formats
                        '%Y-%m-%d',           # 2024-01-15
                        '%m/%d/%Y',           # 01/15/2024
                        '%d/%m/%Y',           # 15/01/2024
                        '%d-%m-%Y',           # 15-01-2024
                        '%Y%m%d',             # 20240115
                        '%d-%b-%Y',           # 15-Jan-2024
                        '%b %d, %Y',          # Jan 15, 2024
                        
                        # DateTime formats
                        '%Y-%m-%d %H:%M:%S',  # 2024-01-15 10:30:00
                        '%m/%d/%Y %H:%M:%S',  # 01/15/2024 10:30:00
                        '%d/%m/%Y %H:%M:%S',  # 15/01/2024 10:30:00
                        '%d %m %Y %H:%M',     # 22 11 2024 00:00
                        
                        # ISO 8601 formats - FIXED ORDER (most specific first)
                        '%Y-%m-%dT%H:%M:%S.%f%z',  # 2024-11-22T00:00:02.000000+0700 (6 digits)
                        '%Y-%m-%dT%H:%M:%S%z',     # 2024-11-22T00:00:02+0700
                        '%Y-%m-%dT%H:%M:%S.%fZ',   # 2024-01-15T10:30:00.123456Z
                        '%Y-%m-%dT%H:%M:%S.%f',    # 2024-01-15T10:30:00.123456
                        '%Y-%m-%dT%H:%M:%SZ',      # 2024-01-15T10:30:00Z
                        '%Y-%m-%dT%H:%M:%S',       # 2024-01-15T10:30:00
                        
                        # Additional common formats
                        '%Y-%m-%d %H:%M:%S.%f',   # 2024-01-15 10:30:00.123456
                        '%m/%d/%Y %H:%M:%S.%f',   # 01/15/2024 10:30:00.123456
                    ]
                    
                    for date_format in common_formats:
                        try:
                            print(f"  Trying format: {date_format}")
                            
                            # Special handling for milliseconds vs microseconds
                            if '.%f' in date_format and any('.000' in str(val) for val in test_sample.head(3)):
                                # Convert milliseconds to microseconds for parsing
                                adjusted_sample = test_sample.str.replace(r'\.(\d{3})(\+|Z)', r'.\g<1>000\g<2>', regex=True)
                                print(f"    Adjusted sample for microseconds: {adjusted_sample.head(1).tolist()}")
                                format_parsed = pd.to_datetime(adjusted_sample, format=date_format, errors='coerce')
                            else:
                                format_parsed = pd.to_datetime(test_sample, format=date_format, errors='coerce')
                            
                            valid_ratio = format_parsed.notna().sum() / len(test_sample)
                            print(f"    Valid ratio: {valid_ratio:.2f} (threshold: {threshold})")
                            
                            if valid_ratio >= threshold:
                                parsed_sample = format_parsed
                                successful_format = date_format
                                print(f"Column '{col}' parsed using format: {date_format}")
                                break
                            
                        except Exception as format_error:
                            print(f"    Format {date_format} failed: {format_error}")
                            continue
                
                # Approach 3: Manual preprocessing for common timezone issues
                if parsed_sample is None:
                    print("Trying manual preprocessing for timezone issues...")
                    try:
                        # Handle the specific case of .000+timezone by converting to .000000
                        preprocessed_sample = test_sample.str.replace(
                            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{3})(\+\d{4})', 
                            r'\1.\2000\3', 
                            regex=True
                        )
                        
                        print(f"  Preprocessed sample: {preprocessed_sample.head(3).tolist()}")
                        
                        # Try parsing the preprocessed version
                        format_parsed = pd.to_datetime(preprocessed_sample, format='%Y-%m-%dT%H:%M:%S.%f%z', errors='coerce')
                        valid_ratio = format_parsed.notna().sum() / len(test_sample)
                        print(f"  Preprocessed valid ratio: {valid_ratio:.2f}")
                        
                        if valid_ratio >= threshold:
                            parsed_sample = format_parsed
                            successful_format = "manual_preprocessing"
                            print(f"Column '{col}' parsed using manual preprocessing")
                        
                    except Exception as e:
                        print(f"  Manual preprocessing failed: {e}")
                
                # Approach 4: Flexible pandas parsing as final fallback
                if parsed_sample is None:
                    print("Trying flexible pandas parsing as final fallback...")
                    try:
                        # This is more flexible but slower - handles many edge cases
                        parsed_sample = pd.to_datetime(test_sample, errors='coerce', utc=False)
                        valid_ratio = parsed_sample.notna().sum() / len(test_sample)
                        print(f"  Flexible parsing valid ratio: {valid_ratio:.2f}")
                        
                        if valid_ratio >= threshold:
                            successful_format = "flexible_pandas"
                            print(f"Column '{col}' parsed using flexible pandas parsing")
                            
                    except Exception as e:
                        print(f"  Flexible parsing failed: {e}")

                if parsed_sample is not None:
                    valid_ratio = parsed_sample.notna().sum() / len(test_sample)
                    
                    # Lower threshold if column name suggests it's a date
                    effective_threshold = threshold * 0.5 if name_suggests_date else threshold
                    print(f"Effective threshold: {effective_threshold:.2f}, Valid ratio: {valid_ratio:.2f}")
                    
                    if valid_ratio >= effective_threshold:
                        # Parse the entire column using the same successful method
                        try:
                            print(f"Applying successful format '{successful_format}' to entire column...")
                            
                            if len(sample_values) == len(test_sample):
                                # We already parsed the full sample
                                full_parsed = parsed_sample
                            else:
                                # Parse the full column using the same method that worked for the sample
                                full_parsed = pd.to_datetime(sample_values, errors='coerce', infer_datetime_format=True)
                                
                                # If infer didn't work well, try the specific format that worked
                                if full_parsed.notna().sum() / len(sample_values) < effective_threshold:
                                    for date_format in common_formats:
                                        try:
                                            format_parsed = pd.to_datetime(sample_values, format=date_format, errors='coerce')
                                            if format_parsed.notna().sum() / len(sample_values) >= effective_threshold:
                                                full_parsed = format_parsed
                                                break
                                        except:
                                            continue
                                    
                                    # If format-specific parsing failed, try flexible parsing on full column
                                    if full_parsed.notna().sum() / len(sample_values) < effective_threshold:
                                        try:
                                            flexible_parsed = pd.to_datetime(sample_values, errors='coerce', utc=False)
                                            if flexible_parsed.notna().sum() / len(sample_values) >= effective_threshold:
                                                full_parsed = flexible_parsed
                                                print(f"Used flexible parsing for full column '{col}'")
                                        except:
                                            pass
                            
                            # Apply the parsed dates to the original DataFrame
                            df.loc[df[col].notna(), col] = full_parsed
                            
                            # Convert the column to datetime
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                            
                            final_valid_ratio = df[col].notna().sum() / non_null_count
                            date_columns_converted.append({
                                'column': col,
                                'valid_ratio': final_valid_ratio,
                                'name_suggested_date': name_suggests_date,
                                'format_used': successful_format
                            })
                            
                            print(f"Successfully converted column '{col}' to datetime (final valid ratio: {final_valid_ratio:.2f}, format: {successful_format})")
                            
                        except Exception as e:
                            print(f"Failed to convert full column '{col}' to datetime: {str(e)}")
                    else:
                        print(f"Valid ratio {valid_ratio:.2f} below threshold {effective_threshold:.2f}")
                else:
                    print(f"No valid parsing method found for column '{col}'")
                            
            except Exception as e:
                print(f"Could not parse column '{col}' as date: {str(e)}")
                continue
    
    if date_columns_converted:
        print(f"\nSuccessfully converted {len(date_columns_converted)} columns to datetime:")
        
        # Log details for each converted column
        for item in date_columns_converted:
            print(f"  {item['column']}: {item['valid_ratio']:.1%} valid dates, format: {item['format_used']}")
    else:
        print("\nNo date columns detected or converted")
    
    return df

def detect_noisy_columns(df):
    """
    Automatically detect XML/noisy columns without hardcoding specific column names.
    
    Args:
        df: DataFrame to analyze
    Returns:
        list: Detected noisy columns
    """
    noisy_columns = []

    for col in df.columns:
        if df[col].dtype == object:  # Only check text columns
            # Convert to string in case we have non-string objects
            sample_values = df[col].astype(str).dropna().head(50)

            if len(sample_values) == 0:
                continue

            # Calculate metrics for this column
            avg_length = sample_values.str.len().mean()

            # Check for XML patterns (opening/closing tags or XML declarations)
            xml_pattern = re.compile(r'<[^>]+>|<\?xml')
            xml_matches = sum(sample_values.apply(lambda x: bool(xml_pattern.search(x))))
            xml_ratio = xml_matches / len(sample_values) if len(sample_values) > 0 else 0

            # Check for very long text
            is_long_text = avg_length > 500

            # Special character density (brackets, quotes, etc.)
            special_char_pattern = re.compile(r'[<>{}[\]\"\'=:]')
            special_char_density = sample_values.apply(
                lambda x: len(special_char_pattern.findall(x)) / len(x) if len(x) > 0 else 0
            ).mean()

            # Decision to flag based on combined metrics
            if (xml_ratio > 0.3 or              # Has XML content
                (is_long_text and special_char_density > 0.05) or  # Long text with many special chars
                avg_length > 1000):             # Extremely long text
                noisy_columns.append(col)

    print(f"Noisy columns: {noisy_columns}")
    return noisy_columns

def find_duplicate_columns(df):
    """
    Find columns with identical values using a smarter approach.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        tuple: (list of results, list of columns to drop)
    """
    # Get all columns to check
    columns = df.columns.tolist()
    
    if len(columns) < 2:
        return ["Less than 2 columns found"], []

    # Initialize groups
    groups = []
    processed_columns = set()

    # Process each column
    for col1 in columns:
        # Skip if we've already processed this column
        if col1 in processed_columns:
            continue

        # Start a new group with this column
        current_group = [col1]
        processed_columns.add(col1)

        # Compare with all other unprocessed columns
        for col2 in columns:
            if col2 in processed_columns:
                continue

            # Check if columns are equal (handling NaN values)
            try:
                is_equal = ((df[col1] == df[col2]) | 
                          (pd.isna(df[col1]) & pd.isna(df[col2]))).all()
                
                if is_equal:
                    current_group.append(col2)
                    processed_columns.add(col2)
            except:
                # Skip comparison if types are incompatible
                continue

        # Add this group to our results
        if len(current_group) > 1:  # Only add if we found duplicates
            groups.append(current_group)

    # Format results and decide which columns to drop
    results = []
    columns_to_drop = []
    
    for group in groups:
        if len(group) > 1:
            # Create a scoring system for column names
            scores = {}
            
            for col in group:
                col_lower = col.lower()
                score = 0
                
                # Prefer shorter names (often cleaner/simpler)
                score -= len(col) * 0.1
                
                # Prefer names without underscores or special characters
                score -= col.count('_') * 0.5
                score -= sum(c in '!@#$%^&*()+-={}[]|\\:;"\'<>,.?/' for c in col) * 1
                
                # Prefer common field names
                common_names = ['id', 'name', 'user', 'username', 'timestamp', 'time', 
                               'date', 'ip', 'address', 'email', 'status']
                               
                for name in common_names:
                    if name == col_lower or col_lower.endswith('_' + name) or col_lower.startswith(name + '_'):
                        score += 3
                    elif name in col_lower:
                        score += 1
                
                # Store the score
                scores[col] = score
            
            # Keep the column with the highest score
            keep_col = max(scores, key=scores.get)
            drop_cols = [col for col in group if col != keep_col]
            
            columns_to_drop.extend(drop_cols)
            results.append(f"Column '{keep_col}' equals to columns {drop_cols} - keeping '{keep_col}'")

    return results, columns_to_drop

def analyze_correlation(df, threshold=0.95):
    """
    Analyze correlation between numeric columns.
    
    Args:
        df (DataFrame): DataFrame to analyze
        threshold (float): Correlation threshold to report
        
    Returns:
        dict: Dictionary with correlation information and columns to drop
    """
    try:
        # Get only numeric columns
        numeric_df = df.select_dtypes(include=['number'])
        
        if numeric_df.shape[1] < 2:
            return {"error": "Not enough numeric columns for correlation analysis"}
            
        # Calculate correlation matrix
        corr_matrix = numeric_df.corr()
        
        # Find highly correlated pairs
        high_corr_pairs = []
        
        for i in range(len(corr_matrix.columns)):
            for j in range(i):
                if abs(corr_matrix.iloc[i, j]) >= threshold:
                    col1 = corr_matrix.columns[i]
                    col2 = corr_matrix.columns[j]
                    high_corr_pairs.append({
                        "column1": col1,
                        "column2": col2,
                        "correlation": corr_matrix.iloc[i, j]
                    })
        
        # Group highly correlated columns together
        correlated_groups = []
        
        # Start with all columns in separate groups
        remaining_cols = set(numeric_df.columns)
        
        while remaining_cols:
            # Start a new group with one column
            current_col = next(iter(remaining_cols))
            current_group = {current_col}
            remaining_cols.remove(current_col)
            
            # Flag to track if we found new connections
            expanded = True
            
            # Keep expanding the group as long as we find new connections
            while expanded:
                expanded = False
                
                # Find all columns correlated with any column in the current group
                for pair in high_corr_pairs:
                    col1, col2 = pair["column1"], pair["column2"]
                    
                    # If exactly one column is in current_group and one is in remaining_cols,
                    # add the one from remaining_cols to current_group
                    if col1 in current_group and col2 in remaining_cols:
                        current_group.add(col2)
                        remaining_cols.remove(col2)
                        expanded = True
                    elif col2 in current_group and col1 in remaining_cols:
                        current_group.add(col1)
                        remaining_cols.remove(col1)
                        expanded = True
            
            # Add the complete group to our list
            if len(current_group) > 1:  # Only add groups with at least 2 columns
                correlated_groups.append(current_group)
        
        # Determine which columns to drop from each group
        columns_to_drop = []
        
        for group in correlated_groups:
            # Convert group from set to list for consistent ordering
            group_list = list(group)
            
            # Keep the first column, drop the rest
            # This is arbitrary but consistent
            columns_to_drop.extend(group_list[1:])
        
        return {
            "numeric_columns": numeric_df.columns.tolist(),
            "high_correlation_pairs": high_corr_pairs,
            "correlated_groups": [list(group) for group in correlated_groups],
            "columns_to_drop": columns_to_drop
        }
        
    except Exception as e:
        return {"error": str(e)}

def preprocess_data(file_path):
    """
    MODIFIED: Preprocess data using the approach from the notebook with date detection, IP column detection, 
    and datetime separation.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        tuple: (DataFrame, dict, dict) - Cleaned and preprocessed DataFrame, IP detection results, and datetime separation results
    """
    try:
        # ---- MODIFIED SECTION: DETECT FILE TYPE AND READ ----
        print(f"Starting preprocessing for file: {file_path}")
        file_extension = os.path.splitext(file_path)[1].lower()
        df_original = None

        # Conditionally read the file based on its extension
        if file_extension in ['.xlsx', '.xls']:
            print("Detected Excel file. Reading the first sheet.")
            # By default, reads the first sheet of the Excel file
            df_original = pd.read_excel(file_path, sheet_name=0)
        
        elif file_extension in ['.csv', '.txt']:
            print("Detected text/csv file. Detecting delimiter.")
            # First, detect the delimiter by reading a few lines
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                sample = f.read(4096)  # Read first 4KB
            
            # Check for common delimiters in the sample
            delimiters = {',': sample.count(','), ';': sample.count(';'), '\t': sample.count('\t'), '|': sample.count('|')}
            
            # Choose the most frequent delimiter
            if any(delimiters.values()):
                delimiter = max(delimiters, key=delimiters.get)
            else:
                delimiter = ',' # Default to comma if no delimiters are found
            
            print(f"Detected delimiter: '{delimiter}'")
            df_original = pd.read_csv(file_path, sep=delimiter)
        
        else:
            raise ValueError(f"Unsupported file type: '{file_extension}'. Only Excel (.xlsx, .xls) and text (.csv, .txt) files are supported.")

        if df_original is None:
            raise ValueError("Failed to read the data file. It may be empty or corrupted.")
        df = df_original.copy()
        
        print(f"Original dataset shape: {df.shape}")

        # Remove completely empty rows (rows with all values missing or empty)
        # Convert empty strings to NaN first for consistent handling
        df = df.replace(r'^\s*$', np.nan, regex=True)

        
        # Identify rows where all values are NaN
        empty_rows = df.isna().all(axis=1)
        empty_row_count = empty_rows.sum()
        
        if empty_row_count > 0:
            print(f"Found {empty_row_count} completely empty rows")
            # Keep only non-empty rows
            df = df[~empty_rows].reset_index(drop=True)
            print(f"Removed {empty_row_count} empty rows. New shape: {df.shape}")
        else:
            print("No completely empty rows found")
        
        # Step 1: Deduplicate column names
        df.columns = deduplicate_columns(df.columns)
        print(f"Deduplicated columns: {list(df.columns)}")
        
        # Step 1.5: Detect IP columns BEFORE other processing
        print("\n" + "="*50)
        print("DETECTING IP COLUMNS...")
        print("="*50)
        ip_detection_result = detect_ip_columns(df)
        
        # Step 1.6: Process detected IP columns
        if ip_detection_result['ip_columns']:
            df = process_ip_columns(df, ip_detection_result)
        
        # Step 2: Detect and parse date columns
        print("\nDetecting and parsing date columns...")
        df = detect_and_parse_dates(df, threshold=0.7)
        
        # CAPTURE THE LIST OF DATE COLUMNS IMMEDIATELY AFTER DETECTION
        date_columns_detected = [col for col in df.columns if 'datetime64' in str(df[col].dtype)]
        
        if date_columns_detected:
            print(f"✅ Date columns captured: {date_columns_detected}")
        else:
            print("❌ No date columns were captured after parsing.")
        
        # Step 2.5: Separate datetime columns into date and time components
        print("\n" + "="*50)
        print("SEPARATING DATETIME COLUMNS...")
        print("="*50)
        df, datetime_separation_result = separate_datetime_columns(df)
        
        # Step 3: Drop columns with no variance
        dup_cols = [col for col in df.columns if df[col].nunique(dropna=False) == 1]
        if dup_cols:
            df = df.drop(columns=dup_cols)
            print(f"Dropped {len(dup_cols)} columns with no variance")
            print(f"Columns dropped due to no variance: {dup_cols}")
        
        # Step 4: Use analyze_correlation to identify and drop highly correlated columns
        correlation_info = analyze_correlation(df, threshold=0.95)
        
        if "columns_to_drop" in correlation_info and correlation_info["columns_to_drop"]:
            columns_to_drop = correlation_info["columns_to_drop"]
            df = df.drop(columns=columns_to_drop, errors='ignore')
            print(f"Dropped {len(columns_to_drop)} highly correlated columns")
            print(f"Columns dropped due to high correlation: {columns_to_drop}")
            
            # Print the correlation pairs for reference
            print("High correlation pairs:")
            for pair in correlation_info.get("high_correlation_pairs", []):
                print(f" - {pair['column1']} & {pair['column2']}: {pair['correlation']:.2f}")
            
            # Print the correlated groups for reference
            print("Correlated groups:")
            for i, group in enumerate(correlation_info.get("correlated_groups", [])):
                print(f" - Group {i+1}: {group} - kept {group[0]}")
        
        # Step 5: Find and drop duplicate columns automatically
        equality_results, duplicate_columns_to_drop = find_duplicate_columns(df)
        
        # Print the equality analysis results
        for result in equality_results:
            print(result)
        
        # Drop the identified duplicate columns
        if duplicate_columns_to_drop:
            df = df.drop(columns=duplicate_columns_to_drop, errors='ignore')
            print(f"Dropped {len(duplicate_columns_to_drop)} duplicate columns")
            print(f"Columns dropped due to duplication: {duplicate_columns_to_drop}")
        
        # Step 6: Drop noisy columns
        noisy_cols = detect_noisy_columns(df)
        if noisy_cols:
            df = df.drop(columns=noisy_cols)
            print(f"Dropped {len(noisy_cols)} noisy columns")
            print(f"Columns dropped due to noisy content: {noisy_cols}")
        
        # Step 7: Check for missing values
        missing_values = df.isnull().sum()
        print("Missing values in remaining columns:")
        print(missing_values[missing_values > 0])
        
        # Step 8: Check for duplicates
        duplicates = df.duplicated().sum()
        print(f"Number of duplicate rows: {duplicates}")
        
        # Final summary of column types
        print("\nFinal column types:")
        for col in df.columns:
            col_type = str(df[col].dtype)
            tags = []
            
            if col in ip_detection_result['ip_columns']:
                tags.append("IP COLUMN")
            
            # Check if this is an original datetime column
            if col in datetime_separation_result:
                tags.append("ORIGINAL DATETIME")
            
            # Check if this is a separated date column
            for original_col, info in datetime_separation_result.items():
                if col == info['date_column']:
                    tags.append("SEPARATED DATE")
                elif col == info['hour_column']:
                    tags.append("SEPARATED TIME")
            
            tag_str = f" ({', '.join(tags)})" if tags else ""
            print(f"  {col}: {col_type}{tag_str}")
        
        print(f"Final preprocessed dataset shape: {df.shape}")
        print(f"Final columns: {list(df.columns)}")
        
        return df, ip_detection_result, datetime_separation_result, date_columns_detected
        
    except Exception as e:
        print(f"Error preprocessing data: {e}")
        traceback.print_exc()
        return None, None, None, None