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
    
    compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in ip_name_patterns]
    
    print("=== IP COLUMN DETECTION ===")
    
    for col in df.columns:
        col_lower = col.lower()
        is_ip_column = False
        matched_patterns = []
        for i, pattern in enumerate(compiled_patterns):
            if pattern.search(col_lower):
                is_ip_column = True
                matched_patterns.append(ip_name_patterns[i])
        
        content_suggests_ip = False
        ip_ratio = 0.0
        
        if df[col].dtype == 'object':  # Only check string/object columns
            sample_values = df[col].dropna().astype(str).head(100)
            
            if len(sample_values) > 0:
                ipv4_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
                ipv6_pattern = r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::1$|^::$'
                ipv4_matches = sample_values.str.match(ipv4_pattern).sum()
                ipv6_matches = sample_values.str.match(ipv6_pattern).sum()
                total_ip_matches = ipv4_matches + ipv6_matches
                
                ip_ratio = total_ip_matches / len(sample_values)

                if ip_ratio > 0.5:
                    content_suggests_ip = True
        
        if is_ip_column or content_suggests_ip:
            ip_columns.append(col)
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
            print(f"Processing IP column: '{col}'")
            df[col] = df[col].astype(str).str.strip()
            invalid_values = ['', 'null', 'none', 'n/a', 'unknown', '0.0.0.0', 'nan']
            df[col] = df[col].replace(invalid_values, pd.NA, regex=False)
            ipv4_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
            ipv6_pattern = r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::1$|^::$'
            valid_ipv4 = df[col].str.match(ipv4_pattern, na=False)
            valid_ipv6 = df[col].str.match(ipv6_pattern, na=False)
            valid_ip = valid_ipv4 | valid_ipv6
            total_non_null = df[col].notna().sum()
            valid_count = valid_ip.sum()
            invalid_count = total_non_null - valid_count
            
            print(f"   - Total non-null values: {total_non_null}")
            print(f"   - Valid IP addresses: {valid_count}")
            print(f"   - Invalid IP addresses: {invalid_count}")
            
            if invalid_count > 0:
                invalid_ips = df.loc[~valid_ip & df[col].notna(), col].unique()[:5]  # Show first 5 invalid IPs
                print(f"   - Sample invalid IPs: {list(invalid_ips)}")
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
            non_null_count = df[col].notna().sum()
            if non_null_count < len(df) * 0.1:  # Less than 10% non-null
                continue
            sample_values = df[col].dropna().astype(str)
            if len(sample_values) == 0:
                continue
            col_lower = col.lower()
            date_indicators = ['date', 'time', 'timestamp', 'created', 'updated', 'modified', 
                             'start', 'end', 'birth', 'dob', 'expire', 'due', 'schedule', '_time']
            name_suggests_date = (
                any(indicator in col_lower for indicator in date_indicators) or
                col_lower.startswith('_time') or 
                col_lower.endswith('_time') or
                col_lower == '_time'
            )
            print(f"\n=== Analyzing column '{col}' ===")
            print(f"Sample values: {sample_values.head(3).tolist()}")
            print(f"Column name suggests date: {name_suggests_date}")

            try:
                test_sample = sample_values.head(min(1000, len(sample_values)))
                parsed_sample = None
                successful_format = None
                
                try:
                    parsed_sample = pd.to_datetime(test_sample, errors='coerce', infer_datetime_format=True)
                except:
                    pass
                if parsed_sample is None or parsed_sample.notna().sum() < len(test_sample) * threshold:
                    common_formats = [
                        '%Y-%m-%d',           # 2024-01-15
                        '%m/%d/%Y',           # 01/15/2024
                        '%d/%m/%Y',           # 15/01/2024
                        '%d-%m-%Y',           # 15-01-2024
                        '%Y%m%d',             # 20240115
                        '%d-%b-%Y',           # 15-Jan-2024
                        '%b %d, %Y',          # Jan 15, 2024
                        '%Y-%m-%d %H:%M:%S',  # 2024-01-15 10:30:00
                        '%m/%d/%Y %H:%M:%S',  # 01/15/2024 10:30:00
                        '%d/%m/%Y %H:%M:%S',  # 15/01/2024 10:30:00
                        '%d %m %Y %H:%M',     # 22 11 2024 00:00
                        '%Y-%m-%dT%H:%M:%S.%f%z',  # 2024-11-22T00:00:02.000000+0700 (6 digits)
                        '%Y-%m-%dT%H:%M:%S%z',     # 2024-11-22T00:00:02+0700
                        '%Y-%m-%dT%H:%M:%S.%fZ',   # 2024-01-15T10:30:00.123456Z
                        '%Y-%m-%dT%H:%M:%S.%f',    # 2024-01-15T10:30:00.123456
                        '%Y-%m-%dT%H:%M:%SZ',      # 2024-01-15T10:30:00Z
                        '%Y-%m-%dT%H:%M:%S',       # 2024-01-15T10:30:00
                        '%Y-%m-%d %H:%M:%S.%f',   # 2024-01-15 10:30:00.123456
                        '%m/%d/%Y %H:%M:%S.%f',   # 01/15/2024 10:30:00.123456
                    ]
                    
                    for date_format in common_formats:
                        try:
                            print(f"  Trying format: {date_format}")
                            
                            if '.%f' in date_format and any('.000' in str(val) for val in test_sample.head(3)):
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
                if parsed_sample is None:
                    print("Trying manual preprocessing for timezone issues...")
                    try:
                        preprocessed_sample = test_sample.str.replace(
                            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{3})(\+\d{4})', 
                            r'\1.\2000\3', 
                            regex=True
                        )
                        
                        print(f"  Preprocessed sample: {preprocessed_sample.head(3).tolist()}")
                        format_parsed = pd.to_datetime(preprocessed_sample, format='%Y-%m-%dT%H:%M:%S.%f%z', errors='coerce')
                        valid_ratio = format_parsed.notna().sum() / len(test_sample)
                        print(f"  Preprocessed valid ratio: {valid_ratio:.2f}")
                        
                        if valid_ratio >= threshold:
                            parsed_sample = format_parsed
                            successful_format = "manual_preprocessing"
                            print(f"Column '{col}' parsed using manual preprocessing")
                        
                    except Exception as e:
                        print(f"  Manual preprocessing failed: {e}")

                if parsed_sample is None:
                    print("Trying flexible pandas parsing as final fallback...")
                    try:
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
                    effective_threshold = threshold * 0.5 if name_suggests_date else threshold
                    print(f"Effective threshold: {effective_threshold:.2f}, Valid ratio: {valid_ratio:.2f}")
                    
                    if valid_ratio >= effective_threshold:
                        try:
                            print(f"Applying successful format '{successful_format}' to entire column...")
                            
                            if len(sample_values) == len(test_sample):
                                full_parsed = parsed_sample
                            else:
                                full_parsed = pd.to_datetime(sample_values, errors='coerce', infer_datetime_format=True)
                                if full_parsed.notna().sum() / len(sample_values) < effective_threshold:
                                    for date_format in common_formats:
                                        try:
                                            format_parsed = pd.to_datetime(sample_values, format=date_format, errors='coerce')
                                            if format_parsed.notna().sum() / len(sample_values) >= effective_threshold:
                                                full_parsed = format_parsed
                                                break
                                        except:
                                            continue
                                    if full_parsed.notna().sum() / len(sample_values) < effective_threshold:
                                        try:
                                            flexible_parsed = pd.to_datetime(sample_values, errors='coerce', utc=False)
                                            if flexible_parsed.notna().sum() / len(sample_values) >= effective_threshold:
                                                full_parsed = flexible_parsed
                                                print(f"Used flexible parsing for full column '{col}'")
                                        except:
                                            pass

                            df.loc[df[col].notna(), col] = full_parsed
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
        if df[col].dtype == object:  
            sample_values = df[col].astype(str).dropna().head(50)

            if len(sample_values) == 0:
                continue
            avg_length = sample_values.str.len().mean()
            xml_pattern = re.compile(r'<[^>]+>|<\?xml')
            xml_matches = sum(sample_values.apply(lambda x: bool(xml_pattern.search(x))))
            xml_ratio = xml_matches / len(sample_values) if len(sample_values) > 0 else 0
            is_long_text = avg_length > 500
            special_char_pattern = re.compile(r'[<>{}[\]\"\'=:]')
            special_char_density = sample_values.apply(
                lambda x: len(special_char_pattern.findall(x)) / len(x) if len(x) > 0 else 0
            ).mean()

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
    columns = df.columns.tolist()
    
    if len(columns) < 2:
        return ["Less than 2 columns found"], []
    groups = []
    processed_columns = set()

    for col1 in columns:
        if col1 in processed_columns:
            continue
        current_group = [col1]
        processed_columns.add(col1)
        for col2 in columns:
            if col2 in processed_columns:
                continue
            try:
                is_equal = ((df[col1] == df[col2]) | 
                          (pd.isna(df[col1]) & pd.isna(df[col2]))).all()
                
                if is_equal:
                    current_group.append(col2)
                    processed_columns.add(col2)
            except:
                continue

        if len(current_group) > 1:  
            groups.append(current_group)

    results = []
    columns_to_drop = []
    
    for group in groups:
        if len(group) > 1:
            scores = {}
            
            for col in group:
                col_lower = col.lower()
                score = 0
                score -= len(col) * 0.1
                score -= col.count('_') * 0.5
                score -= sum(c in '!@#$%^&*()+-={}[]|\\:;"\'<>,.?/' for c in col) * 1
                common_names = ['id', 'name', 'user', 'username', 'timestamp', 'time', 
                               'date', 'ip', 'address', 'email', 'status']
                               
                for name in common_names:
                    if name == col_lower or col_lower.endswith('_' + name) or col_lower.startswith(name + '_'):
                        score += 3
                    elif name in col_lower:
                        score += 1
                
                scores[col] = score
            
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
        numeric_df = df.select_dtypes(include=['number'])
        
        if numeric_df.shape[1] < 2:
            return {"error": "Not enough numeric columns for correlation analysis"}
        corr_matrix = numeric_df.corr()
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

        correlated_groups = []
        remaining_cols = set(numeric_df.columns)
        
        while remaining_cols:
            current_col = next(iter(remaining_cols))
            current_group = {current_col}
            remaining_cols.remove(current_col)
            expanded = True

            while expanded:
                expanded = False
                for pair in high_corr_pairs:
                    col1, col2 = pair["column1"], pair["column2"]
                    if col1 in current_group and col2 in remaining_cols:
                        current_group.add(col2)
                        remaining_cols.remove(col2)
                        expanded = True
                    elif col2 in current_group and col1 in remaining_cols:
                        current_group.add(col1)
                        remaining_cols.remove(col1)
                        expanded = True
            
            if len(current_group) > 1: 
                correlated_groups.append(current_group)

        columns_to_drop = []
        
        for group in correlated_groups:
            group_list = list(group)
            columns_to_drop.extend(group_list[1:])
        
        return {
            "numeric_columns": numeric_df.columns.tolist(),
            "high_correlation_pairs": high_corr_pairs,
            "correlated_groups": [list(group) for group in correlated_groups],
            "columns_to_drop": columns_to_drop
        }
        
    except Exception as e:
        return {"error": str(e)}

def preprocess_dataframe(df):
    """
    Performs all preprocessing steps on a DataFrame that is already in memory.
    This is the new core preprocessing logic.
    """
    try:
        print(f"Starting preprocessing on in-memory DataFrame with shape: {df.shape}")
        
        # Remove completely empty rows
        df = df.replace(r'^\s*$', np.nan, regex=True).dropna(how='all').reset_index(drop=True)
        print(f"Shape after removing empty rows: {df.shape}")

        # Step 1: Deduplicate column names
        df.columns = deduplicate_columns(df.columns)
        
        # Step 2: Detect and process IP columns
        ip_detection_result = detect_ip_columns(df)
        if ip_detection_result.get('ip_columns'):
            df = process_ip_columns(df, ip_detection_result)
            
        # Step 3: Detect and parse date columns
        df = detect_and_parse_dates(df, threshold=0.7)
        date_columns_detected = [col for col in df.columns if 'datetime64' in str(df[col].dtype)]

        # Step 4: Separate datetime columns
        df, datetime_separation_result = separate_datetime_columns(df)
        
        # Step 5: Drop columns with no variance
        no_variance_cols = [col for col in df.columns if df[col].nunique(dropna=False) == 1]
        if no_variance_cols:
            df = df.drop(columns=no_variance_cols)
            print(f"Dropped {len(no_variance_cols)} columns with no variance: {no_variance_cols}")
            
        # Step 6: Analyze and drop highly correlated columns
        correlation_info = analyze_correlation(df, threshold=0.95)
        if "columns_to_drop" in correlation_info and correlation_info["columns_to_drop"]:
            df = df.drop(columns=correlation_info["columns_to_drop"], errors='ignore')
            print(f"Dropped {len(correlation_info['columns_to_drop'])} highly correlated columns.")

        # Step 7: Find and drop duplicate columns
        _, duplicate_columns_to_drop = find_duplicate_columns(df)
        if duplicate_columns_to_drop:
            df = df.drop(columns=duplicate_columns_to_drop, errors='ignore')
            print(f"Dropped {len(duplicate_columns_to_drop)} duplicate columns.")

        # Step 8: Drop noisy columns
        noisy_cols = detect_noisy_columns(df)
        if noisy_cols:
            df = df.drop(columns=noisy_cols)
            print(f"Dropped {len(noisy_cols)} noisy columns.")

        print(f"Final preprocessed DataFrame shape: {df.shape}")
        return df, ip_detection_result, datetime_separation_result, date_columns_detected

    except Exception as e:
        print(f"Error during DataFrame preprocessing: {e}")
        traceback.print_exc()
        return None, None, None, None

def preprocess_data(file_path):
    """
    MODIFIED: Reads a file and passes its DataFrame to the core preprocessing function.
    Maintains compatibility with the /load endpoint.
    """
    try:
        print(f"Reading file for preprocessing: {file_path}")
        file_extension = os.path.splitext(file_path)[1].lower()
        df_original = None

        if file_extension in ['.xlsx', '.xls']:
            df_original = pd.read_excel(file_path, sheet_name=0)
        elif file_extension in ['.csv', '.txt']:
            # Try different encodings and handle BOM
            encodings = ['utf-8-sig', 'utf-8', 'latin1', 'iso-8859-1', 'cp1252']
            detected_delimiter = None
            last_error = None
            
            # First try to detect delimiter with a small sample
            try:
                with open(file_path, 'rb') as f:
                    raw_sample = f.read(4096)
                    # Try to decode with various encodings just to count delimiters
                    for encoding in encodings:
                        try:
                            sample = raw_sample.decode(encoding)
                            delimiters = {',': sample.count(','), ';': sample.count(';'), 
                                        '\t': sample.count('\t'), '|': sample.count('|')}
                            detected_delimiter = max(delimiters, key=delimiters.get) if any(delimiters.values()) else ','
                            break
                        except UnicodeDecodeError:
                            continue
            except Exception as e:
                print(f"Warning: Could not detect delimiter from sample: {e}")
                detected_delimiter = ','

            # Try reading with each encoding
            for encoding in encodings:
                try:
                    print(f"Attempting to read with encoding: {encoding}, delimiter: {repr(detected_delimiter)}")
                    df_original = pd.read_csv(file_path, 
                                           sep=detected_delimiter, 
                                           encoding=encoding,
                                           engine='python',  # More flexible but slower engine
                                           on_bad_lines='warn')  # Don't fail on bad lines
                    print(f"Successfully read file with encoding: {encoding}")
                    break
                except UnicodeDecodeError as e:
                    last_error = e
                    print(f"Failed with encoding {encoding}: {str(e)}")
                    continue
                except Exception as e:
                    last_error = e
                    print(f"Unexpected error with encoding {encoding}: {str(e)}")
                    continue
            
            if df_original is None:
                raise ValueError(f"Could not read file with any encoding. Last error: {last_error}")
        else:
            raise ValueError(f"Unsupported file type: '{file_extension}'")

        if df_original is None:
            raise ValueError("Failed to read the data file.")
        return preprocess_dataframe(df_original.copy())

    except Exception as e:
        print(f"Error reading or preprocessing file: {e}")
        traceback.print_exc()
        return None, None, None, None