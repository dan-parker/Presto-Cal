"""
MicroPython Timezone Library

A lightweight timezone conversion library for MicroPython devices.
Supports timezone data loading from binary files and provides utilities
for converting between UTC and local time with DST support.

Usage:
    from timezone_lib import (
        load_timezone_data,
        convert_to_utc,
        convert_to_local,
        normalize_timezone_id,
    )

    #Import and load the blob
    load_timezone_data()

    #UTC offset in seconds (positive for east of UTC, negative for west)
    get_utc_offset("America/New_York", timestamp)
    
    #Localtime converted to UTC timestamp
    convert_to_local(Local time as (year, month, day, hour, min, sec),"America/New_York")

    #Returns: tuple: Local time as (year, month, day, hour, min, sec, weekday, yearday)
    convert_to_local(utc_timestamp, "America/New_York"):
   
    #Attempt lookup of text to matched iana timezone_id in blob
    normalize_timezone_id("Pacific Standard Time (Mexico)")


Author: Dan Parker
License: GPL v3
Version: 1.0.0
"""

import struct
import utime
import gc

__version__ = "1.0.0"
__author__ = "Dan Parker"
__license__ = "GPLv3"

# === MODULE CONSTANTS ===
_TIMEZONE_STRUCT_FORMAT = "<iiBBBBBB"
_DEFAULT_LOAD_TIMEOUT = 10
_MAX_TIMEZONE_NAME_LENGTH = 100
_TIMEOUT_CHECK_INTERVAL = 50

# === MODULE STATE ===
_tzid_table = None
_tz_cache = {}
_timezone_loading = False

def load_timezone_data(blob_path="tzid_blob.bin", timeout=_DEFAULT_LOAD_TIMEOUT):
    """
    Load timezone data from binary file and cache it in memory.
    
    Args:
        blob_path (str): Path to timezone binary data file
        timeout (int): Maximum time in seconds to spend loading data
    
    Returns:
        dict: Dictionary mapping timezone IDs to timezone data tuples
        
    Note:
        Returns cached data if already loaded. Returns empty dict if
        another loading operation is in progress.
    """
    global _tzid_table, _timezone_loading
    
    if _tzid_table is not None:
        return _tzid_table
    
    if _timezone_loading:
        return {}
    
    _timezone_loading = True
    _tzid_table = {}
    
    try:
        start_time = utime.time()
        
        with open(blob_path, "rb") as f:
            entry_count = 0
            
            while True:
                # Check timeout periodically to avoid blocking
                if entry_count % _TIMEOUT_CHECK_INTERVAL == 0:
                    if (utime.time() - start_time) > timeout:
                        print("Timezone loading timeout, using partial data")
                        break
                
                # Read timezone name length
                name_len_bytes = f.read(1)
                if not name_len_bytes:
                    break
                    
                name_len = name_len_bytes[0]
                if name_len == 0 or name_len > _MAX_TIMEZONE_NAME_LENGTH:
                    break
                
                # Read timezone name and data in single operation
                data_chunk = f.read(name_len + 14)
                if len(data_chunk) < name_len + 14:
                    break

                name_bytes = data_chunk[:name_len]
                timezone_data = data_chunk[name_len:]
                
                try:
                    # Unpack timezone data: std_offset, dst_offset, start/end info
                    std_offset, dst_offset, start_month, start_day, start_hour, \
                    end_month, end_day, end_hour = struct.unpack(_TIMEZONE_STRUCT_FORMAT, timezone_data)
                    
                    timezone_id = name_bytes.decode("utf-8")
                    
                    # Store DST transition info (None if no DST)
                    dst_start = (start_month, start_day, start_hour) if start_month else None
                    dst_end = (end_month, end_day, end_hour) if end_month else None
                    
                    _tzid_table[timezone_id] = (std_offset, dst_offset, dst_start, dst_end)
                    entry_count += 1
                    
                except (struct.error, UnicodeDecodeError):
                    # Skip malformed entries
                    continue
                    
        # Clean up memory after loading large datasets
        if len(_tzid_table) > 100:
            gc.collect()
                    
    except OSError as e:
        print(f"Timezone file error: {e}")
    except MemoryError as e:
        print(f"Memory error loading timezones: {e}")
        _tzid_table.clear()
        gc.collect()
    except Exception as e:
        print(f"Unexpected timezone loading error: {e}")
    finally:
        _timezone_loading = False
        
    print(f"Loaded {len(_tzid_table)} timezones")
    return _tzid_table

def get_utc_offset(timezone_id, timestamp):
    """
    Calculate UTC offset for given timezone at specific timestamp.
    
    Args:
        timezone_id (str): Timezone identifier (e.g., "America/New_York")
        timestamp (int): Unix timestamp
    
    Returns:
        int: UTC offset in seconds (positive for east of UTC, negative for west)
    
    Note:
        Accounts for Daylight Saving Time transitions. Returns 0 for
        unknown timezones or on calculation errors.
    """
    timezone_table = load_timezone_data()
    if not timezone_table:
        return 0
    
    timezone_info = timezone_table.get(timezone_id)
    if not timezone_info:
        return 0

    try:
        std_offset, dst_offset, dst_start, dst_end = timezone_info
        
        # No DST transitions defined
        if not dst_start or not dst_end:
            return std_offset

        # Get year from timestamp for DST calculations
        local_time = utime.localtime(timestamp + std_offset)
        year = local_time[0]

        def calculate_transition_timestamp(month, day, hour):
            """Calculate UTC timestamp for DST transition."""
            transition_tuple = (year, month, day, hour, 0, 0, 0, 0)
            return utime.mktime(transition_tuple) - std_offset

        dst_start_timestamp = calculate_transition_timestamp(*dst_start)
        dst_end_timestamp = calculate_transition_timestamp(*dst_end)

        # Determine if timestamp falls within DST period
        if dst_start_timestamp < dst_end_timestamp:
            # DST period within same year (e.g., March to October)
            in_dst_period = dst_start_timestamp <= timestamp < dst_end_timestamp
        else:
            # DST period crosses year boundary (e.g., October to March)
            in_dst_period = timestamp >= dst_start_timestamp or timestamp < dst_end_timestamp

        return dst_offset if in_dst_period else std_offset
        
    except Exception:
        # Return standard offset on any calculation error
        return 0

def convert_to_utc(local_time_tuple, timezone_id):
    """
    Convert local time tuple to UTC timestamp.
    
    Args:
        local_time_tuple (tuple): Local time as (year, month, day, hour, min, sec)
        timezone_id (str): Timezone identifier
    
    Returns:
        int: UTC timestamp
    
    Note:
        Handles incomplete tuples by padding with zeros.
    """
    try:
        # Ensure tuple has required elements
        if len(local_time_tuple) < 8:
            local_time_tuple = local_time_tuple + (0, 0)
            
        local_timestamp = utime.mktime(local_time_tuple)
        utc_offset = get_utc_offset(timezone_id, local_timestamp)
        return local_timestamp - utc_offset
        
    except Exception:
        # Fallback: create timestamp from available elements
        padded_tuple = local_time_tuple[:6] + (0, 0)
        return utime.mktime(padded_tuple)

def convert_to_local(utc_timestamp, timezone_id):
    """
    Convert UTC timestamp to local time tuple.
    
    Args:
        utc_timestamp (int): UTC timestamp
        timezone_id (str): Timezone identifier
    
    Returns:
        tuple: Local time as (year, month, day, hour, min, sec, weekday, yearday)
    
    Note:
        Returns UTC time on conversion errors.
    """
    try:
        utc_offset = get_utc_offset(timezone_id, utc_timestamp)
        local_timestamp = utc_timestamp + utc_offset
        return utime.localtime(local_timestamp)
    except Exception:
        return utime.localtime(utc_timestamp)

def normalize_timezone_id(raw_timezone_id):
    """
    Normalize and validate timezone identifier.
    
    Args:
        raw_timezone_id (str): Raw timezone identifier string
    
    Returns:
        str: Normalized timezone identifier or "UTC" if invalid
    
    Note:
        Performs case-insensitive matching against loaded timezone data.
        Results are cached for subsequent lookups.
    """
    if not raw_timezone_id:
        return "UTC"
        
    # Check cache first
    cached_result = _tz_cache.get(raw_timezone_id)
    if cached_result:
        return cached_result

    try:
        # Clean up timezone ID string
        cleaned_id = raw_timezone_id.strip().rstrip("/")
        if "\\" in cleaned_id or '"' in cleaned_id:
            cleaned_id = cleaned_id.replace("\\", "").replace('"', "")
            
        timezone_table = load_timezone_data()
        if not timezone_table:
            _tz_cache[raw_timezone_id] = cleaned_id
            return cleaned_id
        
        # Try exact match first
        if cleaned_id in timezone_table:
            result = cleaned_id
        else:
            # Try case-insensitive matching
            cleaned_lower = cleaned_id.lower()
            result = cleaned_id  # Default fallback
            
            for timezone_key in timezone_table:
                key_lower = timezone_key.lower()
                if key_lower == cleaned_lower or key_lower.endswith("/" + cleaned_lower):
                    result = timezone_key
                    break

        _tz_cache[raw_timezone_id] = result
        return result
        
    except Exception:
        _tz_cache[raw_timezone_id] = "UTC"
        return "UTC"

def clear_timezone_cache():
    """
    Clear the timezone ID normalization cache and trigger garbage collection.
    """
    global _tz_cache
    _tz_cache.clear()
    gc.collect()

def clear_timezone_data():
    """
    Clear loaded timezone data and trigger garbage collection.
    """
    global _tzid_table
    if _tzid_table:
        _tzid_table.clear()
    _tzid_table = None
    gc.collect()

def get_library_info():
    """
    Get information about library state and memory usage.
    
    Returns:
        dict: Dictionary containing library statistics
    """
    try:
        info = {
            'version': __version__,
            'timezones_loaded': len(_tzid_table) if _tzid_table else 0,
            'cache_size': len(_tz_cache),
            'loading_in_progress': _timezone_loading
        }
        
        # Add memory info if available
        if hasattr(gc, 'mem_free'):
            info['free_memory_bytes'] = gc.mem_free()
            
        return info
    except Exception:
        return {'error': 'Library info unavailable'}
