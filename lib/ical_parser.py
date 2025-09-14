"""
ICS Calendar Parser for MicroPython

A memory-efficient iCalendar (.ics) parser designed for MicroPython environments.
Supports recurring events, timezones, and HTTP(S) calendar feeds with caching.

Features:
- Parse iCalendar files from HTTP(S) URLs
- Support for recurring events (RRULE) with DAILY, WEEKLY, MONTHLY, YEARLY frequencies
- Timezone handling via timezone_lib
- Memory-efficient parsing with configurable limits
- HTTP caching with ETag/Last-Modified support
- Exception dates (EXDATE) and additional dates (RDATE) support

Usage:
    from ics_parser import get_events, clear_cache
    
    # Get events from a calendar URL
    events = get_events("https://calendar.example.com/calendar.ics", max_events=20)
    
    for event in events:
        print(f"{event.summary}: {event.dtstart} - {event.dtend}")

Author: Dan Parker
License: GPL v3
Version: 1.0.1
"""

import urequests
import gc
import utime
from timezone_lib import (
    load_timezone_data,
    convert_to_utc,
    convert_to_local,
    normalize_timezone_id,
)

# === CONFIGURATION ===
DEFAULT_MAX_EVENTS = 40
DEFAULT_HTTP_TIMEOUT = 60
DEFAULT_MAX_RECURRENCE_ITERATIONS = 200
DEFAULT_MAX_OCCURRENCES_PER_EVENT = 50
DEFAULT_CACHE_VALIDITY_SECONDS = 300  # 5 minutes
DEFAULT_MAX_DESCRIPTION_LENGTH = 200
DEFAULT_MAX_RDATE_COUNT = 20

# Configuration globals (can be modified via set_limits())
MAX_RECURRENCE_ITERATIONS = DEFAULT_MAX_RECURRENCE_ITERATIONS
MAX_OCCURRENCES_PER_EVENT = DEFAULT_MAX_OCCURRENCES_PER_EVENT
CACHE_VALIDITY_SECONDS = DEFAULT_CACHE_VALIDITY_SECONDS
MAX_DESCRIPTION_LENGTH = DEFAULT_MAX_DESCRIPTION_LENGTH
MAX_RDATE_COUNT = DEFAULT_MAX_RDATE_COUNT
HTTP_TIMEOUT = DEFAULT_HTTP_TIMEOUT

# === CONSTANTS ===
_DAY_MAP = {'MO': 0, 'TU': 1, 'WE': 2, 'TH': 3, 'FR': 4, 'SA': 5, 'SU': 6}
_SECONDS_PER_DAY = 86400
_SECONDS_PER_WEEK = 604800

# === GLOBAL STATE ===
load_timezone_data()
_parsing_cache = {}
_in_event_state = False
_current_raw_event_state = None
_raw_events_data_list = []

# === EVENT DATA STRUCTURE ===
class Event:
    """Represents a calendar event with start/end times and metadata."""
    
    __slots__ = ('dtstart', 'dtend', 'summary', 'description', 'tzid')

    def __init__(self, dtstart, dtend, summary="", description="", tzid="UTC"):
        """
        Initialize a calendar event.
        
        Args:
            dtstart: Start timestamp (Unix timestamp)
            dtend: End timestamp (Unix timestamp)
            summary: Event title/summary
            description: Event description
            tzid: Timezone identifier
        """
        self.dtstart = dtstart
        self.dtend = dtend
        self.summary = summary if summary is not None else ""
        self.description = description if description is not None else ""
        self.tzid = tzid if tzid is not None else "UTC"

    def __lt__(self, other):
        if not isinstance(other, Event):
            return NotImplemented
        return self.dtstart < other.dtstart

    def __eq__(self, other):
        if not isinstance(other, Event):
            return NotImplemented
        return (self.dtstart == other.dtstart and
                self.dtend == other.dtend and
                self.summary == other.summary and
                self.description == other.description and
                self.tzid == other.tzid)

    def __hash__(self):
        return hash((self.dtstart, self.dtend, self.summary, self.description, self.tzid))
    
    def __repr__(self):
        start_local = utime.localtime(self.dtstart)
        end_local = utime.localtime(self.dtend)
        
        return (f"Event(dtstart={self.dtstart} ({start_local[0]}-{start_local[1]:02d}-{start_local[2]:02d} "
                f"{start_local[3]:02d}:{start_local[4]:02d}:{start_local[5]:02d}), "
                f"dtend={self.dtend} ({end_local[0]}-{end_local[1]:02d}-{end_local[2]:02d} "
                f"{end_local[3]:02d}:{end_local[4]:02d}:{end_local[5]:02d}), "
                f"summary='{self.summary}', "
                f"description='{self.description[:50]}...', "
                f"tzid='{self.tzid}')")

# === DATETIME PARSING ===
def parse_datetime(line):
    """
    Parse iCalendar datetime string to Unix timestamp.
    
    Supports both DATE and DATE-TIME formats with timezone information.
    
    Args:
        line: iCalendar property line (e.g., "DTSTART;TZID=America/New_York:20231201T120000")
        
    Returns:
        Unix timestamp or None if parsing fails
    """
    if ':' not in line:
        return None
        
    tzid = "UTC"
    tzid_pos = line.find("TZID=")
    if tzid_pos != -1:
        tz_start = tzid_pos + 5
        tz_end = line.find(":", tz_start)
        if tz_end != -1:
            tzid = normalize_timezone_id(line[tz_start:tz_end])
    
    colon_pos = line.rfind(':')
    dt_val = line[colon_pos + 1:].strip()
    
    # Handle UTC indicator and timezone offsets
    is_zulu = dt_val.endswith('Z')
    if is_zulu or dt_val.find('+') != -1 or dt_val.find('-') != -1:
        for suffix in ['Z', '+', '-']:
            pos = dt_val.find(suffix)
            if pos != -1:
                dt_val = dt_val[:pos]
                break
    
    try:
        if 'T' in dt_val:
            # DATE-TIME format
            if len(dt_val) < 8:
                return None
            year = int(dt_val[0:4])
            month = int(dt_val[4:6])
            day = int(dt_val[6:8])
            
            if len(dt_val) > 9:
                hour = int(dt_val[9:11]) if len(dt_val) >= 11 else 0
                minute = int(dt_val[11:13]) if len(dt_val) >= 13 else 0
                second = int(dt_val[13:15]) if len(dt_val) >= 15 else 0
            else:
                hour = minute = second = 0
        else:
            # DATE format
            if len(dt_val) < 8:
                return None
            year = int(dt_val[0:4])
            month = int(dt_val[4:6])
            day = int(dt_val[6:8])
            hour = minute = second = 0
    except (ValueError, IndexError):
        return None
    
    tpl = (year, month, day, hour, minute, second)
    
    if is_zulu or tzid == "UTC":
        return utime.mktime(tpl + (0, 0))
    
    return convert_to_utc(tpl, tzid)

def parse_rrule(rrule_str):
    """
    Parse RRULE string into dictionary.
    
    Args:
        rrule_str: RRULE value (e.g., "FREQ=DAILY;INTERVAL=1;COUNT=10")
        
    Returns:
        Dictionary with parsed RRULE components
    """
    rule = {}
    for part in rrule_str.split(";"):
        if "=" in part:
            key, val = part.split("=", 1)
            rule[key] = val
    return rule

def parse_date_list(line, current, field_name):
    """
    Parse comma-separated date list (EXDATE/RDATE) and add to current event data.
    
    Args:
        line: iCalendar property line
        current: Current event data dictionary
        field_name: Field name to store parsed dates ('exdates' or 'rdates')
    """
    colon_pos = line.find(':')
    if colon_pos == -1:
        return
        
    date_str = line[colon_pos + 1:]
    tzid = current.get('tzid', 'UTC')
    
    for date_part in date_str.split(','):
        date_part = date_part.strip()
        if date_part:
            dt_line = f"{line[:colon_pos]};TZID={tzid}:{date_part}"
            ts = parse_datetime(dt_line)
            if ts is not None:
                current[field_name].append(ts)

# === TEXT PROCESSING ===
def clean_description(raw):
    """
    Clean and truncate event description text.
    
    Args:
        raw: Raw description text from iCalendar
        
    Returns:
        Cleaned and truncated description string
    """
    if not raw:
        return ""
        
    # Unescape common iCalendar escape sequences
    text = raw.replace("\\n", "\n").replace("\\,", ",").replace("\\;", ";")
    
    # Truncate if too long
    if len(text) > MAX_DESCRIPTION_LENGTH:
        text = text[:MAX_DESCRIPTION_LENGTH-1] + "…"
    
    return text.strip()

# === RECURRENCE UTILITIES ===
def matches_recurrence_rules(timestamp, tzid, byday=None, bymonth=None, bymonthday=None):
    """
    Check if timestamp matches RRULE constraints (BYDAY, BYMONTH, BYMONTHDAY).
    
    Args:
        timestamp: Unix timestamp to check
        tzid: Timezone for conversion
        byday: List of weekdays (0=Monday, 6=Sunday)
        bymonth: List of months (1-12)
        bymonthday: List of days of month (1-31)
        
    Returns:
        True if timestamp matches all specified constraints
    """
    if not byday and not bymonth and not bymonthday:
        return True  # No constraints to check
    
    try:
        local_time = convert_to_local(timestamp, tzid)
        year, month, day, hour, minute, second, weekday, yearday = local_time
        
        # Check constraints in order of most restrictive first
        if bymonthday and day not in bymonthday:
            return False
            
        if bymonth and month not in bymonth:
            return False
            
        if byday and weekday not in byday:
            return False
                
        return True
    except:
        return False

def calculate_skip_count(base_ts, search_start_ts, freq, interval):
    """
    Calculate how many recurrence intervals to skip to approach search_start_ts.
    
    This helps avoid iterating through many occurrences when searching for
    events far in the future.
    
    Args:
        base_ts: Base timestamp of recurring event
        search_start_ts: Target timestamp to approach
        freq: Recurrence frequency (DAILY, WEEKLY, etc.)
        interval: Recurrence interval
        
    Returns:
        Number of intervals to skip
    """
    if base_ts >= search_start_ts:
        return 0
        
    time_diff = search_start_ts - base_ts
    
    if freq == "DAILY":
        interval_seconds = _SECONDS_PER_DAY * interval
    elif freq == "WEEKLY":
        interval_seconds = _SECONDS_PER_WEEK * interval
    elif freq == "MONTHLY":
        interval_seconds = _SECONDS_PER_DAY * 30 * interval  # Rough estimate
    elif freq == "YEARLY":
        interval_seconds = _SECONDS_PER_DAY * 365 * interval  # Rough estimate
    else:
        return 0
    
    # Skip to within a few intervals of the target
    return max(0, int(time_diff / interval_seconds) - 2)

def advance_recurrence(current_ts, freq, interval, tzid):
    """
    Advance a timestamp by one recurrence interval.
    
    Handles calendar arithmetic for different frequencies while respecting
    timezone conversions and edge cases (leap years, month boundaries).
    
    Args:
        current_ts: Current timestamp
        freq: Frequency (DAILY, WEEKLY, MONTHLY, YEARLY)
        interval: Interval multiplier
        tzid: Timezone for calculations
        
    Returns:
        Next occurrence timestamp
    """
    try:
        local_time = convert_to_local(current_ts, tzid)
        
        if freq == "DAILY":
            new_local = (local_time[0], local_time[1], local_time[2] + interval,
                       local_time[3], local_time[4], local_time[5])
        elif freq == "WEEKLY":
            new_local = (local_time[0], local_time[1], local_time[2] + (7 * interval),
                       local_time[3], local_time[4], local_time[5])
        elif freq == "MONTHLY":
            new_month = local_time[1] + interval
            new_year = local_time[0] + (new_month - 1) // 12
            new_month = (new_month - 1) % 12 + 1
            
            # Handle month boundaries (simplified approach)
            new_day = local_time[2]
            if new_month == 2 and new_day > 28:
                new_day = 28
            elif new_month in (4, 6, 9, 11) and new_day > 30:
                new_day = 30
                
            new_local = (new_year, new_month, new_day,
                       local_time[3], local_time[4], local_time[5])
        elif freq == "YEARLY":
            new_year = local_time[0] + interval
            new_day = local_time[2]
            # Handle Feb 29 in non-leap years
            if local_time[1] == 2 and local_time[2] == 29 and new_year % 4 != 0:
                new_day = 28
                
            new_local = (new_year, local_time[1], new_day,
                       local_time[3], local_time[4], local_time[5])
        else:
            return current_ts + _SECONDS_PER_DAY * interval
        
        return convert_to_utc(new_local, tzid)
        
    except:
        # Fallback to simple arithmetic
        fallback_intervals = {
            "DAILY": _SECONDS_PER_DAY,
            "WEEKLY": _SECONDS_PER_WEEK,
            "MONTHLY": _SECONDS_PER_DAY * 30,
            "YEARLY": _SECONDS_PER_DAY * 365
        }
        return current_ts + fallback_intervals.get(freq, _SECONDS_PER_DAY) * interval

def find_next_occurrence(search_start_ts, event_data, duration_sec, now_ts, tzid, exdates=None, end_filter_ts=None):
    """
    Find the next occurrence of a recurring event after search_start_ts.
    
    Args:
        search_start_ts: Start searching from this timestamp
        event_data: Event data dictionary with RRULE information
        duration_sec: Event duration in seconds
        now_ts: Current timestamp
        tzid: Event timezone
        exdates: List of excluded dates
        end_filter_ts: Stop searching after this timestamp
        
    Returns:
        Next occurrence timestamp or None if no more occurrences
    """
    if event_data is None or '_parsed_rrule' not in event_data:
        return None

    original_base_dtstart = event_data['dtstart']
    rule = event_data['_parsed_rrule']
    
    freq = rule.get("FREQ")
    if not freq:
        return None
        
    interval = int(rule.get("INTERVAL", "1"))
    byday_processed = rule.get("_byday_processed", [])
    bymonth_processed = rule.get("_bymonth_processed", [])
    bymonthday_processed = rule.get("_bymonthday_processed", [])
    until_ts = rule.get("_until_ts")
    count_limit = int(rule.get("COUNT", "0"))

    # Determine effective end time
    effective_until_ts = until_ts
    if end_filter_ts:
        effective_until_ts = min(until_ts, end_filter_ts) if until_ts else end_filter_ts

    if effective_until_ts and original_base_dtstart > effective_until_ts:
        return None

    ex_set = set(exdates or []) if exdates else set()
    
    # Use skip calculation to avoid iterating through many occurrences
    candidate_ts = original_base_dtstart
    skip_count = calculate_skip_count(original_base_dtstart, search_start_ts, freq, interval)
    
    # Apply calculated skip
    for _ in range(skip_count):
        candidate_ts = advance_recurrence(candidate_ts, freq, interval, tzid)
        if candidate_ts is None:
            return None
    
    # Search for valid occurrence
    iterations = 0
    count = skip_count  # Track total count including skipped
    
    while iterations < MAX_RECURRENCE_ITERATIONS:
        iterations += 1
        
        # Check termination conditions
        if effective_until_ts and candidate_ts > effective_until_ts:
            return None
            
        if count_limit and count >= count_limit:
            return None

        # Check RRULE constraints
        is_valid = matches_recurrence_rules(
            candidate_ts, tzid, byday_processed, bymonth_processed, bymonthday_processed
        )

        # Check if this occurrence is excluded
        is_excluded = candidate_ts in ex_set

        if is_valid and not is_excluded and candidate_ts >= search_start_ts:
            return candidate_ts

        # Advance to next occurrence
        candidate_ts = advance_recurrence(candidate_ts, freq, interval, tzid)
        if candidate_ts is None:
            return None
            
        count += 1
            
    return None

# === EVENT PROCESSING ===
def process_event_occurrences(event_data, start_filter_ts, end_filter_ts, now_ts):
    """
    Generate all occurrences of an event within the specified time range.
    
    Handles both single events and recurring events with their exceptions.
    
    Args:
        event_data: Raw event data dictionary
        start_filter_ts: Filter events starting after this timestamp
        end_filter_ts: Filter events starting before this timestamp  
        now_ts: Current timestamp
        
    Yields:
        Event objects for each occurrence
    """
    base_dtstart = event_data['dtstart']
    base_dtend = event_data['dtend']
    summary = event_data.get('summary', '')
    description = event_data.get('description', '')
    tzid = event_data.get('tzid', 'UTC')

    duration = base_dtend - base_dtstart

    is_recurring_series = '_parsed_rrule' in event_data and event_data.get('_parsed_rrule') is not None
    has_rdate_exceptions = len(event_data.get('rdates', [])) > 0
    is_override = 'recurrence_id' in event_data

    # Skip recurrence exception events (they override specific occurrences)
    if is_override:
        return

    # Handle single (non-recurring) events
    if not is_recurring_series and not has_rdate_exceptions:
        if base_dtend >= start_filter_ts and base_dtstart <= end_filter_ts:
            yield Event(base_dtstart, base_dtend, summary, description, tzid)
        return

    # Handle recurring events
    if is_recurring_series:
        current_search_point = start_filter_ts
        occurrences_found = 0

        while occurrences_found < MAX_OCCURRENCES_PER_EVENT:
            next_occurrence_ts = find_next_occurrence(
                search_start_ts=current_search_point,
                event_data=event_data,
                duration_sec=duration,
                now_ts=now_ts,
                tzid=tzid,
                exdates=event_data.get('exdates', []),
                end_filter_ts=end_filter_ts
            )

            if next_occurrence_ts is None or next_occurrence_ts > end_filter_ts:
                break

            yield Event(next_occurrence_ts, next_occurrence_ts + duration, summary, description, tzid)
            occurrences_found += 1
            current_search_point = next_occurrence_ts + 1

    # Handle additional dates (RDATE)
    for i, r_ts in enumerate(event_data.get('rdates', [])):
        if i >= MAX_RDATE_COUNT:
            break
        if (r_ts + duration >= start_filter_ts) and (r_ts <= end_filter_ts):
            yield Event(r_ts, r_ts + duration, summary, description, tzid)

# === HTTP UTILITIES ===
def canonicalize_url(url):
    """
    Normalize calendar URL to HTTPS format.
    
    Args:
        url: Input URL (may be webcal://, ical://, or missing protocol)
        
    Returns:
        Normalized HTTPS URL
    """
    if url.startswith("webcal://"):
        return url.replace("webcal://", "https://", 1)
    elif url.startswith("ical://"):
        return url.replace("ical://", "https://", 1)
    elif not (url.startswith("http://") or url.startswith("https://")):
        return "https://" + url
    return url

def calculate_content_hash(content_bytes):
    """
    Calculate hash of content for change detection when HTTP headers unavailable.
    
    Args:
        content_bytes: Raw content bytes
        
    Returns:
        Content hash string
    """
    try:
        import uhashlib
        hasher = uhashlib.sha256()
        hasher.update(content_bytes)
        return hasher.hexdigest()[:16]  # Compact hash
    except:
        # Fallback checksum
        return str(sum(content_bytes) % 999999)

def http_head_request(url, timeout=None):
    """
    Perform HEAD request to get metadata without downloading content.
    
    Args:
        url: URL to request
        timeout: Request timeout in seconds
        
    Returns:
        Tuple of (last_modified, etag, content_length) or (None, None, None) on failure
    """
    if timeout is None:
        timeout = HTTP_TIMEOUT
        
    print(f"DEBUG: Performing HEAD request to {url}")
    response = None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; MicroPython ICS Parser)",
            "Accept": "text/calendar, text/html, application/xml;q=0.9, */*;q=0.8",
            "Accept-Encoding": "identity"
        }
        
        response = urequests.request("HEAD", url, headers=headers, timeout=timeout)
        
        if response.status_code != 200:
            print(f"HEAD request failed with status {response.status_code}")
            return None, None, None
        
        print("Processing HEAD response headers")
        
        last_modified = response.headers.get("Last-Modified")
        etag = response.headers.get("ETag")
        content_length = response.headers.get("Content-Length")
        
        try:
            content_length = int(content_length) if content_length else None
        except (ValueError, TypeError):
            content_length = None
            
        print(f"DEBUG: HEAD response - Last-Modified: {last_modified}, ETag: {etag}, Content-Length: {content_length}")
        
        return last_modified, etag, content_length
        
    except Exception as e:
        print(f"HEAD request failed: {e}")
        return None, None, None
    finally:
        if response:
            response.close()

def http_fetch_content(url, cached_entry=None, timeout=None):
    """
    Fetch calendar content via HTTP GET request.
    
    Args:
        url: URL to fetch
        cached_entry: Cached data for comparison (unused in current implementation)
        timeout: Request timeout in seconds
        
    Returns:
        Tuple of (content_bytes, last_modified, etag, status_code, content_length, content_hash)
    """
    if timeout is None:
        timeout = HTTP_TIMEOUT
        
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MicroPython ICS Parser)",
        "Accept": "text/calendar, text/html, application/xml;q=0.9, */*;q=0.8",
        "Accept-Encoding": "identity"
    }

    response = urequests.get(url, headers=headers, timeout=timeout)
    content_bytes = response.content
    last_modified = response.headers.get("Last-Modified")
    etag = response.headers.get("ETag")
    
    # Calculate hash and size for caching
    content_hash = calculate_content_hash(content_bytes)
    content_length = len(content_bytes)
    
    return content_bytes, last_modified, etag, 200, content_length, content_hash

# === CACHING UTILITIES ===
def is_content_fresh(url, cached_entry):
    """
    Use HEAD request to check if cached content is still fresh.
    
    Args:
        url: URL to check
        cached_entry: Tuple of cached data
        
    Returns:
        False if content needs to be refetched, True if cache is still valid
    """
    if not cached_entry:
        return False  # No cache, need to fetch
        
    # Handle cache format safely
    if len(cached_entry) >= 4:
        events_list, cached_last_modified, cached_etag, parse_time = cached_entry[:4]
        cached_content_length = cached_entry[4] if len(cached_entry) > 4 else None
        cached_hash = cached_entry[5] if len(cached_entry) > 5 else None
    else:
        return False  # Invalid cache format
    
    # Perform HEAD request to get current metadata
    head_last_modified, head_etag, head_content_length = http_head_request(url)
    
    # Compare headers (most reliable indicators)
    if head_last_modified and cached_last_modified:
        if head_last_modified != cached_last_modified:
            print("Content changed: Last-Modified differs")
            return False
            
    if head_etag and cached_etag:
        if head_etag != cached_etag:
            print("Content changed: ETag differs")
            return False
    
    # If we have reliable headers that match, content is fresh
    if (head_last_modified and cached_last_modified) or (head_etag and cached_etag):
        print("Content fresh: Headers match")
        return True
    
    # Fallback to content-length if no reliable headers
    if head_content_length and cached_content_length:
        if head_content_length != cached_content_length:
            print("Content likely changed: Content-Length differs")
            return False
        print("Content possibly fresh: Content-Length matches")
        return True
    
    # No reliable indicators - use time-based fallback
    cache_age = utime.time() - parse_time
    if cache_age > 900:  # 15 minutes
        print("Cache expired: Time-based refresh")
        return False
        
    print("Using cached content: No change indicators")
    return True

# === ICS PARSING ===
def process_ics_line(line_bytes):
    """
    Process a single logical line from an iCalendar file.
    
    Handles VEVENT parsing and extracts relevant properties into global state.
    This function maintains state across calls for the current event being parsed.
    
    Args:
        line_bytes: Single unfolded line from iCalendar as bytes
    """
    global _in_event_state, _current_raw_event_state, _raw_events_data_list

    # Handle event boundaries
    if line_bytes == b'BEGIN:VEVENT':
        _in_event_state = True
        _current_raw_event_state = {
            'rdates': [], 'exdates': [], '_parsed_rrule': None,
            'dtstart': 0, 'dtend': 0, 'tzid': "UTC",
            'summary': "", 'description': ""
        }
        return

    if line_bytes == b'END:VEVENT' and _in_event_state:
        if _current_raw_event_state and _current_raw_event_state['dtstart']:
            # Set default end time if missing (1 hour duration)
            if _current_raw_event_state['dtend'] == 0:
                _current_raw_event_state['dtend'] = _current_raw_event_state['dtstart'] + 3600
            _raw_events_data_list.append(_current_raw_event_state)
        _current_raw_event_state = None
        _in_event_state = False
        return

    if not _in_event_state or not _current_raw_event_state:
        return

    # Parse event properties
    if line_bytes.startswith(b'DTSTART'):
        line_str = line_bytes.decode('utf-8')
        ts = parse_datetime(line_str)
        if ts is not None:
            _current_raw_event_state['dtstart'] = ts
            # Extract timezone from DTSTART line
            tzid_pos = line_str.find("TZID=")
            if tzid_pos != -1:
                tz_start = tzid_pos + 5
                tz_end = line_str.find(":", tz_start)
                if tz_end != -1:
                    _current_raw_event_state['tzid'] = normalize_timezone_id(line_str[tz_start:tz_end])

    elif line_bytes.startswith(b'DTEND'):
        line_str = line_bytes.decode('utf-8')
        ts = parse_datetime(line_str)
        if ts is not None:
            _current_raw_event_state['dtend'] = ts

    elif line_bytes.startswith(b'SUMMARY:'):
        _current_raw_event_state['summary'] = line_bytes[8:].decode('utf-8').strip()

    elif line_bytes.startswith(b'DESCRIPTION'):
        colon_pos = line_bytes.find(b':')
        if colon_pos != -1:
            _current_raw_event_state['description'] = clean_description(
                line_bytes[colon_pos+1:].decode('utf-8').strip()
            )

    elif line_bytes.startswith(b'RRULE:'):
        rrule_val_str = line_bytes[6:].decode('utf-8')
        _current_raw_event_state['rrule'] = rrule_val_str

        try:
            parsed_rule = parse_rrule(rrule_val_str)
            
            # Process BYDAY constraints
            if "BYDAY" in parsed_rule:
                try:
                    processed_byday = []
                    for bd_str in parsed_rule["BYDAY"].split(","):
                        clean_bd = bd_str.strip().upper()
                        if clean_bd in _DAY_MAP:
                            processed_byday.append(_DAY_MAP[clean_bd])
                    parsed_rule['_byday_processed'] = processed_byday
                except:
                    parsed_rule['_byday_processed'] = []
            else:
                parsed_rule['_byday_processed'] = []

            # Process BYMONTH constraints
            if "BYMONTH" in parsed_rule:
                try:
                    processed_bymonth = []
                    for m_str in parsed_rule["BYMONTH"].split(","):
                        try:
                            processed_bymonth.append(int(m_str))
                        except ValueError:
                            pass
                    parsed_rule['_bymonth_processed'] = processed_bymonth
                except:
                    parsed_rule['_bymonth_processed'] = []
            else:
                parsed_rule['_bymonth_processed'] = []

            # Process BYMONTHDAY constraints
            if "BYMONTHDAY" in parsed_rule:
                try:
                    processed_bymonthday = []
                    for md_str in parsed_rule["BYMONTHDAY"].split(","):
                        try:
                            processed_bymonthday.append(int(md_str))
                        except ValueError:
                            pass
                    parsed_rule['_bymonthday_processed'] = processed_bymonthday
                except:
                    parsed_rule['_bymonthday_processed'] = []
            else:
                parsed_rule['_bymonthday_processed'] = []

            # Process UNTIL constraint
            if "UNTIL" in parsed_rule:
                u = parsed_rule["UNTIL"]
                is_utc = u.endswith("Z")
                u_clean = u[:-1] if is_utc else u
                try:
                    if len(u_clean) >= 8:
                        y, mo, d = int(u_clean[0:4]), int(u_clean[4:6]), int(u_clean[6:8])
                        h = int(u_clean[9:11]) if len(u_clean) >= 11 else 0
                        mi = int(u_clean[11:13]) if len(u_clean) >= 13 else 0
                        s = int(u_clean[13:15]) if len(u_clean) >= 15 else 0

                        if is_utc:
                            parsed_rule['_until_ts'] = utime.mktime((y, mo, d, h, mi, s, 0, 0))
                        else:
                            parsed_rule['_until_ts'] = convert_to_utc((y, mo, d, h, mi, s), _current_raw_event_state.get('tzid', 'UTC'))
                except:
                    parsed_rule['_until_ts'] = None
            else:
                parsed_rule['_until_ts'] = None

        except:
            parsed_rule = {}
        
        _current_raw_event_state['_parsed_rrule'] = parsed_rule

    elif line_bytes.startswith(b'EXDATE'):
        parse_date_list(line_bytes.decode('utf-8'), _current_raw_event_state, 'exdates')

    elif line_bytes.startswith(b'RDATE'):
        parse_date_list(line_bytes.decode('utf-8'), _current_raw_event_state, 'rdates')

    elif line_bytes.startswith(b'RECURRENCE-ID'):
        rid = parse_datetime(line_bytes.decode('utf-8'))
        if rid is not None:
            _current_raw_event_state['recurrence_id'] = rid

def parse_calendar_from_url(url, end_filter_days=31):
    """
    Parse iCalendar data from a URL and yield Event objects.
    
    This is the main parsing function that handles HTTP fetching, caching,
    and iCalendar parsing with recurrence processing.
    
    Args:
        url: Calendar URL to fetch and parse
        end_filter_days: Only return events starting within this many days
        
    Yields:
        Event objects sorted by start time
    """
    global _in_event_state, _current_raw_event_state, _raw_events_data_list, _parsing_cache

    # Initialize parsing state
    _in_event_state = False
    _current_raw_event_state = None
    _raw_events_data_list = []

    # Calculate time filters
    start_filter_ts = utime.time()
    end_filter_ts = start_filter_ts + (end_filter_days * _SECONDS_PER_DAY)
    now_ts = utime.time()

    # Normalize URL
    processed_url = canonicalize_url(url)

    # Check cache freshness
    cached_entry = _parsing_cache.get(processed_url)
    
    if is_content_fresh(processed_url, cached_entry):
        # Content is fresh, return cached events if they exist
        if cached_entry:
            print("Using cached events (HEAD check confirms freshness)")
            events_list = cached_entry[0]
            for event in events_list:
                if event.dtend >= start_filter_ts and event.dtstart <= end_filter_ts:
                    yield event
            return

    # Content changed or no cache - fetch with GET
    fetch_result = http_fetch_content(processed_url)
    
    if len(fetch_result) == 6:
        all_ics_content_bytes, new_last_modified, new_etag, status_code, content_length, content_hash = fetch_result
    else:
        # Fallback for different return format
        all_ics_content_bytes, new_last_modified, new_etag, status_code = fetch_result[:4]
        content_length = len(all_ics_content_bytes) if all_ics_content_bytes else 0
        content_hash = calculate_content_hash(all_ics_content_bytes) if all_ics_content_bytes else ""

    if status_code == 304 or all_ics_content_bytes is None:
        return

    # Process iCalendar content (unfold lines and parse)
    unfolded_line_bytes = b""
    for raw_line_bytes in all_ics_content_bytes.splitlines():
        if not raw_line_bytes:
            continue
            
        # Handle line unfolding (lines starting with space/tab are continuations)
        if raw_line_bytes[0:1] in (b' ', b'\t') and unfolded_line_bytes:
            unfolded_line_bytes += raw_line_bytes[1:]
        else:
            # Process previous complete line
            if unfolded_line_bytes:
                process_ics_line(unfolded_line_bytes)
            unfolded_line_bytes = raw_line_bytes
    
    # Process final line
    if unfolded_line_bytes:
        process_ics_line(unfolded_line_bytes)

    # Post-processing: separate recurrence exceptions from regular events
    overrides_map = {}
    regular_events_raw = []
    
    for raw_event in _raw_events_data_list:
        if 'recurrence_id' in raw_event:
            # This is a recurrence exception (overrides a specific occurrence)
            rid_dtstart = raw_event['recurrence_id']
            rid_tzid = raw_event.get('tzid', 'UTC')
            overrides_map[(rid_dtstart, rid_tzid)] = raw_event
        else:
            regular_events_raw.append(raw_event)
    
    # Clear raw events data to free memory
    _raw_events_data_list.clear()
    gc.collect()

    # Generate event occurrences
    generated_events = []
    for raw_event in regular_events_raw:
        for occurrence in process_event_occurrences(raw_event, start_filter_ts, end_filter_ts, now_ts):
            # Check if this occurrence has an override
            override_key = (occurrence.dtstart, occurrence.tzid)
            if override_key in overrides_map:
                # Use the override event instead of the generated occurrence
                override_raw_data = overrides_map[override_key]
                override_event = Event(
                    override_raw_data['dtstart'], 
                    override_raw_data['dtend'],
                    override_raw_data.get('summary', ''), 
                    override_raw_data.get('description', ''),
                    override_raw_data.get('tzid', 'UTC')
                )
                generated_events.append(override_event)
            else:
                generated_events.append(occurrence)
        
        # Periodic garbage collection during processing
        if len(generated_events) % 10 == 0:
            gc.collect()
    
    # Sort and yield events
    if generated_events:
        generated_events.sort()
        for event in generated_events:
            yield event

    # Update cache with new data (consistent 6-element format)
    _parsing_cache[processed_url] = (
        generated_events, 
        new_last_modified, 
        new_etag, 
        utime.time(),
        content_length,
        content_hash
    )

# === PUBLIC API ===
def get_events(url, max_events=None, start_filter_ts=None, end_filter_days=31):
    """
    Get calendar events from a URL with caching and filtering.
    
    This is the main public function for retrieving calendar events.
    
    Args:
        url: Calendar URL to fetch (supports http://, https://, webcal://, ical://)
        max_events: Maximum number of events to return (default: DEFAULT_MAX_EVENTS)
        start_filter_ts: Only return events ending after this timestamp (default: now)
        end_filter_days: Only return events starting within this many days (default: 14)
        
    Returns:
        List of Event objects sorted by start time
        
    Example:
        events = get_events("https://calendar.example.com/cal.ics", max_events=20)
        for event in events:
            print(f"{event.summary}: {event.dtstart}")
    """
    global _parsing_cache

    if max_events is None:
        max_events = DEFAULT_MAX_EVENTS
    if start_filter_ts is None:
        start_filter_ts = utime.time()

    # Normalize URL for cache key
    processed_url = canonicalize_url(url)

    # Check cache validity
    cached_entry = _parsing_cache.get(processed_url)
    now = utime.time()
    
    if cached_entry and len(cached_entry) >= 4 and (now - cached_entry[3] < CACHE_VALIDITY_SECONDS):
        # Use cached data if still valid
        events_list = cached_entry[0]
        end_filter_ts = start_filter_ts + (end_filter_days * _SECONDS_PER_DAY)
        filtered_events = [e for e in events_list 
                         if e.dtend >= start_filter_ts and e.dtstart <= end_filter_ts]
        filtered_events.sort()
        return filtered_events[:max_events]

    try:
        # Parse fresh data from URL
        temp_events_list = []
        seen_events = set()

        for event in parse_calendar_from_url(processed_url, end_filter_days):
            event_hash = hash(event)
            if event_hash not in seen_events:
                temp_events_list.append(event)
                seen_events.add(event_hash)
                if max_events > 0 and len(temp_events_list) >= max_events:
                    break

        if temp_events_list:
            temp_events_list.sort()
            return temp_events_list[:max_events]
        elif cached_entry and len(cached_entry) >= 1:
            # Fallback to cached events if available
            events_list = cached_entry[0]
            end_filter_ts = start_filter_ts + (end_filter_days * _SECONDS_PER_DAY)
            filtered_events = [e for e in events_list 
                             if e.dtend >= start_filter_ts and e.dtstart <= end_filter_ts]
            filtered_events.sort()
            return filtered_events[:max_events]
        
        return []
        
    except Exception as e:
        print(f"Calendar fetch or parse failed: {e}")
        # Try to use cached data as fallback
        if cached_entry and len(cached_entry) >= 1:
            events_list = cached_entry[0]
            end_filter_ts = start_filter_ts + (end_filter_days * _SECONDS_PER_DAY)
            filtered_events = [e for e in events_list 
                             if e.dtend >= start_filter_ts and e.dtstart <= end_filter_ts]
            filtered_events.sort()
            return filtered_events[:max_events]
        return []

def clear_cache():
    """
    Clear the calendar parsing cache.
    
    Use this to force fresh fetching of all calendars or to free memory.
    """
    global _parsing_cache
    _parsing_cache.clear()

def set_limits(max_recurrence_iterations=None, max_occurrences_per_event=None, 
               cache_validity_seconds=None, max_description_length=None,
               max_rdate_count=None, http_timeout=None):
    """
    Adjust performance and memory limits.
    
    Use this function to tune the parser for your specific memory and 
    performance requirements.
    
    Args:
        max_recurrence_iterations: Maximum iterations when calculating recurring events
        max_occurrences_per_event: Maximum occurrences to generate per recurring event
        cache_validity_seconds: How long cached data remains valid
        max_description_length: Maximum length of event descriptions
        max_rdate_count: Maximum number of RDATE entries to process per event
        http_timeout: HTTP request timeout in seconds
    """
    global MAX_RECURRENCE_ITERATIONS, MAX_OCCURRENCES_PER_EVENT, CACHE_VALIDITY_SECONDS
    global MAX_DESCRIPTION_LENGTH, MAX_RDATE_COUNT, HTTP_TIMEOUT
    
    if max_recurrence_iterations is not None:
        MAX_RECURRENCE_ITERATIONS = max_recurrence_iterations
    if max_occurrences_per_event is not None:
        MAX_OCCURRENCES_PER_EVENT = max_occurrences_per_event
    if cache_validity_seconds is not None:
        CACHE_VALIDITY_SECONDS = cache_validity_seconds
    if max_description_length is not None:
        MAX_DESCRIPTION_LENGTH = max_description_length
    if max_rdate_count is not None:
        MAX_RDATE_COUNT = max_rdate_count
    if http_timeout is not None:
        HTTP_TIMEOUT = http_timeout

def get_memory_stats():
    """
    Get basic memory usage information if available.
    
    Returns:
        Dictionary with memory statistics or status message
    """
    try:
        import gc
        gc.collect()
        # Try to get memory info (may not be available on all MicroPython implementations)
        try:
            import micropython
            return {
                'free_heap': micropython.mem_info()[0] if hasattr(micropython, 'mem_info') else 'unknown',
                'allocated': micropython.mem_info()[1] if hasattr(micropython, 'mem_info') else 'unknown'
            }
        except:
            return {'status': 'gc.collect() called, detailed stats not available'}
    except:
        return {'status': 'memory stats not available'}

def get_cache_info():
    """
    Get information about the current cache state.
    
    Returns:
        Dictionary with cache statistics
    """
    return {
        'cached_urls': len(_parsing_cache),
        'cache_keys': list(_parsing_cache.keys()) if _parsing_cache else []
    }


