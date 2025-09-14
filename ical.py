import utime
import urequests
import gc
import sys
from machine import RTC

# Add local module path
sys.path.append('./lib')

from presto import Buzzer, Presto
from picovector import PicoVector, ANTIALIAS_BEST, Polygon, Transform
import ical_parser
import config_data

CONFIG = config_data.CONFIG
ICS_URL = CONFIG["ical_url"]

# Exchange Online compatible user agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0"
HEADERS = {"User-Agent": USER_AGENT}

# === GLOBAL VARIABLES ===
REGIONS = {}
THEMES = None
THEME = None
_sleep_text_pen = None

# Application state
_current_page = 0
_events = []
_last_refresh = 0
_last_activity = 0
_last_touch = None
_initial_touch_x = None
_initial_touch_y = None

# Hardware state
_backlight_dimmed = False
_backlight_check_interval = 0

# Network status tracking
_network_status = {"connected": True, "last_error": 0, "error_count": 0, "last_success": 0}

# Screen sleep state
_screen_sleep_state = {"active": False, "sleep_start_time": 0}

# === HARDWARE INITIALIZATION ===
buzzer = Buzzer(CONFIG.get("buzzer_pin", 43))
presto = Presto(ambient_light=False, full_res=True)
display = presto.display
touch = presto.touch
WIDTH, HEIGHT = display.get_bounds()
vector = PicoVector(display)
vector.set_antialiasing(ANTIALIAS_BEST)
font_size = CONFIG.get("FONT_HEIGHT", 8)
vector.set_font("Roboto-Medium-With-Material-Symbols.af", font_size)
transform = Transform()

# === SCREEN REGION MANAGEMENT ===
class ScreenRegion:
    def __init__(self, x, y, width, height, name):
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.name = name
        self.last_content = None
        self.dirty = True
        
    def mark_dirty(self):
        self.dirty = True
        
    def is_dirty(self):
        return self.dirty
        
    def clear_dirty(self):
        self.dirty = False
        
    def set_content_hash(self, content_hash):
        if self.last_content != content_hash:
            self.last_content = content_hash
            self.dirty = True
            return True
        return False

def init_regions():
    """Initialize screen regions - 6 horizontal regions in top row, events below."""
    global REGIONS
    
    # Calculate header row height based on title font
    title_font_size = CONFIG.get("TITLE_FONT_HEIGHT", 32)
    vector.set_font_size(title_font_size)
    _, _, _, title_height = vector.measure_text("Calendar Mg")
    vector.set_font_size(CONFIG.get("FONT_HEIGHT", 8))  # Reset to normal font
    
    # Header row height with padding
    padding_top = CONFIG.get("TITLE_PADDING_TOP", 20)
    padding_bottom = CONFIG.get("TITLE_PADDING_BOTTOM", 15)
    header_row_height = padding_top + title_height + padding_bottom
    
    # Define widths for each region in the top row
    pagination_left_width = 50
    title_width = 190
    refresh_width = 130
    memory_width = 60
    status_width = 60
    pagination_right_width = 50
    
    # Calculate starting positions
    x_positions = []
    current_x = 0
    widths = [pagination_left_width, title_width, refresh_width, memory_width, status_width, pagination_right_width]
    
    for width in widths:
        x_positions.append(current_x)
        current_x += width
    
    # Adjust widths to fit screen if needed
    total_width = sum(widths)
    if total_width > WIDTH:
        scale_factor = WIDTH / total_width
        widths = [int(w * scale_factor) for w in widths]
        # Recalculate positions
        x_positions = []
        current_x = 0
        for width in widths:
            x_positions.append(current_x)
            current_x += width
    
    REGIONS = {
        "pagination_left": ScreenRegion(x_positions[0], 0, widths[0], header_row_height, "pagination_left"),
        "header": ScreenRegion(x_positions[1], 0, widths[1], header_row_height, "header"),
        "refresh": ScreenRegion(x_positions[2], 0, widths[2], header_row_height, "refresh"),
        "memory": ScreenRegion(x_positions[3], 0, widths[3], header_row_height, "memory"),
        "status": ScreenRegion(x_positions[4], 0, widths[4], header_row_height, "status"),
        "pagination_right": ScreenRegion(x_positions[5], 0, widths[5], header_row_height, "pagination_right"),
        "events": ScreenRegion(0, header_row_height + 5, WIDTH, HEIGHT - header_row_height - 5, "events"),
    }

def update_region(region_name, update_func, force=False):
    """Update a specific screen region only if dirty or forced."""
    if region_name not in REGIONS:
        return False
        
    region = REGIONS[region_name]
    if not force and not region.is_dirty():
        return False
    
    # Clear the region background first
    display.set_pen(THEME["BG_COLOR"])
    clear_rect = Polygon()
    clear_rect.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
    vector.draw(clear_rect)
    
    # Call the update function
    try:
        result = update_func(region)
        region.clear_dirty()
        return True
    except Exception as e:
        print(f"Region update failed for {region_name}: {e}")
        return False

def mark_region_dirty(region_name):
    """Mark a region as needing update."""
    if region_name in REGIONS:
        REGIONS[region_name].mark_dirty()

def mark_all_regions_dirty():
    """Mark all regions as needing update."""
    for region in REGIONS.values():
        region.mark_dirty()

# === THEME MANAGEMENT ===
def create_theme_pens():
    """Create pen objects for all theme colors."""
    return {
        "dark": {
            "TEXT_COLOR": display.create_pen(255, 255, 255),
            "TEXT_SECONDARY": display.create_pen(128, 128, 128),
            "BG_COLOR": display.create_pen(0, 0, 0),
            "ROW_BG_COLOR_ODD": display.create_pen(8, 8, 8),
            "ROW_BG_COLOR_EVEN": display.create_pen(32, 32, 32),
            "HEADER_BG_COLOR": display.create_pen(45, 45, 45),
            "HEADER_TEXT_COLOR": display.create_pen(240, 240, 240),
            "ONGOING_ACCENT": display.create_pen(0, 255, 0),
            "FUTURE_ACCENT": display.create_pen(74, 144, 226),
            "PAST_ACCENT": display.create_pen(128, 128, 128),
            "ALLDAY_ACCENT": display.create_pen(155, 89, 182),
            "PAGINATION_COLOR": display.create_pen(200, 200, 200),
            "TITLE_COLOR": display.create_pen(240, 240, 240),
            "REFRESH_COLOR": display.create_pen(180, 180, 180),
            "MEMORY_COLOR": display.create_pen(135, 206, 235),
            "EVENT_TEXT_CANCELED": display.create_pen(153, 153, 153),
            "EVENT_TEXT_ONGOING": display.create_pen(255, 255, 255),
            "EVENT_TEXT_ALLDAY": display.create_pen(204, 204, 204),
            "EVENT_TEXT_FUTURE": display.create_pen(238, 238, 238),
            "EVENT_TEXT_PAST": display.create_pen(170, 170, 170),
            "EVENT_DESC_CANCELED": display.create_pen(107, 107, 107),
            "EVENT_DESC_ONGOING": display.create_pen(178, 178, 178),
            "EVENT_DESC_ALLDAY": display.create_pen(142, 142, 142),
            "EVENT_DESC_FUTURE": display.create_pen(166, 166, 166),
            "EVENT_DESC_PAST": display.create_pen(119, 119, 119),
        },
        "light": {
            "TEXT_COLOR": display.create_pen(33, 37, 41),
            "TEXT_SECONDARY": display.create_pen(73, 80, 87),
            "BG_COLOR": display.create_pen(250, 251, 252),
            "ROW_BG_COLOR_ODD": display.create_pen(255, 255, 255),
            "ROW_BG_COLOR_EVEN": display.create_pen(240, 242, 245),
            "HEADER_BG_COLOR": display.create_pen(52, 58, 64),
            "HEADER_TEXT_COLOR": display.create_pen(248, 249, 250),
            "ONGOING_ACCENT": display.create_pen(40, 167, 69),
            "FUTURE_ACCENT": display.create_pen(0, 123, 255),
            "PAST_ACCENT": display.create_pen(108, 117, 125),
            "ALLDAY_ACCENT": display.create_pen(111, 66, 193),
            "PAGINATION_COLOR": display.create_pen(248, 249, 250),
            "TITLE_COLOR": display.create_pen(248, 249, 250),
            "REFRESH_COLOR": display.create_pen(206, 212, 218),
            "MEMORY_COLOR": display.create_pen(255, 193, 203),
            "EVENT_TEXT_CANCELED": display.create_pen(108, 117, 125),
            "EVENT_TEXT_ONGOING": display.create_pen(33, 37, 41),
            "EVENT_TEXT_ALLDAY": display.create_pen(52, 58, 64),
            "EVENT_TEXT_FUTURE": display.create_pen(33, 37, 41),
            "EVENT_TEXT_PAST": display.create_pen(134, 142, 150),
            "EVENT_DESC_CANCELED": display.create_pen(134, 142, 150),
            "EVENT_DESC_ONGOING": display.create_pen(73, 80, 87),
            "EVENT_DESC_ALLDAY": display.create_pen(108, 117, 125),
            "EVENT_DESC_FUTURE": display.create_pen(73, 80, 87),
            "EVENT_DESC_PAST": display.create_pen(173, 181, 189),
        },
    }

def init_themes():
    """Initialize theme pen objects."""
    global THEMES, THEME, _sleep_text_pen
    THEMES = create_theme_pens()
    THEME = THEMES[CONFIG.get("theme", "dark")]
    _sleep_text_pen = display.create_pen(102, 102, 102)
    
# === UTILITY FUNCTIONS ===
def get_max_events():
    """Get maximum events from config with fallback."""
    return CONFIG.get("MAX_EVENTS", 40)

def get_text_metrics(text="M"):
    """Get text dimensions."""
    if not text:
        return 0, 0
    _, _, w, h = vector.measure_text(text)
    return int(w), int(h)

def truncate_text_to_width(text, max_width):
    """Truncate text to fit within specified pixel width."""
    if not text:
        return ""
    
    # Check if full text fits
    full_w, _ = get_text_metrics(text)
    if full_w <= max_width:
        return text
    
    # Binary search for optimal length
    left, right = 0, len(text)
    best_text = ""
    
    while left <= right:
        mid = (left + right) // 2
        candidate = text[:mid] + "…" if mid < len(text) else text[:mid]
        test_w, _ = get_text_metrics(candidate)
        
        if test_w <= max_width:
            best_text = candidate
            left = mid + 1
        else:
            right = mid - 1
    
    return best_text or "…"

def get_memory_stats():
    """Get memory usage statistics."""
    try:
        free_mem = gc.mem_free()
        alloc_mem = gc.mem_alloc()
        total_mem = free_mem + alloc_mem
        usage_pct = (alloc_mem / total_mem) * 100 if total_mem > 0 else 0
        free_pct = (free_mem / total_mem) * 100 if total_mem > 0 else 0
        return {"free": free_mem, "used": alloc_mem, "total": total_mem, "usage_pct": usage_pct, "free_pct": free_pct}
    except:
        return {"free": 0, "used": 0, "total": 0, "usage_pct": 0, "free_pct": 0}

def format_time_tuple(t, fmt="%m/%d %H:%M"):
    """Simple strftime replacement."""
    y, mo, d, hh, mm, ss = t[0:6]
    return (fmt
        .replace("%Y", str(y))
        .replace("%m", f"{mo:02d}")
        .replace("%d", f"{d:02d}")
        .replace("%H", f"{hh:02d}")
        .replace("%M", f"{mm:02d}")
        .replace("%S", f"{ss:02d}")
    )

def in_quiet_hours(ts):
    """Check if in quiet hours."""
    h = utime.localtime(ts)[3]
    start = CONFIG.get("QUIET_START_HOUR", 22)
    end = CONFIG.get("QUIET_END_HOUR", 7)
    if start < end:
        return start <= h < end
    else:
        return h >= start or h < end

def mark_activity():
    """Mark user activity to prevent dimming."""
    global _last_activity
    _last_activity = utime.time()
    
# === EVENT PROCESSING ===
def sort_events_by_priority(events):
    """Sort events: multi-day, all-day, then timed."""
    default_tz = CONFIG.get("default_tz", "UTC")

    def sort_key(e):
        tzid = e.tzid if e.tzid is not None else default_tz
        start_ts = e.dtstart
        end_ts = e.dtend
        duration = end_ts - start_ts

        sd = ical_parser.convert_to_local(start_ts, tzid)[:3]
        ed = ical_parser.convert_to_local(end_ts, tzid)[:3]

        if sd != ed:
            priority = 0  # Multi-day
        elif duration >= 86400:
            priority = 1  # All-day
        else:
            priority = 2  # Timed

        return (priority, start_ts)

    return sorted(events, key=sort_key)

def get_event_type_and_state(event, now_ts):
    """Determine event type and state."""
    display_tz = CONFIG.get("display_tz", CONFIG.get("default_tz", "America/Los_Angeles"))
    
    duration = event.dtend - event.dtstart
    start_ts = event.dtstart
    start_local = ical_parser.convert_to_local(start_ts, display_tz)
    end_local = ical_parser.convert_to_local(event.dtend, display_tz)
    
    # Event type
    multi_day = start_local[:3] != end_local[:3]
    all_day = not multi_day and duration >= 86400
    is_timed = not (multi_day or all_day)
    
    # Event state
    now_display_tz = ical_parser.convert_to_local(now_ts, display_tz)
    now_display_ts = utime.mktime(now_display_tz[:6] + (0, 0, 0))
    start_display_ts = utime.mktime(start_local[:6] + (0, 0, 0))
    
    if multi_day or all_day:
        event_start_day_ts = utime.mktime(start_local[:3] + (0,0,0,0,0))
        event_end_day_ts = utime.mktime(end_local[:3] + (0,0,0,0,0))
        ongoing = (now_display_ts >= event_start_day_ts and now_display_ts <= event_end_day_ts)
    else:
        ongoing = (start_ts <= now_ts <= event.dtend)

    is_canceled = event.summary.lower().startswith("canceled:")
    
    return {
        'multi_day': multi_day,
        'all_day': all_day,
        'is_timed': is_timed,
        'ongoing': ongoing,
        'is_canceled': is_canceled,
        'start_local': start_local,
        'end_local': end_local,
        'duration': duration,
        'summary': event.summary,
        'description': event.description
    }

def load_events():
    """Load and process events."""
    now = utime.time()
    display_tz = CONFIG.get("display_tz", CONFIG.get("default_tz", "America/Los_Angeles"))
    local_now = ical_parser.convert_to_local(now, display_tz)

    start_of_day_local = (local_now[0], local_now[1], local_now[2], 0, 0, 0)
    start_of_day_utc = ical_parser.convert_to_utc(start_of_day_local, display_tz)

    filter_time = min(CONFIG.get("start_filter", now), start_of_day_utc)

    try:
        days_to_parse = CONFIG.get("DAYS_TO_PARSE", 14)
        events = ical_parser.get_events(ICS_URL, get_max_events(), filter_time, days_to_parse)
        if events:
            sorted_events = sort_events_by_priority(events)
            gc.collect()
            return sorted_events
        else:
            print("No events loaded, returning empty list")
            return []
    except Exception as e:
        print(f"Load events failed: {e}")
        return []
    
# === DISPLAY CALCULATIONS ===
def calculate_event_card_height(event):
    """Calculate event card height using accurate text measurement."""
    title = str(event.summary if event.summary is not None else 'Untitled')
    desc = event.description if event.description is not None else ''
    
    _, line_height = get_text_metrics("Mg")
    line_spacing = CONFIG.get("LINE_SPACING", 4)
    padding = CONFIG.get("CARD_PADDING", 8)
    
    total_lines = 2  # Time and title lines always present
    
    if desc and len(desc) < 50:
        total_lines += 1
    
    card_height = padding + (total_lines * line_height) + ((total_lines - 1) * line_spacing) + padding
    
    return card_height

def get_events_per_page():
    """Calculate optimal events per page."""
    base_events_per_page = CONFIG.get("events_per_page", 8)

    if not _events:
        return base_events_per_page

    events_region = REGIONS.get("events")
    if not events_region:
        return base_events_per_page

    available_height = int(events_region.height)
    event_spacing = int(CONFIG.get("EVENT_SPACING", 2))

    sample_size = min(8, len(_events))
    sample_heights = []

    for i in range(sample_size):
        event_height = calculate_event_card_height(_events[i])
        sample_heights.append(event_height)

    if not sample_heights:
        return base_events_per_page

    # Remove outliers
    if len(sample_heights) >= 3:
        sample_heights.sort()
        if sample_heights[-1] > sample_heights[-2] * 1.5:
            sample_heights = sample_heights[:-1]

    avg_event_height = sum(sample_heights) / len(sample_heights)
    avg_event_height_with_spacing = avg_event_height + event_spacing

    if avg_event_height_with_spacing == 0:
        return base_events_per_page

    theoretical_fit = available_height / avg_event_height_with_spacing
    practical_fit = int(theoretical_fit * 0.98)

    return max(4, min(15, practical_fit))

def get_adaptive_refresh_interval():
    """Adjust refresh rate based on upcoming events and time of day."""
    now = utime.time()
    hour = utime.localtime(now)[3]
    
    if 8 <= hour <= 18:
        base_interval = CONFIG.get("REFRESH_INTERVAL", 900)
    else:
        base_interval = CONFIG.get("REFRESH_INTERVAL", 900) * 2

    if _events:
        next_event_time = None
        for event in _events[:3]:
            event_info = get_event_type_and_state(event, now)
            if (not event_info['is_canceled'] and 
                event_info['is_timed'] and 
                event.dtstart > now):
                next_event_time = event.dtstart
                break
        
        if next_event_time:
            time_until = next_event_time - now
            if time_until <= 1800:  # Next 30 minutes
                return base_interval // 2
    
    return base_interval

# === CONTENT HASHING FOR PARTIAL UPDATES ===
def calculate_content_hash(region_name, data=None):
    """Calculate a simple hash for region content to detect changes."""
    if region_name == "header":
        return hash((_last_refresh, CONFIG.get("SHOW_MEMORY", False)))
    elif region_name in ["pagination_left", "pagination_right"]:
        per_page = get_events_per_page()
        total_pages = (len(_events) + per_page - 1) // per_page
        return hash((_current_page, total_pages))
    elif region_name == "status":
        return hash((_network_status["connected"], _network_status["error_count"], 
                    _network_status["last_error"], _network_status["last_success"]))
    elif region_name == "events":
        per_page = get_events_per_page()
        start_idx = _current_page * per_page
        end_idx = start_idx + per_page
        page_events = _events[start_idx:end_idx]
        now_ts = utime.time()
        events_data = []
        for e in page_events:
            events_data.append((e.dtstart, e.dtend, e.summary, 
                              e.description, now_ts // 60))
        return hash(tuple(events_data))
    elif region_name == "memory":
        if CONFIG.get("SHOW_MEMORY", False):
            now_ts = utime.time()
            mem_stats = get_memory_stats()
            time_component = now_ts // 10
            return hash((mem_stats['free_pct'], time_component))
        return hash((0,))
    elif region_name == "refresh":
        return hash((_last_refresh,))
    return 0

# === REGION DRAWING FUNCTIONS ===
def draw_header_region(region):
    """Draw title in its own dedicated region."""
    display.set_pen(THEME["HEADER_BG_COLOR"])
    display.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
    
    title = CONFIG.get("TITLE_TEXT", "Calendar")
    title_h = int(CONFIG.get("TITLE_FONT_HEIGHT", 32))
    
    vector.set_font_size(title_h)
    title_w, title_measured_h = get_text_metrics(title)
    
    x = region.x + (region.width - title_w) // 2
    y = region.y + (region.height + title_measured_h) // 2 - 2
    
    display.set_pen(THEME["HEADER_TEXT_COLOR"])
    vector.text(title, int(x), int(y))
    
    vector.set_font_size(int(CONFIG.get("FONT_HEIGHT", 8)))

def draw_refresh_region(region):
    """Draw refresh time."""
    display.set_pen(THEME["HEADER_BG_COLOR"])
    display.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
    
    display_tz = CONFIG.get("display_tz", CONFIG.get("default_tz", "America/Los_Angeles"))
    refresh_local = ical_parser.convert_to_local(_last_refresh, display_tz)
    refresh_str = format_time_tuple(refresh_local, "%m/%d %H:%M")
    
    txt_w, txt_h = get_text_metrics(refresh_str)
    x = region.x + (region.width - txt_w) // 2
    y = region.y + (region.height + txt_h) // 2 - 2
    
    display.set_pen(THEME["REFRESH_COLOR"])
    vector.text(refresh_str, int(x), int(y))
    
def draw_memory_region(region):
    """Draw memory info."""
    display.set_pen(THEME["HEADER_BG_COLOR"])
    display.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
    
    if not CONFIG.get("SHOW_MEMORY", False):
        return
        
    mem_stats = get_memory_stats()
    mem_str = f"{mem_stats['free_pct']:.0f}%"
    
    txt_w, txt_h = get_text_metrics(mem_str)
    x = region.x + (region.width - txt_w) // 2
    y = region.y + (region.height + txt_h) // 2 - 2
    
    display.set_pen(THEME["MEMORY_COLOR"])
    vector.text(mem_str, int(x), int(y))
    
def draw_pagination_left_region(region):
    """Draw left pagination arrow."""
    display.set_pen(THEME["HEADER_BG_COLOR"])
    display.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
    
    remaining_before = _current_page
    
    if remaining_before > 0:
        txt = f"<{remaining_before}"
        txt_w, txt_h = get_text_metrics(txt)
        x = region.x + (region.width - txt_w) // 2
        y = region.y + (region.height + txt_h) // 2 - 2
        
        display.set_pen(THEME["PAGINATION_COLOR"])
        vector.text(txt, int(x), int(y))
        
def draw_pagination_right_region(region):
    """Draw right pagination arrow."""
    display.set_pen(THEME["HEADER_BG_COLOR"])
    display.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
    
    per_page = get_events_per_page()
    total_pages = (len(_events) + per_page - 1) // per_page
    remaining_after = max(0, total_pages - _current_page - 1)
    
    if remaining_after > 0:
        txt = f"{remaining_after}>"
        txt_w, txt_h = get_text_metrics(txt)
        x = region.x + (region.width - txt_w) // 2
        y = region.y + (region.height + txt_h) // 2 - 2
        
        display.set_pen(THEME["PAGINATION_COLOR"])
        vector.text(txt, int(x), int(y))        
    
def draw_status_region(region):
    """Draw network status."""
    display.set_pen(THEME["HEADER_BG_COLOR"])
    display.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
    
    global _network_status
    now = utime.time()
    
    if (not _network_status["connected"] or 
        (now - _network_status["last_error"] < 300)):
        
        x = region.x + region.width // 2
        y = region.y + region.height // 2 + 5
        
        if _network_status["error_count"] >= 3:
            display.set_pen(0xF800)  # Red
            vector.text("X", int(x), int(y))
        else:
            display.set_pen(0xFFE0)  # Yellow
            vector.text("!", int(x), int(y))
    
    elif (now - _network_status["last_success"] < 60 and 
          _network_status["error_count"] > 0):
        x = region.x + region.width // 2
        y = region.y + region.height // 2 + 5
        display.set_pen(0x07E0)  # Green
        vector.text("✓", int(x), int(y))

def draw_event_card(idx, e, y, now_ts):
    """Draw event card with theme-based colors."""
    display_tz = CONFIG.get("display_tz", CONFIG.get("default_tz", "America/Los_Angeles"))
    
    duration = e.dtend - e.dtstart
    start_ts = e.dtstart
    start_local = ical_parser.convert_to_local(start_ts, display_tz)
    end_local = ical_parser.convert_to_local(e.dtend, display_tz)
    
    sd, ed = start_local[:3], end_local[:3]
    multi_day = (sd != ed)
    all_day = not multi_day and duration >= 86400
    
    # Determine state
    now_display_tz = ical_parser.convert_to_local(now_ts, display_tz)
    now_display_ts = utime.mktime(now_display_tz[:6] + (0, 0, 0))
    start_display_ts = utime.mktime(start_local[:6] + (0, 0, 0))
    ongoing = (start_display_ts <= now_display_ts <= start_display_ts + duration)
              
    # Format time string
    if multi_day:
        time_str = format_time_tuple(start_local,'%m/%d') + "-" + format_time_tuple(end_local,'%m/%d') + " Multi-Day"
    elif all_day:
        time_str = format_time_tuple(start_local, '%m/%d') + " All-Day"
    else:
        duration_minutes = duration // 60
        time_str = format_time_tuple(start_local, '%m/%d %H:%M') + f" {duration_minutes}min"
        
    title = str(e.summary if e.summary is not None else 'Untitled')
    desc = e.description if e.description is not None else ''
    
    _, line_height = get_text_metrics()
    line_spacing = int(CONFIG.get("LINE_SPACING", 4))
    padding = int(CONFIG.get("CARD_PADDING", 8))
    
    # Include short description if available
    desc_lines = []
    if desc and len(desc) < 50:
        desc_text = desc[:40] + "…" if len(desc) > 40 else desc
        desc_lines = [desc_text]
    
    # Calculate card height
    total_lines = 2 + len(desc_lines)
    card_height = int(padding + (total_lines * line_height) + ((total_lines - 1) * line_spacing) + padding)
    
    # Check if event is canceled
    is_canceled = get_event_type_and_state(e, now_ts)['is_canceled']

    # Background and styling
    bg_pen = THEME["ROW_BG_COLOR_ODD"] if idx % 2 == 0 else THEME["ROW_BG_COLOR_EVEN"]

    if is_canceled:
        border_pen = THEME["PAST_ACCENT"]
        text_pen = THEME["EVENT_TEXT_CANCELED"]
        desc_pen = THEME["EVENT_DESC_CANCELED"]
    elif ongoing:
        border_pen = THEME["ONGOING_ACCENT"]
        text_pen = THEME["EVENT_TEXT_ONGOING"]
        desc_pen = THEME["EVENT_DESC_ONGOING"]
    elif all_day or multi_day:
        border_pen = THEME["ALLDAY_ACCENT"]
        text_pen = THEME["EVENT_TEXT_ALLDAY"]
        desc_pen = THEME["EVENT_DESC_ALLDAY"]
    elif start_ts > now_ts:
        border_pen = THEME["FUTURE_ACCENT"]
        text_pen = THEME["EVENT_TEXT_FUTURE"]
        desc_pen = THEME["EVENT_DESC_FUTURE"]
    else:
        border_pen = THEME["PAST_ACCENT"]
        text_pen = THEME["EVENT_TEXT_PAST"]
        desc_pen = THEME["EVENT_DESC_PAST"]
        
    # Draw background and borders
    display.set_pen(bg_pen)
    card_box = Polygon()
    card_box.rectangle(0, int(y), WIDTH, card_height)
    vector.draw(card_box)

    display.set_pen(border_pen)
    top_border = Polygon()
    top_border.rectangle(0, int(y), WIDTH, 2)
    vector.draw(top_border)
    bottom_border = Polygon()
    bottom_border.rectangle(0, int(y) + card_height - 2, WIDTH, 2)
    vector.draw(bottom_border)

    # Draw text
    text_x = 8
    current_y = int(y + padding + line_height)
    
    display.set_pen(text_pen)
    vector.text(time_str, text_x, current_y)
    
    current_y += line_height + line_spacing
    vector.text(title, text_x, current_y)

    if desc_lines:
        current_y += line_height + line_spacing
        display.set_pen(desc_pen)
        vector.text(desc_lines[0], text_x, current_y)

    return card_height + int(CONFIG.get("EVENT_SPACING", 2))

def draw_events_region(region):
    """Draw all events in the events region."""
    now_ts = utime.time()
    per_page = get_events_per_page()
    start_idx = _current_page * per_page
    end_idx = start_idx + per_page
    page_events = _events[start_idx:end_idx]
    
    y = int(region.y)
    for idx, ev in enumerate(page_events):
        event_height = draw_event_card(idx, ev, y, now_ts)
        y += int(event_height + CONFIG.get("EVENT_SPACING", 2))

def display_events_with_partial_updates(events, page, refresh_timestamp):
    """Display events using partial updates."""
    global _events, _current_page, _last_refresh

    _events = events
    _current_page = page
    _last_refresh = refresh_timestamp

    region_functions = {
        "pagination_left": draw_pagination_left_region,
        "header": draw_header_region,
        "refresh": draw_refresh_region,
        "memory": draw_memory_region,
        "status": draw_status_region,
        "pagination_right": draw_pagination_right_region,
        "events": draw_events_region,
    }

    # Check which regions need updating
    regions_to_update = []
    for region_name, region in REGIONS.items():
        content_hash = calculate_content_hash(region_name)
        if region.set_content_hash(content_hash) or region.is_dirty():
            regions_to_update.append(region_name)

    # Force full update if no events or many regions are dirty
    if not events or len(regions_to_update) >= 4:
        display.set_pen(THEME["BG_COLOR"])
        display.clear()
        mark_all_regions_dirty()
        regions_to_update = list(region_functions.keys())

    # Update only dirty regions
    for region_name in regions_to_update:
        if region_name in region_functions:
            update_region(region_name, region_functions[region_name])

    presto.update()
    gc.collect()
    
# === LED AND BACKLIGHT MANAGEMENT ===
def update_led_status(events, now_ts):
    """Update LED status based on current/upcoming events."""
    relevant_events = [e for e in events if not get_event_type_and_state(e, now_ts)['is_canceled']]
    
    if not relevant_events:
        presto.set_led_rgb(0, 0, 0, 0)
        presto.set_led_rgb(1, 0, 0, 0)
        return

    current_event = None
    next_timed_event = None
    
    # Check for ongoing event
    for event in relevant_events[:5]:
        event_info = get_event_type_and_state(event, now_ts)
        if event_info['ongoing']:
            current_event = event
            break
    
    # Find next timed event
    for event in relevant_events[:10]:
        event_info = get_event_type_and_state(event, now_ts)
        
        if not event_info['is_timed']:
            continue
            
        if event.dtstart > now_ts:
            if not next_timed_event or event.dtstart < next_timed_event.dtstart:
                next_timed_event = event

    if current_event:
        current_info = get_event_type_and_state(current_event, now_ts)
        
        if current_info['multi_day']:
            presto.set_led_hsv(0, 0.83, 1.0, 0.7)  # Purple
            presto.set_led_hsv(1, 0.83, 1.0, 0.7)
        elif current_info['all_day']:
            presto.set_led_hsv(0, 0.67, 1.0, 0.6)  # Blue
            presto.set_led_hsv(1, 0.67, 1.0, 0.6)
        else:
            presto.set_led_hsv(0, 0.33, 1.0, 0.8)  # Green
            presto.set_led_hsv(1, 0.33, 1.0, 0.8)
            
    elif next_timed_event:
        time_until = next_timed_event.dtstart - now_ts
        if time_until <= 900:  # 15 minutes
            brightness = 0.5 + 0.3 * ((utime.ticks_ms() // 500) % 2)
            presto.set_led_hsv(0, 0.0, 1.0, brightness)
            presto.set_led_hsv(1, 0.0, 1.0, brightness)
        elif time_until <= 3600:  # 1 hour
            presto.set_led_hsv(0, 0.08, 1.0, 0.6)  # Orange
            presto.set_led_hsv(1, 0.08, 1.0, 0.6)
        else:
            presto.set_led_hsv(0, 0.67, 0.8, 0.3)
            presto.set_led_hsv(1, 0.67, 0.8, 0.3)
    else:
        presto.set_led_rgb(0, 0, 0, 0)
        presto.set_led_rgb(1, 0, 0, 0)

def should_dim_backlight(events, now_ts):
    """Check if backlight should be dimmed."""
    for event in events[:5]:
        event_info = get_event_type_and_state(event, now_ts)
        
        if (event.dtend < now_ts or
            event_info['is_canceled'] or 
            not event_info['is_timed']):
            continue
        
        if event_info['ongoing']:
            return False
            
        if event.dtstart > now_ts and (event.dtstart - now_ts) <= 3600:
            return False
    
    return True

def update_backlight(events, now_ts):
    """Update backlight based on event status and activity."""
    global _backlight_dimmed, _backlight_check_interval
    
    if now_ts - _backlight_check_interval < 5:
        return
    _backlight_check_interval = now_ts
    
    should_dim = should_dim_backlight(events, now_ts)
    time_since_activity = now_ts - _last_activity
    
    if should_dim and time_since_activity > 30 and not _backlight_dimmed:
        try:
            presto.set_backlight(0.1)
            _backlight_dimmed = True
        except Exception as e:
            print(f"Backlight dim failed: {e}")
    elif (not should_dim or time_since_activity <= 30) and _backlight_dimmed:
        try:
            presto.set_backlight(1.0)
            _backlight_dimmed = False
        except Exception as e:
            print(f"Backlight brighten failed: {e}")

# === NETWORK STATUS TRACKING ===
def update_network_status(success, error_msg=""):
    """Track network status for display."""
    global _network_status
    now = utime.time()
    
    if success:
        _network_status["connected"] = True
        _network_status["last_success"] = now
        _network_status["error_count"] = 0
        mark_region_dirty("status")
    else:
        _network_status["connected"] = False
        _network_status["last_error"] = now
        _network_status["error_count"] += 1
        print(f"Network status: ERROR ({error_msg}) - Count: {_network_status['error_count']}")
        mark_region_dirty("status")

# === SCREEN SLEEP MANAGEMENT ===
def should_enter_screen_sleep():
    """Determine if device should enter screen sleep mode."""
    now = utime.time()
    hour = utime.localtime(now)[3]
    
    sleep_start = CONFIG.get("SLEEP_START_HOUR", 23)
    sleep_end = CONFIG.get("SLEEP_END_HOUR", 6)
    
    if sleep_start < sleep_end:
        in_sleep_hours = sleep_start <= hour < sleep_end
    else:
        in_sleep_hours = hour >= sleep_start or hour < sleep_end
    
    inactivity_timeout_minutes = CONFIG.get("SLEEP_INACTIVITY_MINUTES", 60)
    time_since_activity = now - _last_activity
    no_recent_activity = time_since_activity > (inactivity_timeout_minutes * 60)
    
    return in_sleep_hours and no_recent_activity

def enter_screen_sleep():
    """Enter screen sleep mode."""
    global _screen_sleep_state
    
    print("Entering screen sleep mode")
    _screen_sleep_state["active"] = True
    _screen_sleep_state["sleep_start_time"] = utime.time()
    
    presto.set_backlight(0.05)
    presto.set_led_rgb(0, 0, 0, 0)
    presto.set_led_rgb(1, 0, 0, 0)
    
    update_sleep_display()
    
def update_sleep_display():
    """Update sleep display with burn-in prevention."""
    display.set_pen(THEME["BG_COLOR"])
    display.clear()

    display.set_pen(_sleep_text_pen)
    
    # Calculate burn-in prevention offset
    now = utime.time()
    time_in_sleep = now - _screen_sleep_state["sleep_start_time"]
    offset_cycle = int(time_in_sleep // 180) % 4
    
    offset_x = (offset_cycle % 2) * 50 - 25
    offset_y = (offset_cycle // 2) * 30 - 15
    
    center_x = WIDTH // 2 + offset_x
    center_y = HEIGHT // 2 + offset_y
    
    sleep_text = "Sleep Mode"
    wake_text = "Touch to wake"
    
    sleep_w, _ = get_text_metrics(sleep_text)
    wake_w, _ = get_text_metrics(wake_text)
    char_h = get_text_metrics("M")[1]
    
    sleep_x = max(5, min(WIDTH - sleep_w - 5, center_x - sleep_w // 2))
    wake_x = max(5, min(WIDTH - wake_w - 5, center_x - wake_w // 2))
    sleep_y = max(char_h, min(HEIGHT - char_h * 3, center_y - char_h))
    wake_y = max(char_h * 2, min(HEIGHT - char_h, center_y + char_h))
    
    vector.text(sleep_text, sleep_x, sleep_y)
    vector.text(wake_text, wake_x, wake_y)
    presto.update()
    
def exit_screen_sleep():
    """Exit screen sleep mode."""
    global _screen_sleep_state

    if not _screen_sleep_state["active"]:
        return

    print("Exiting screen sleep mode")
    _screen_sleep_state["active"] = False

    mark_activity()
    presto.set_backlight(1.0)
    mark_all_regions_dirty()

    utime.sleep_ms(100)
    display_events_with_partial_updates(_events, _current_page, _last_refresh)
    gc.collect()

# === AUDIO AND ALERTS ===
def buzz(pattern):
    """Play buzzer pattern."""
    if in_quiet_hours(utime.time()):
        return
    for idx, duration in enumerate(pattern):
        if idx % 2 == 0:
            buzzer.set_tone(idx)
        else:
            buzzer.set_tone(-1)
        utime.sleep_ms(duration)
    buzzer.set_tone(-1)

def check_alerts(events, now_ts):
    """Check for event alerts."""
    for ev in events:
        if not hasattr(ev, "alert_fired"):
            ev.alert_fired = False

        if ev.alert_fired:
            continue

        event_info = get_event_type_and_state(ev, now_ts)

        if (event_info['is_canceled'] or not event_info['is_timed']): 
            continue

        start_ts = ev.dtstart
        if start_ts is None:
            continue

        offset = getattr(ev, "alert_offset", 300) 
        alert_time = start_ts - offset
        if now_ts >= alert_time:
            buzz(CONFIG.get("BUZZ_PATTERN", [200, 100, 200]))
            ev.alert_fired = True

# === TOUCH HANDLING ===
def animate_pull_to_refresh():
    """Animate pull-to-refresh action."""
    region = REGIONS["events"]

    refresh_frames = [
        "Refreshing.",
        "Refreshing..",
        "Refreshing...",
        "Loading events..."
    ]

    center_x = int(WIDTH // 2)
    center_y = int(region.y + region.height // 2)

    for i, frame in enumerate(refresh_frames):
        display.set_pen(THEME["BG_COLOR"])
        clear_rect = Polygon()
        clear_rect.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
        vector.draw(clear_rect)

        display.set_pen(THEME["TEXT_COLOR"])
        text_width, _ = get_text_metrics(frame)
        x = int(max(region.x, center_x - text_width // 2))
        y = int(center_y - 20)
        vector.text(frame, x, y)

        spinner_chars = ["|", "/", "-", "\\"]
        spinner_char = spinner_chars[i % 4]
        vector.text(spinner_char, center_x, int(center_y + 10))

        presto.update()
        utime.sleep_ms(300)

    display.set_pen(THEME["BG_COLOR"])
    clear_rect = Polygon()
    clear_rect.rectangle(int(region.x), int(region.y), int(region.width), int(region.height))
    vector.draw(clear_rect)

    display.set_pen(THEME["TEXT_COLOR"])
    final_text = "Updating..."
    text_width, _ = get_text_metrics(final_text)
    x = int(max(region.x, center_x - text_width // 2))
    vector.text(final_text, x, center_y)
    presto.update()
    utime.sleep_ms(200)

def handle_touch():
    """Handle touch input for navigation."""
    global _last_touch, _current_page, _events, _last_refresh, _backlight_dimmed
    global THEME, CONFIG, THEMES
    global _initial_touch_x, _initial_touch_y

    touch.poll()
    data = presto.touch_a

    if data is None or not isinstance(data, tuple) or len(data) != 3:
        _last_touch = None
        _initial_touch_x = None
        _initial_touch_y = None
        return

    x, y, state = data

    if state:  # Touch pressed
        if x < 0 or y < 0 or x > WIDTH or y > HEIGHT:
            _last_touch = None
            _initial_touch_x = None
            _initial_touch_y = None
            return

        current_read_ms = utime.ticks_ms()

        # Reduced debounce time for better responsiveness
        if _last_touch and _initial_touch_x is not None:
            time_since_initial = utime.ticks_diff(current_read_ms, _last_touch[2])
            if time_since_initial < 50:  # Reduced from 100ms to 50ms
                distance_from_initial = ((x - _initial_touch_x) ** 2 + (y - _initial_touch_y) ** 2) ** 0.5
                if distance_from_initial < 5:
                    return

        mark_activity()

        # Exit screen sleep mode
        if _screen_sleep_state["active"]:
            exit_screen_sleep()
            return

        # Restore backlight
        if _backlight_dimmed:
            try:
                presto.set_backlight(1.0)
                _backlight_dimmed = False
            except Exception as e:
                print(f"Error brightening backlight: {e}")

        # Initialize or update touch tracking - simplified logic
        if _last_touch is None or (len(_last_touch) >= 4 and _last_touch[3]):
            _initial_touch_x = x
            _initial_touch_y = y
            _last_touch = (x, y, current_read_ms, False)
        else:
            _last_touch = (x, y, _last_touch[2], _last_touch[3] if len(_last_touch) >= 4 else False)

    else:  # Touch released
        _last_touch = None
        _initial_touch_x = None
        _initial_touch_y = None
        return

    # Process gestures
    if _last_touch is None or _initial_touch_x is None or len(_last_touch) < 4:
        return

    x0, y0 = _initial_touch_x, _initial_touch_y
    t0 = _last_touch[2]
    refresh_started = _last_touch[3]

    if refresh_started:
        return

    dx, dy = x - x0, y - y0
    dt = utime.ticks_diff(current_read_ms, t0)
    abs_dx, abs_dy = abs(dx), abs(dy)

    # KEY FIX: Reduced minimum time for faster response
    min_time = 75   # Reduced from 150ms to 75ms
    min_distance = 25  # Slightly reduced from 30 to 25
    max_time = 2000

    if dt < min_time:
        return

    # Only reset if max time exceeded - more efficient
    if dt > max_time:
        _last_touch = (x, y, current_read_ms, False)
        _initial_touch_x = x
        _initial_touch_y = y
        return

    if abs_dx < min_distance and abs_dy < min_distance:
        return

    gesture_ratio = 2.0

    if abs_dx > abs_dy * gesture_ratio:
        # Horizontal swipe - pagination (OPTIMIZED)
        pages = (len(_events) + get_events_per_page() - 1) // get_events_per_page()
        if pages > 1:
            old_page = _current_page
            if dx < 0:  # Left swipe - next page
                _current_page = (_current_page + 1) % pages
            else:  # Right swipe - previous page
                _current_page = (_current_page - 1) % pages
            
            # Only update if page actually changed
            if old_page != _current_page:
                # OPTIMIZATION: Only mark essential regions dirty for pagination
                mark_region_dirty("events")
                mark_region_dirty("pagination_left")
                mark_region_dirty("pagination_right")
                # Remove header update - not needed for simple pagination
                display_events_with_partial_updates(_events, _current_page, _last_refresh)
        
        # Clear touch state immediately for next gesture
        _last_touch = None
        _initial_touch_x = None
        _initial_touch_y = None

    elif abs_dy > abs_dx * gesture_ratio:
        # Vertical swipe
        if dy > 0:  # Down swipe - refresh
            _last_touch = (x0, y0, t0, True)
            animate_pull_to_refresh()

            ical_parser.clear_cache()
            _events = load_events()
            # FIX: Use hasattr instead of dictionary access for alert_fired
            for e in _events:
                if not hasattr(e, "alert_fired"):
                    e.alert_fired = False
            _last_refresh = utime.time()

            mark_all_regions_dirty()
            display_events_with_partial_updates(_events, _current_page, _last_refresh)
            gc.collect()
            
        else:  # Up swipe - theme toggle
            current_theme = CONFIG.get("theme", "dark")
            new_theme = "light" if current_theme == "dark" else "dark"
            CONFIG["theme"] = new_theme

            # OPTIMIZATION: Call init_themes() directly instead of recreating theme pens
            init_themes()

            mark_all_regions_dirty()
            display_events_with_partial_updates(_events, _current_page, _last_refresh)

        # Clear touch state
        _last_touch = None
        _initial_touch_x = None
        _initial_touch_y = None

def main():
    """Main application loop."""
    global _events, _last_refresh

    # Show startup status
    display.set_pen(THEME["TEXT_COLOR"])
    vector.text("Loading calendar...", 10, 80)
    presto.update()

    # Load initial events
    try:
        _events = load_events()
        print(f"Loaded {len(_events)} events")
        for e in _events:
            if not hasattr(e, "alert_fired"):
                e.alert_fired = False
    except Exception as e:
        print(f"Initial load failed: {e}")
        _events = []

    _last_refresh = utime.time()
    mark_activity()

    display_events_with_partial_updates(_events, _current_page, _last_refresh)
    update_led_status(_events, _last_refresh)

    loop_counter = 0
    last_sleep_update = 0
    last_memory_update = 0

    while True:
        try:
            now_sec = utime.time()
            loop_counter += 1

            # Screen sleep mode handling
            if loop_counter % 240 == 0:
                if should_enter_screen_sleep() and not _screen_sleep_state["active"]:
                    enter_screen_sleep()

            # Handle screen sleep mode
            if _screen_sleep_state["active"]:
                handle_touch()

                if now_sec - last_sleep_update > 180:
                    update_sleep_display()
                    last_sleep_update = now_sec

                if _last_touch is None:
                    utime.sleep_ms(1000)
                else:
                    utime.sleep_ms(50)
                continue

            # Memory updates (if enabled)
            if CONFIG.get("SHOW_MEMORY", False) and (now_sec - last_memory_update) >= 10:
                mark_region_dirty("memory")
                update_region("memory", draw_memory_region)
                presto.update()
                last_memory_update = now_sec

            # Periodic refresh
            refresh_interval = get_adaptive_refresh_interval()

            if (now_sec - _last_refresh) >= refresh_interval:
                print(f"Periodic refresh starting (interval: {refresh_interval}s)...")
                try:
                    new_events = load_events()
                    if new_events:
                        _events = new_events
                        for e in _events:
                            if not hasattr(e, "alert_fired"):
                                e.alert_fired = False 
                        print(f"Refresh successful: {len(_events)} events")
                        update_network_status(True)
                    else:
                        print("Refresh returned no events, keeping existing")

                    _last_refresh = now_sec
                    mark_region_dirty("events")
                    mark_region_dirty("header")
                    mark_region_dirty("refresh")
                    mark_region_dirty("status")
                    mark_region_dirty("pagination_left")
                    mark_region_dirty("pagination_right")
                    display_events_with_partial_updates(_events, _current_page, _last_refresh)
                    gc.collect()
                except Exception as e:
                    print(f"Periodic refresh failed: {e}")
                    update_network_status(False, str(e))
                    gc.collect()

            # Update LEDs
            update_led_status(_events, now_sec)

            # Update backlight
            if loop_counter % 20 == 0:
                update_backlight(_events, now_sec)

            handle_touch()
            check_alerts(_events, now_sec)

            utime.sleep_ms(20)

        except Exception as e:
            print(f"Main loop error: {e}")
            utime.sleep_ms(1000)

# === INITIALIZATION ===
def init_device():
    """Initialize hardware and load config."""
    global _last_activity

    init_themes()
    init_regions()

    display.set_pen(THEME["BG_COLOR"])
    display.clear()

    display.set_pen(THEME["TEXT_COLOR"])
    vector.text("Loading...", 10, 50)
    presto.update()

    vector.set_font_size(CONFIG.get("FONT_HEIGHT", 8))
    _last_activity = utime.time()

    # Connect to WiFi
    try:
        wifi = presto.connect()
        print("WiFi connected")
    except ValueError as e:
        while True:
            vector.text(str(e), 10, 70)
            presto.update()
    except ImportError as e:
        while True:
            vector.text(str(e), 10, 70)
            presto.update()

    # Sync time
    try:
        import ntptime
        print("Attempting NTP time sync...")

        ntp_servers = [
            "time.nist.gov",
            "pool.ntp.org",
            "time.google.com",
            "time.cloudflare.com"
        ]

        time_synced = False
        for server in ntp_servers:
            try:
                print(f"Trying NTP server: {server}")
                if hasattr(ntptime, 'host'):
                    ntptime.host = server
                ntptime.timeout = 2
                ntptime.settime()
                print(f"Time synced successfully with {server}")
                time_synced = True
                update_network_status(True)
                break
            except Exception as server_error:
                print(f"NTP server {server} failed: {server_error}")
                update_network_status(False, f"NTP: {server_error}")
                utime.sleep(2)
                continue
            
        # Try worldtimeapi if NTP fails
        if not time_synced:        
            try:
                response = urequests.get("http://worldtimeapi.org/api/timezone/Etc/UTC")

                if response.status_code == 200:
                    data = response.json()
                    utc_timestamp = data["unixtime"]
                    new_time_tuple = utime.localtime(utc_timestamp)

                    rtc = RTC()
                    rtc.datetime((new_time_tuple[0], new_time_tuple[1], new_time_tuple[2], new_time_tuple[6] + 1,
                                  new_time_tuple[3], new_time_tuple[4], new_time_tuple[5], 0))

                    print("RTC time updated from worldtimeapi")
                    time_synced = True
                    update_network_status(True)
                else:
                    print(f"API returned status code {response.status_code}")

                response.close()

            except Exception as e:
                update_network_status(False, f"Worldtimeapi error: {e}")

        if not time_synced:
            print("All time sync methods failed, continuing with system time")
            update_network_status(False, "All time sync failed")

    except ImportError:
        print("ntptime module not available")
        update_network_status(False, "ntptime module missing")
    except Exception as e:
        print(f"Time sync failed: {e}")
        update_network_status(False, f"Time sync general: {e}")

    # Load timezone data
    print("Starting timezone loading...")
    try:
        ical_parser.load_timezone_data()
        gc.collect()
    except Exception as e:
        print(f"Timezone loading failed: {e}")

    # Clear loading message
    display.set_pen(THEME["BG_COLOR"])
    display.clear()
    presto.update()

if __name__ == "__main__":
    init_device()
    main()

