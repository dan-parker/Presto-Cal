CONFIG = {
    # Core functionality
    "ical_url": "https://www.kayaposoft.com/enrico/ics/v2.0?country=can&fromDate=01-01-2025&toDate=31-12-2025&region=ab&holidayType=all&lang=en,
    "default_tz": "America/Los_Angeles",
    "display_tz": "America/Los_Angeles",
    "theme": "dark",
    "MAX_EVENTS": 40,
    "DAYS_TO_PARSE": 31,
    "events_per_page": 8,
    
    # Display settings
    "SHOW_MEMORY": True,
    "FONT_HEIGHT": 22,
    "LINE_SPACING": 4,
    "CARD_PADDING": 8,
    "EVENT_SPACING": 2,
    "TITLE_TEXT": "Holiday Calendar",
    "TITLE_FONT_HEIGHT": 22,
    "TITLE_PADDING_TOP": 5,
    "TITLE_PADDING_BOTTOM": 4,
    
    # Timing and behavior
    "QUIET_START_HOUR": 22,
    "QUIET_END_HOUR": 7,
    "REFRESH_INTERVAL": 900,  # 15 minutes
    "SLEEP_START_HOUR": 23,
    "SLEEP_END_HOUR": 6,
    "SLEEP_INACTIVITY_MINUTES": 60,
    
    # Hardware
    "BUZZ_PATTERN": [200, 100, 200],
    "buzzer_pin": 43,
}
