#!/usr/bin/env python3
"""
Timezone Mapping Generator

A utility to generate comprehensive timezone mappings between Windows timezone names
and IANA timezone identifiers, including DST transition information.

This script:
1. Downloads the latest CLDR windowsZones.xml mapping from Unicode CLDR repository
2. Uses hardcoded fallback mappings for reliability when CLDR is unavailable  
3. Reads Windows timezone registry entries (Windows only)
4. Generates a CSV with timezone mappings and DST transition data
5. Includes both Windows-mapped and IANA-only timezones

Usage:
    # Generate timezone mappings CSV
    python timezone_mapper.py
    
    # Generate with custom output filename
    python timezone_mapper.py --output my_timezones.csv
    
    # Check for missing mappings and get update suggestions
    python timezone_mapper.py --update-suggestions

Maintenance:
    Run with --update-suggestions periodically to identify new Windows timezones
    that have been added to CLDR but aren't in the hardcoded fallback map.
    This helps keep the fallback mappings current for maximum reliability.

Author: Dan Parker
License: GPL v3
Version: 1.0.1
"""

import csv
import logging
import platform
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.request import urlretrieve
from urllib.error import URLError
import zoneinfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
CLDR_WINDOWS_ZONES_URL = (
    "https://raw.githubusercontent.com/unicode-org/cldr/main/common/supplemental/windowsZones.xml"
)

# Hardcoded fallback mapping for Windows time zones
# This serves as a reliable backup when CLDR data is unavailable or incomplete
# To keep this updated: run `python timezone_mapper.py --update-suggestions` 
# periodically to get code snippets for new mappings found in CLDR
WINDOWS_TZ_FALLBACK_MAP = {
    # North America
    "Dateline Standard Time": "Etc/GMT+12",
    "UTC-11": "Etc/GMT+11",
    "Aleutian Standard Time": "America/Adak",
    "Hawaiian Standard Time": "Pacific/Honolulu",
    "Marquesas Standard Time": "Pacific/Marquesas",
    "Alaskan Standard Time": "America/Anchorage",
    "UTC-09": "Etc/GMT+9",
    "Pacific Standard Time (Mexico)": "America/Tijuana",
    "Pacific Standard Time": "America/Los_Angeles",
    "US Pacific Standard Time": "America/Los_Angeles",
    "Mountain Standard Time (Mexico)": "America/Mazatlan",
    "Mountain Standard Time": "America/Denver",
    "US Mountain Standard Time": "America/Phoenix",
    "Central Standard Time (Mexico)": "America/Mexico_City",
    "Canada Central Standard Time": "America/Regina",
    "Central Standard Time": "America/Chicago",
    "US Central Standard Time": "America/Chicago",
    "SA Pacific Standard Time": "America/Bogota",
    "Eastern Standard Time (Mexico)": "America/Cancun",
    "Eastern Standard Time": "America/New_York",
    "US Eastern Standard Time": "America/New_York",
    "Atlantic Standard Time": "America/Halifax",
    "SA Western Standard Time": "America/La_Paz",
    "Newfoundland Standard Time": "America/St_Johns",
    "Turks And Caicos Standard Time": "America/Grand_Turk",

    # South America
    "SA Eastern Standard Time": "America/Cayenne",
    "Argentina Standard Time": "America/Argentina/Buenos_Aires",
    "E. South America Standard Time": "America/Sao_Paulo",
    "Paraguay Standard Time": "America/Asuncion",
    "Uruguay Standard Time": "America/Montevideo",
    "Venezuela Standard Time": "America/Caracas",
    "Magallanes Standard Time": "America/Punta_Arenas",

    # Europe / Africa
    "GMT Standard Time": "Europe/London",
    "Greenwich Standard Time": "Atlantic/Reykjavik",
    "Morocco Standard Time": "Africa/Casablanca",
    "W. Europe Standard Time": "Europe/Berlin",
    "Central Europe Standard Time": "Europe/Budapest",
    "Romance Standard Time": "Europe/Paris",
    "Central European Standard Time": "Europe/Warsaw",
    "E. Europe Standard Time": "Europe/Bucharest",
    "FLE Standard Time": "Europe/Helsinki",
    "Russian Standard Time": "Europe/Moscow",
    "Turkey Standard Time": "Europe/Istanbul",
    "Israel Standard Time": "Asia/Jerusalem",
    "Egypt Standard Time": "Africa/Cairo",
    "South Africa Standard Time": "Africa/Johannesburg",
    "Namibia Standard Time": "Africa/Windhoek",

    # Middle East / Asia
    "Arab Standard Time": "Asia/Riyadh",
    "Arabic Standard Time": "Asia/Baghdad",
    "Arabian Standard Time": "Asia/Dubai",
    "Iran Standard Time": "Asia/Tehran",
    "Syria Standard Time": "Asia/Damascus",
    "Jordan Standard Time": "Asia/Amman",
    "Middle East Standard Time": "Asia/Beirut",
    "Georgian Standard Time": "Asia/Tbilisi",
    "Azerbaijan Standard Time": "Asia/Baku",
    "Caucasus Standard Time": "Asia/Yerevan",
    "Pakistan Standard Time": "Asia/Karachi",
    "India Standard Time": "Asia/Kolkata",
    "Nepal Standard Time": "Asia/Kathmandu",
    "Bangladesh Standard Time": "Asia/Dhaka",
    "Myanmar Standard Time": "Asia/Yangon",
    "SE Asia Standard Time": "Asia/Bangkok",
    "North Asia East Standard Time": "Asia/Irkutsk",
    "North Asia Standard Time": "Asia/Krasnoyarsk",
    "Yakutsk Standard Time": "Asia/Yakutsk",
    "Vladivostok Standard Time": "Asia/Vladivostok",
    "Magadan Standard Time": "Asia/Magadan",
    "Kamchatka Standard Time": "Asia/Kamchatka",
    "Sakhalin Standard Time": "Asia/Sakhalin",
    "Korea Standard Time": "Asia/Seoul",
    "Tokyo Standard Time": "Asia/Tokyo",
    "China Standard Time": "Asia/Shanghai",
    "Singapore Standard Time": "Asia/Singapore",
    "Taipei Standard Time": "Asia/Taipei",
    "Hong Kong Standard Time": "Asia/Hong_Kong",
    "W. Mongolia Standard Time": "Asia/Hovd",

    # Australia / Pacific
    "AUS Eastern Standard Time": "Australia/Sydney",
    "AUS Central Standard Time": "Australia/Darwin",
    "AUS Western Standard Time": "Australia/Perth",
    "Tasmania Standard Time": "Australia/Hobart",
    "Lord Howe Standard Time": "Australia/Lord_Howe",
    "New Zealand Standard Time": "Pacific/Auckland",
    "Chatham Islands Standard Time": "Pacific/Chatham",
    "Fiji Standard Time": "Pacific/Fiji",
    "Tonga Standard Time": "Pacific/Tongatapu",
    "Samoa Standard Time": "Pacific/Apia",
    
    # Updates
    "Afghanistan Standard Time": "Asia/Kabul",
    "Altai Standard Time": "Asia/Barnaul",
    "Astrakhan Standard Time": "Europe/Astrakhan",
    "Aus Central W. Standard Time": "Australia/Eucla",
    "Azores Standard Time": "Atlantic/Azores",
    "Bahia Standard Time": "America/Bahia",
    "Belarus Standard Time": "Europe/Minsk",
    "Bougainville Standard Time": "Pacific/Bougainville",
    "Cape Verde Standard Time": "Atlantic/Cape_Verde",
    "Cen. Australia Standard Time": "Australia/Adelaide",
    "Central America Standard Time": "America/Guatemala",
    "Central Asia Standard Time": "Asia/Bishkek",
    "Central Brazilian Standard Time": "America/Cuiaba",
    "Central Pacific Standard Time": "Pacific/Guadalcanal",
    "Cuba Standard Time": "America/Havana",
    "E. Africa Standard Time": "Africa/Nairobi",
    "E. Australia Standard Time": "Australia/Brisbane",
    "Easter Island Standard Time": "Pacific/Easter",
    "Ekaterinburg Standard Time": "Asia/Yekaterinburg",
    "GTB Standard Time": "Europe/Bucharest",
    "Greenland Standard Time": "America/Godthab",
    "Haiti Standard Time": "America/Port-au-Prince",
    "Kaliningrad Standard Time": "Europe/Kaliningrad",
    "Libya Standard Time": "Africa/Tripoli",
    "Line Islands Standard Time": "Pacific/Kiritimati",
    "Mauritius Standard Time": "Indian/Mauritius",
    "Montevideo Standard Time": "America/Montevideo",
    "N. Central Asia Standard Time": "Asia/Novosibirsk",
    "Norfolk Standard Time": "Pacific/Norfolk",
    "North Korea Standard Time": "Asia/Pyongyang",
    "Omsk Standard Time": "Asia/Omsk",
    "Pacific SA Standard Time": "America/Santiago",
    "Qyzylorda Standard Time": "Asia/Qyzylorda",
    "Russia Time Zone 10": "Asia/Srednekolymsk",
    "Russia Time Zone 11": "Asia/Kamchatka",
    "Russia Time Zone 3": "Europe/Samara",
    "Saint Pierre Standard Time": "America/Miquelon",
    "Sao Tome Standard Time": "Africa/Sao_Tome",
    "Saratov Standard Time": "Europe/Saratov",
    "South Sudan Standard Time": "Africa/Juba",
    "Sri Lanka Standard Time": "Asia/Colombo",
    "Sudan Standard Time": "Africa/Khartoum",
    "Tocantins Standard Time": "America/Araguaina",
    "Tomsk Standard Time": "Asia/Tomsk",
    "Transbaikal Standard Time": "Asia/Chita",
    "UTC": "Etc/UTC",
    "UTC+12": "Etc/GMT-12",
    "UTC+13": "Etc/GMT-13",
    "UTC-02": "Etc/GMT+2",
    "UTC-08": "Etc/GMT+8",
    "Ulaanbaatar Standard Time": "Asia/Ulaanbaatar",
    "Volgograd Standard Time": "Europe/Volgograd",
    "W. Australia Standard Time": "Australia/Perth",
    "W. Central Africa Standard Time": "Africa/Lagos",
    "West Asia Standard Time": "Asia/Tashkent",
    "West Bank Standard Time": "Asia/Hebron",
    "West Pacific Standard Time": "Pacific/Port_Moresby",
    "Yukon Standard Time": "America/Whitehorse",    
}


class TimezoneMapper:
    """Main class for generating timezone mappings."""
    
    def __init__(self, output_file: str = "timezone_mappings.csv"):
        """Initialize the timezone mapper.
        
        Args:
            output_file: Name of the output CSV file
        """
        self.output_file = Path(output_file)
        self.xml_file = Path("windowsZones.xml")
        
    def download_cldr_data(self) -> bool:
        """Download the latest CLDR windowsZones.xml file.
        
        Returns:
            True if download successful, False otherwise
        """
        try:
            logger.info("Downloading latest windowsZones.xml from CLDR...")
            urlretrieve(CLDR_WINDOWS_ZONES_URL, self.xml_file)
            logger.info(f"Successfully downloaded to {self.xml_file}")
            return True
        except URLError as e:
            logger.error(f"Failed to download CLDR data: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading CLDR data: {e}")
            return False
    
    def parse_cldr_mapping(self) -> Dict[str, str]:
        """Parse the CLDR XML file to extract comprehensive Windows -> IANA mappings.
        
        This method extracts all Windows timezone mappings from CLDR, including:
        - Global default mappings (territory="001") 
        - Regional mappings for better coverage
        - Multiple IANA zones for a single Windows zone
        
        Returns:
            Dictionary mapping Windows timezone names to IANA identifiers
        """
        try:
            tree = ET.parse(self.xml_file)
            root = tree.getroot()
            
            # No namespace needed for CLDR XML
            mapping = {}
            regional_mappings = {}
            
            # First pass: collect all mappings
            for map_zone in root.findall(".//mapZone"):
                win_name = map_zone.attrib.get('other')
                territory = map_zone.attrib.get('territory')
                iana_zones = map_zone.attrib.get('type', '')
                
                if not win_name or not iana_zones:
                    continue
                
                # Split multiple IANA zones and take the first (primary) one
                primary_iana = iana_zones.split()[0] if iana_zones else ''
                
                if territory == "001":  # Global default
                    mapping[win_name] = primary_iana
                else:  # Regional mapping
                    if win_name not in regional_mappings:
                        regional_mappings[win_name] = []
                    regional_mappings[win_name].append((territory, primary_iana))
            
            # Second pass: fill gaps with regional mappings
            # Use regional mappings for Windows zones not in global defaults
            for win_name, regional_list in regional_mappings.items():
                if win_name not in mapping and regional_list:
                    # Prefer US mappings, then others
                    us_mapping = next((iana for territory, iana in regional_list if territory == "US"), None)
                    if us_mapping:
                        mapping[win_name] = us_mapping
                    else:
                        # Use the first available regional mapping
                        mapping[win_name] = regional_list[0][1]
            
            logger.info(f"Parsed {len(mapping)} timezone mappings from CLDR")
            logger.info(f"Found {len(regional_mappings)} Windows zones with regional variants")
            return mapping
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse CLDR XML: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error parsing CLDR mapping: {e}")
            return {}
    
    def get_windows_timezones(self) -> List[str]:
        """Get list of Windows timezone names from registry (Windows only).
        
        Returns:
            List of Windows timezone names, empty if not on Windows
        """
        if platform.system() != "Windows":
            logger.warning("Not running on Windows - skipping registry lookup")
            return []
        
        try:
            import winreg
            tz_list = []
            registry_path = r"SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time Zones"
            
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, registry_path) as tz_key:
                for i in range(winreg.QueryInfoKey(tz_key)[0]):
                    subkey_name = winreg.EnumKey(tz_key, i)
                    tz_list.append(subkey_name)
            
            logger.info(f"Found {len(tz_list)} Windows timezones in registry")
            return tz_list
            
        except ImportError:
            logger.error("winreg module not available")
            return []
        except Exception as e:
            logger.error(f"Failed to read Windows registry: {e}")
            return []
    
    def analyze_timezone_transitions(self, iana_zone: str) -> Tuple[int, int, Optional[Tuple[int, int, int]], Optional[Tuple[int, int, int]]]:
        """Analyze a timezone's standard/DST offsets and transition dates.
        
        Args:
            iana_zone: IANA timezone identifier
            
        Returns:
            Tuple of (std_offset, dst_offset, dst_start, dst_end)
            Offsets are in seconds, transitions are (month, day, hour) tuples
        """
        try:
            tz = zoneinfo.ZoneInfo(iana_zone)
            year = datetime.now().year
            
            # Get standard and DST offsets
            jan1 = datetime(year, 1, 1, tzinfo=tz)
            jul1 = datetime(year, 7, 1, tzinfo=tz)
            std_offset = int(jan1.utcoffset().total_seconds())
            dst_offset = int(jul1.utcoffset().total_seconds())
            
            # Find DST transition dates
            dst_start = dst_end = None
            dt = datetime(year, 1, 1)
            prev_offset = tz.utcoffset(dt)
            
            for day in range(1, 367):
                try:
                    dt = datetime(year, 1, 1) + timedelta(days=day)
                    offset = tz.utcoffset(dt)
                    
                    if offset != prev_offset:
                        if offset > prev_offset:  # Spring forward
                            dst_start = (dt.month, dt.day, dt.hour)
                        else:  # Fall back
                            dst_end = (dt.month, dt.day, dt.hour)
                        prev_offset = offset
                        
                except ValueError:
                    # Handle invalid dates (e.g., Feb 30)
                    continue
            
            return std_offset, dst_offset, dst_start, dst_end
            
        except Exception as e:
            logger.warning(f"Failed to analyze timezone {iana_zone}: {e}")
            return 0, 0, None, None
    
    def create_timezone_row(self, win_name: str, iana_zone: str) -> Dict[str, str]:
        """Create a CSV row for a timezone mapping.
        
        Args:
            win_name: Windows timezone name (empty string if none)
            iana_zone: IANA timezone identifier
            
        Returns:
            Dictionary representing a CSV row
        """
        if iana_zone:
            std_offset, dst_offset, dst_start, dst_end = self.analyze_timezone_transitions(iana_zone)
        else:
            std_offset = dst_offset = dst_start = dst_end = None
        
        return {
            "windows_timezone": win_name,
            "iana_timezone": iana_zone or "",
            "standard_offset_seconds": std_offset if std_offset is not None else "",
            "dst_offset_seconds": dst_offset if dst_offset is not None else "",
            "dst_start_month": dst_start[0] if dst_start else "",
            "dst_start_day": dst_start[1] if dst_start else "",
            "dst_start_hour": dst_start[2] if dst_start else "",
            "dst_end_month": dst_end[0] if dst_end else "",
            "dst_end_day": dst_end[1] if dst_end else "",
            "dst_end_hour": dst_end[2] if dst_end else "",
        }
    
    def write_csv(self, rows: List[Dict[str, str]]) -> None:
        """Write timezone data to CSV file.
        
        Args:
            rows: List of dictionaries representing CSV rows
        """
        fieldnames = [
            "windows_timezone", "iana_timezone", 
            "standard_offset_seconds", "dst_offset_seconds",
            "dst_start_month", "dst_start_day", "dst_start_hour",
            "dst_end_month", "dst_end_day", "dst_end_hour"
        ]
        
        try:
            with open(self.output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            
            logger.info(f"Successfully wrote {len(rows)} entries to {self.output_file}")
            
        except Exception as e:
            logger.error(f"Failed to write CSV file: {e}")
            raise
    
    def generate_mappings(self) -> None:
        """Generate the complete timezone mapping CSV file."""
        logger.info("Starting timezone mapping generation...")
        
        # Try to download and parse CLDR data
        cldr_map = {}
        if self.download_cldr_data():
            cldr_map = self.parse_cldr_mapping()
            if cldr_map:
                logger.info(f"Successfully loaded {len(cldr_map)} Windows timezone mappings from CLDR")
            else:
                logger.warning("Failed to parse CLDR mappings, will use fallback mappings only")
        else:
            logger.warning("Failed to download CLDR data, will use fallback mappings only")
        
        # Merge CLDR mappings with fallback mappings
        # CLDR takes precedence over fallback when available
        combined_map = WINDOWS_TZ_FALLBACK_MAP.copy()
        if cldr_map:
            # Update fallback with CLDR data
            combined_map.update(cldr_map)
            cldr_count = len(cldr_map)
            fallback_count = len(WINDOWS_TZ_FALLBACK_MAP)
            overlap_count = len(set(cldr_map.keys()) & set(WINDOWS_TZ_FALLBACK_MAP.keys()))
            new_from_cldr = len(set(cldr_map.keys()) - set(WINDOWS_TZ_FALLBACK_MAP.keys()))
            
            logger.info(f"Merged mappings: {len(combined_map)} total ({cldr_count} from CLDR, "
                       f"{fallback_count} fallback, {overlap_count} overlapping, {new_from_cldr} new from CLDR)")
            
            if new_from_cldr > 0:
                logger.info("CLDR provided additional mappings not in fallback - consider updating fallback map")
                new_mappings = {k: v for k, v in cldr_map.items() if k not in WINDOWS_TZ_FALLBACK_MAP}
                logger.info(f"New mappings from CLDR: {list(new_mappings.keys())[:10]}{'...' if len(new_mappings) > 10 else ''}")
        else:
            logger.info(f"Using fallback mappings only: {len(combined_map)} total")
        
        # Get Windows timezones from registry
        windows_timezones = self.get_windows_timezones()
        
        # If no Windows timezones found, use all known mappings
        if not windows_timezones:
            logger.info("Using all known Windows timezone mappings")
            windows_timezones = list(combined_map.keys())
        
        rows = []
        mapped_iana_zones: Set[str] = set()
        
        # Process Windows timezone mappings
        logger.info("Processing Windows timezone mappings...")
        unmapped_windows_zones = []
        for win_name in sorted(windows_timezones):
            iana_zone = combined_map.get(win_name)
            if iana_zone:
                mapped_iana_zones.add(iana_zone)
            else:
                logger.warning(f"No mapping found for Windows timezone: {win_name}")
                unmapped_windows_zones.append(win_name)
            
            row = self.create_timezone_row(win_name, iana_zone)
            rows.append(row)
        
        # Report unmapped zones for future fallback updates
        if unmapped_windows_zones:
            logger.warning(f"Found {len(unmapped_windows_zones)} unmapped Windows timezones:")
            for zone in unmapped_windows_zones[:20]:  # Show first 20
                logger.warning(f"  - {zone}")
            if len(unmapped_windows_zones) > 20:
                logger.warning(f"  ... and {len(unmapped_windows_zones) - 20} more")
            
            logger.info("To update the fallback map, consider researching these mappings and adding them to WINDOWS_TZ_FALLBACK_MAP")
        
        # Process remaining IANA-only timezones
        logger.info("Processing IANA-only timezones...")
        all_iana_zones = sorted(zoneinfo.available_timezones())
        iana_only_count = 0
        
        for iana_zone in all_iana_zones:
            if iana_zone not in mapped_iana_zones:
                row = self.create_timezone_row("", iana_zone)
                rows.append(row)
                iana_only_count += 1
        
        # Write results
        self.write_csv(rows)
        
        logger.info(f"Generation complete!")
        logger.info(f"Total entries: {len(rows)}")
        logger.info(f"Windows mappings: {len(windows_timezones)}")
        logger.info(f"IANA-only zones: {iana_only_count}")
        if unmapped_windows_zones:
            logger.warning(f"Unmapped Windows zones: {len(unmapped_windows_zones)}")
    
    def generate_fallback_update_suggestions(self) -> None:
        """Generate suggestions for updating the hardcoded fallback mapping.
        
        This method downloads the latest CLDR data and compares it with the
        hardcoded WINDOWS_TZ_FALLBACK_MAP to identify missing mappings.
        Use this periodically to keep the fallback map current.
        
        Usage: python timezone_mapper.py --update-suggestions
        """
        logger.info("Analyzing current mappings to suggest fallback updates...")
        
        if not self.download_cldr_data():
            logger.error("Cannot generate suggestions without CLDR data")
            return
            
        cldr_map = self.parse_cldr_mapping()
        if not cldr_map:
            logger.error("Cannot parse CLDR data")
            return
            
        # Find mappings in CLDR that aren't in our fallback
        missing_from_fallback = {k: v for k, v in cldr_map.items() 
                               if k not in WINDOWS_TZ_FALLBACK_MAP}
        
        # Also check for unmapped Windows registry zones
        windows_timezones = self.get_windows_timezones()
        combined_map = WINDOWS_TZ_FALLBACK_MAP.copy()
        combined_map.update(cldr_map)
        
        unmapped_registry_zones = [tz for tz in windows_timezones if tz not in combined_map]
        
        if missing_from_fallback:
            logger.info(f"Found {len(missing_from_fallback)} mappings in CLDR not in fallback:")
            logger.info("Consider adding these to WINDOWS_TZ_FALLBACK_MAP:")
            print("\n# Additional mappings found in CLDR:")
            for win_name, iana_zone in sorted(missing_from_fallback.items()):
                print(f'    "{win_name}": "{iana_zone}",')
        
        if unmapped_registry_zones:
            logger.warning(f"Found {len(unmapped_registry_zones)} Windows registry zones not in CLDR or fallback:")
            for zone in unmapped_registry_zones:
                logger.warning(f"  - {zone}")
            logger.info("These may be deprecated/obsolete timezones that need manual research")
            logger.info("Check Microsoft documentation or historical timezone data for mappings")
        
        if not missing_from_fallback and not unmapped_registry_zones:
            logger.info("Fallback map appears complete relative to current CLDR data and Windows registry")
        
        # Summary
        total_coverage = len(set(windows_timezones) - set(unmapped_registry_zones))
        total_zones = len(windows_timezones) if windows_timezones else len(combined_map)
        logger.info(f"Coverage: {total_coverage}/{total_zones} Windows timezones ({100*total_coverage/total_zones:.1f}%)")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate comprehensive timezone mappings between Windows and IANA timezones"
    )
    parser.add_argument(
        "--output", "-o", 
        default="timezone_mappings.csv",
        help="Output CSV filename (default: timezone_mappings.csv)"
    )
    parser.add_argument(
        "--update-suggestions", 
        action="store_true",
        help="Generate suggestions for updating the hardcoded fallback map"
    )
    
    args = parser.parse_args()
    
    try:
        mapper = TimezoneMapper(args.output)
        
        if args.update_suggestions:
            mapper.generate_fallback_update_suggestions()
        else:
            mapper.generate_mappings()
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
