#!/usr/bin/env python3
"""
Timezone Mapping Blob Generator

This utility reads timezone mapping data from a CSV file and generates a binary blob
containing packed timezone information. The blob format includes both Windows timezone
names and IANA timezone identifiers with their corresponding DST rules and offsets.

Usage:
    # Generate timezone blob with default input timezone_mappings.csv and output tzid_blob.bin
    python timezone_blob_generator.py
    
    # Generate with custom input and output filenames
    python timezone_blob_generator.py -i timezones.csv -o blob.bin
    
    # Check commands
    python timezone_blob_generator.py --help

Author: Dan Parker
License: GPL v3
Version: 1.0.0
"""

import csv
import struct
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional


# Configuration constants
DEFAULT_CSV_FILE = "timezone_mappings.csv"
DEFAULT_BLOB_FILE = "tzid_blob.bin"

# Expected CSV headers (updated format)
EXPECTED_HEADERS = {
    'windows_timezone', 'iana_timezone', 'standard_offset_seconds', 
    'dst_offset_seconds', 'dst_start_month', 'dst_start_day', 
    'dst_start_hour', 'dst_end_month', 'dst_end_day', 'dst_end_hour'
}

# Binary format: name_length(1B) + name(variable) + std_offset(4B) + dst_offset(4B) + dst_fields(6B)
TIMEZONE_STRUCT_FORMAT = "<B{}siiBBBBBB"


class TimezoneData:
    """Represents timezone data with offsets and DST rules."""
    
    def __init__(self, std_offset: int, dst_offset: int, 
                 dst_start_month: int, dst_start_day: int, dst_start_hour: int,
                 dst_end_month: int, dst_end_day: int, dst_end_hour: int):
        self.std_offset = std_offset
        self.dst_offset = dst_offset
        self.dst_start_month = dst_start_month
        self.dst_start_day = dst_start_day
        self.dst_start_hour = dst_start_hour
        self.dst_end_month = dst_end_month
        self.dst_end_day = dst_end_day
        self.dst_end_hour = dst_end_hour
    
    def to_tuple(self) -> Tuple[int, ...]:
        """Convert to tuple for struct packing."""
        return (
            self.std_offset, self.dst_offset,
            self.dst_start_month, self.dst_start_day, self.dst_start_hour,
            self.dst_end_month, self.dst_end_day, self.dst_end_hour
        )


class TimezoneBlobGenerator:
    """Generates binary timezone mapping blobs from CSV data."""
    
    def __init__(self, csv_file: str = DEFAULT_CSV_FILE, blob_file: str = DEFAULT_BLOB_FILE):
        self.csv_file = Path(csv_file)
        self.blob_file = Path(blob_file)
        self.seen_names: Set[str] = set()
        self.records: List[bytes] = []
    
    def validate_csv_headers(self, reader: csv.DictReader) -> bool:
        """Validate that the CSV has expected headers."""
        csv_headers = set(reader.fieldnames or [])
        if not EXPECTED_HEADERS.issubset(csv_headers):
            missing = EXPECTED_HEADERS - csv_headers
            print(f"Error: CSV missing required headers: {missing}", file=sys.stderr)
            return False
        return True
    
    def parse_int_field(self, value: str, field_name: str, default: int = 0) -> int:
        """Safely parse integer field from CSV with error handling."""
        if not value or value.strip() == '':
            return default
        
        try:
            return int(value.strip())
        except ValueError as e:
            print(f"Warning: Invalid {field_name} value '{value}', using {default}", file=sys.stderr)
            return default
    
    def parse_timezone_row(self, row: Dict[str, str]) -> Optional[TimezoneData]:
        """Parse a CSV row into TimezoneData object."""
        try:
            std_offset = self.parse_int_field(row["standard_offset_seconds"], "standard_offset_seconds")
            dst_offset = self.parse_int_field(row["dst_offset_seconds"], "dst_offset_seconds")
            dst_start_month = self.parse_int_field(row["dst_start_month"], "dst_start_month")
            dst_start_day = self.parse_int_field(row["dst_start_day"], "dst_start_day")
            dst_start_hour = self.parse_int_field(row["dst_start_hour"], "dst_start_hour")
            dst_end_month = self.parse_int_field(row["dst_end_month"], "dst_end_month")
            dst_end_day = self.parse_int_field(row["dst_end_day"], "dst_end_day")
            dst_end_hour = self.parse_int_field(row["dst_end_hour"], "dst_end_hour")
            
            return TimezoneData(
                std_offset, dst_offset,
                dst_start_month, dst_start_day, dst_start_hour,
                dst_end_month, dst_end_day, dst_end_hour
            )
        except Exception as e:
            print(f"Error parsing timezone row: {e}", file=sys.stderr)
            return None
    
    def add_timezone_entry(self, name: str, tz_data: TimezoneData) -> bool:
        """Add a timezone entry to the blob if not already present."""
        if not name or name in self.seen_names:
            return False
        
        self.seen_names.add(name)
        
        # Encode name as UTF-8
        name_bytes = name.encode("utf-8")
        
        # Validate name length (must fit in 1 byte)
        if len(name_bytes) > 255:
            print(f"Warning: Timezone name too long, skipping: {name}", file=sys.stderr)
            return False
        
        # Pack binary data
        try:
            packed = struct.pack(
                TIMEZONE_STRUCT_FORMAT.format(len(name_bytes)),
                len(name_bytes),
                name_bytes,
                *tz_data.to_tuple()
            )
            self.records.append(packed)
            return True
        except struct.error as e:
            print(f"Error packing timezone data for '{name}': {e}", file=sys.stderr)
            return False
    
    def process_csv_file(self) -> bool:
        """Process the CSV file and extract timezone data."""
        if not self.csv_file.exists():
            print(f"Error: CSV file not found: {self.csv_file}", file=sys.stderr)
            return False
        
        try:
            with open(self.csv_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                
                if not self.validate_csv_headers(reader):
                    return False
                
                row_count = 0
                for row in reader:
                    row_count += 1
                    tz_data = self.parse_timezone_row(row)
                    if not tz_data:
                        continue
                    
                    added_count = 0
                    
                    # Add Windows timezone name if present
                    windows_tz = row.get("windows_timezone", "").strip()
                    if windows_tz:
                        if self.add_timezone_entry(windows_tz, tz_data):
                            added_count += 1
                    
                    # Add IANA timezone name if present
                    iana_tz = row.get("iana_timezone", "").strip()
                    if iana_tz:
                        if self.add_timezone_entry(iana_tz, tz_data):
                            added_count += 1
                    
                    # Log when both names are added for the same timezone
                    if added_count == 2:
                        print(f"Added both: '{windows_tz}' and '{iana_tz}'")
                
                print(f"Processed {row_count} rows from CSV file")
                return True
                
        except Exception as e:
            print(f"Error processing CSV file: {e}", file=sys.stderr)
            return False
    
    def write_blob(self) -> bool:
        """Write the binary blob to file."""
        if not self.records:
            print("Warning: No timezone records to write", file=sys.stderr)
            return False
        
        try:
            blob = b"".join(self.records)
            with open(self.blob_file, "wb") as f:
                f.write(blob)
            
            print(f"Successfully wrote {len(self.records)} timezone entries to {self.blob_file}")
            print(f"Blob size: {len(blob)} bytes")
            print(f"Unique timezone names: {len(self.seen_names)}")
            
            return True
            
        except Exception as e:
            print(f"Error writing blob file: {e}", file=sys.stderr)
            return False
    
    def print_summary(self) -> None:
        """Print a summary of processed timezones."""
        if not self.seen_names:
            return
        
        print("\nSample timezone names added:")
        sample_names = sorted(list(self.seen_names))[:10]
        for name in sample_names:
            tz_type = "IANA" if "/" in name else "Windows"
            print(f"  [{tz_type}] {name}")
        
        if len(self.seen_names) > 10:
            print(f"  ... and {len(self.seen_names) - 10} more")
    
    def generate(self) -> bool:
        """Main generation process."""
        print(f"Generating timezone blob from {self.csv_file}")
        
        if not self.process_csv_file():
            return False
        
        if not self.write_blob():
            return False
        
        self.print_summary()
        return True


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate binary timezone mapping blob from CSV data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  %(prog)s                                    # Use default files
  %(prog)s -i timezones.csv -o blob.bin      # Specify custom files
  %(prog)s --help                            # Show this help
        """
    )
    
    parser.add_argument(
        "-i", "--input", 
        default=DEFAULT_CSV_FILE,
        help=f"Input CSV file (default: {DEFAULT_CSV_FILE})"
    )
    
    parser.add_argument(
        "-o", "--output",
        default=DEFAULT_BLOB_FILE, 
        help=f"Output binary blob file (default: {DEFAULT_BLOB_FILE})"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0"
    )
    
    args = parser.parse_args()
    
    # Create and run generator
    generator = TimezoneBlobGenerator(args.input, args.output)
    
    if generator.generate():
        print("\nBlob generation completed successfully!")
        sys.exit(0)
    else:
        print("\nBlob generation failed!", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
