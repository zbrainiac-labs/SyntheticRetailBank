"""
Common utilities for all generators
"""
from datetime import datetime
from typing import List, Dict, Any
import csv
from pathlib import Path


class GeneratorUtils:
    """Common utilities for all generators"""
    
    @staticmethod
    def get_utc_timestamp() -> str:
        """Standardized UTC timestamp format used across all generators"""
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    @staticmethod
    def write_csv_safe(data: List[Any], filepath: Path, headers: List[str]) -> None:
        """Safe CSV writing with error handling"""
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                for row in data:
                    if hasattr(row, '__dict__'):
                        # Handle dataclass objects
                        writer.writerow([getattr(row, field) for field in headers])
                    else:
                        # Handle dict or list objects
                        writer.writerow(row)
        except Exception as e:
            raise Exception(f"Failed to write CSV file {filepath}: {e}")
    
    @staticmethod
    def get_headers_from_dataclass(dataclass_instance: Any) -> List[str]:
        """Extract headers from dataclass field names"""
        if hasattr(dataclass_instance, '__dataclass_fields__'):
            return list(dataclass_instance.__dataclass_fields__.keys())
        return []
