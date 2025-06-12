from datetime import datetime, timedelta
import random
from typing import Dict, List, Any

from .base_generator import BaseGenerator

class WeatherGenerator(BaseGenerator):
    """Generator for weather-related data with temperature, humidity, and other weather metrics."""
    
    def __init__(self, schema_config: Dict[str, Any]):
        super().__init__(schema_config)
        self.date_range = 30  # Default to generating 30 days of weather data
        
    def generate_records(self, num_records: int = None) -> List[Dict[str, Any]]:
        """Generate weather records for each site for the specified date range."""
        records = []
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=self.date_range)
        
        # Get unique site IDs from the dimension references
        site_ids = self.get_reference_values("site_id")
        
        # Generate weather data for each site and date
        current_date = start_date
        while current_date <= end_date:
            for site_id in site_ids:
                # Base temperature and humidity for consistency across hours
                base_temp = random.uniform(-10, 40)
                base_humidity = random.uniform(20, 90)
                
                record = {
                    "site_id": site_id,
                    "date": current_date,
                    "temperature_celsius": round(base_temp + random.uniform(-2, 2), 1),
                    "humidity_percentage": round(min(100, max(0, base_humidity + random.uniform(-5, 5))), 1),
                    "wind_speed_kmh": round(random.uniform(0, 50), 1),
                    "precipitation_mm": round(random.uniform(0, 25), 1),
                    "atmospheric_pressure": round(random.uniform(980, 1020), 1),
                    "weather_condition": random.choice([
                        "Clear", "Partly Cloudy", "Cloudy", "Rain", "Light Rain",
                        "Heavy Rain", "Thunderstorm", "Fog", "Mist"
                    ])
                }
                records.append(record)
            current_date += timedelta(days=1)
        
        return records

    def get_reference_values(self, field_name: str) -> List[str]:
        """Get reference values from dimension tables."""
        # This could be enhanced to read from actual dimension tables
        if field_name == "site_id":
            # Generate some sample site IDs if not provided
            return [f"SITE_{i:03d}" for i in range(1, 6)]
        return [] 