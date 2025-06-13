from datetime import datetime, timedelta
import random
from typing import Dict, List, Any
import pandas as pd
from faker import Faker
import logging

from .base_generator import BaseGenerator

logger = logging.getLogger(__name__)

class WeatherGenerator(BaseGenerator):
    """Generator for weather-related data with temperature, humidity, and other weather metrics."""
    
    def __init__(self, schema_path, output_base_path, is_local=True):
        super().__init__(schema_path, output_base_path, is_local=is_local)
        self.fake = Faker()
        
    def generate_data(self):
        """Generate weather records for each site."""
        records = []
        
        # Set date range to cover historical data
        end_date = datetime.now().date()
        start_date = datetime(1970, 1, 1).date()  # Start from 1970 to cover all possible emission dates
        logger.info(f"Generating weather data from {start_date} to {end_date}")
        
        # Get site IDs from dimension references or generate a reasonable range
        site_ids = []
        try:
            # Try to get site IDs from dimension references
            site_ids = [int(id) for id in range(1, 21)]  # Match site_info num_rows: 20
        except Exception as e:
            logger.warning(f"Could not get site IDs from references: {str(e)}")
            # Fallback to a reasonable range
            site_ids = list(range(1, 21))
        
        logger.info(f"Generating weather data for {len(site_ids)} sites")
        
        # Generate weather data for each site and date
        current_date = start_date
        while current_date <= end_date:
            # Adjust temperature ranges based on month to make it more realistic
            month = current_date.month
            if month in [12, 1, 2]:  # Winter
                temp_min, temp_max = -20, 10
            elif month in [3, 4, 5]:  # Spring
                temp_min, temp_max = 0, 25
            elif month in [6, 7, 8]:  # Summer
                temp_min, temp_max = 15, 40
            else:  # Fall
                temp_min, temp_max = 5, 30
            
            for site_id in site_ids:
                # Base temperature and humidity with seasonal variation
                base_temp = random.uniform(temp_min, temp_max)
                # Humidity tends to be higher in warmer months
                base_humidity = random.uniform(
                    40 if month in [6, 7, 8] else 20,  # Higher minimum in summer
                    90 if month in [6, 7, 8] else 70    # Higher maximum in summer
                )
                
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
            
            # Increment by one day
            current_date += timedelta(days=1)
            
            # Log progress every year to show the generator is working
            if current_date.day == 1 and current_date.month == 1:
                logger.info(f"Generated weather data up to: {current_date}")
        
        logger.info(f"Completed generating {len(records)} weather records")
        return pd.DataFrame(records)

    def get_reference_values(self, field_name: str) -> List[str]:
        """Get reference values from dimension tables."""
        # This could be enhanced to read from actual dimension tables
        if field_name == "site_id":
            # Generate some sample site IDs if not provided
            return [f"SITE_{i:03d}" for i in range(1, 6)]
        return [] 