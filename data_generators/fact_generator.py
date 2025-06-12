from .base_generator import BaseGenerator
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class FactGenerator(BaseGenerator):
    def __init__(self, schema_path, output_base_path, dimension_key_ranges, is_local=True):
        super().__init__(schema_path, output_base_path, is_local=is_local)
        self.fake = Faker()
        self.dimension_key_ranges = dimension_key_ranges
        
        # Load date range from schema configuration
        config = self.schema.get('generator_config', {})
        
        # Get start date from config or default to 1970-01-01
        start_date_str = config.get('start_date')
        if start_date_str:
            self.start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        else:
            self.start_date = datetime(1970, 1, 1)
        
        # Get end date from config or default to now
        end_date_str = config.get('end_date')
        if end_date_str and end_date_str.lower() != 'now':
            self.end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            self.end_date = datetime.now()
            
        logger.info(f"Configured date range for {self.schema.get('table', 'unknown')}: {self.start_date} to {self.end_date}")
        
    def _generate_value(self, col, col_def):
        """Generate a value based on column definition."""
        # Check if there are data quality rules for this column
        if 'data_quality_rules' in self.schema and col in self.schema['data_quality_rules']:
            rules = self.schema['data_quality_rules'][col]
            min_value = rules.get('min_value')
            max_value = rules.get('max_value')
            anomaly_percentage = rules.get('anomaly_percentage', 0)
            
            # Randomly decide if this value should be an anomaly
            if random.random() < anomaly_percentage:
                # Generate an anomalous value outside the normal range
                if random.random() < 0.5:  # 50% chance of being below min
                    value = min_value - random.uniform(0.1, 0.3)  # 10-30% below min
                else:  # 50% chance of being above max
                    value = max_value + random.uniform(0.1, 0.3)  # 10-30% above max
            else:
                # Generate a normal value within the range
                value = random.uniform(min_value, max_value)
            
            # Round to 2 decimal places for float values
            if isinstance(value, float):
                value = round(value, 2)
                
            return value
            
        # Special handling for datetime fields
        if isinstance(col_def, dict) and col_def.get('type') == 'datetime':
            return self.fake.date_time_between(
                start_date=self.start_date,
                end_date=self.end_date
            ).isoformat()
            
        # Use base implementation for all other types
        return super()._generate_value(col, col_def)

    def _generate_value_with_quality_rules(self, col, col_def):
        """Generate a value considering data quality rules if they exist."""
        value = self._generate_value(col, col_def)
        
        # Check if there are data quality rules for this column
        if 'data_quality_rules' in self.schema and col in self.schema['data_quality_rules']:
            rules = self.schema['data_quality_rules'][col]
            min_value = rules.get('min_value')
            max_value = rules.get('max_value')
            anomaly_percentage = rules.get('anomaly_percentage', 0)
            
            # Randomly decide if this value should be an anomaly
            if random.random() < anomaly_percentage:
                # Generate an anomalous value outside the normal range
                if random.random() < 0.5:  # 50% chance of being below min
                    value = min_value - random.uniform(0.1, 0.3)  # 10-30% below min
                else:  # 50% chance of being above max
                    value = max_value + random.uniform(0.1, 0.3)  # 10-30% above max
            else:
                # Generate a normal value within the range
                value = random.uniform(min_value, max_value)
            
            # Round to 2 decimal places for float values
            if isinstance(value, float):
                value = round(value, 2)
        
        return value
        
    def generate_data(self):
        """Generate fact table data."""
        rows = []
        num_rows = self.schema.get('num_rows', 10)
        
        for _ in range(num_rows):
            row = {}
            for col, col_def in self.schema['columns'].items():
                if col in self.dimension_key_ranges:
                    # Use dimension key ranges for foreign keys
                    row[col] = self.fake.random_int(min=1, max=self.dimension_key_ranges[col])
                elif 'data_quality_rules' in self.schema and col in self.schema['data_quality_rules']:
                    # Use quality rules if they exist for this column
                    row[col] = self._generate_value_with_quality_rules(col, col_def)
                else:
                    # Use standard value generation if no quality rules
                    row[col] = self._generate_value(col, col_def)
            rows.append(row)
            
        return pd.DataFrame(rows) 