#Some changes

import os
import json
import logging
import argparse
import requests
from urllib.parse import urljoin
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
logger.info("Loading environment variables from .env file...")
load_dotenv()
logger.info(f"Current working directory: {os.getcwd()}")
logger.info(f"Environment variables loaded: DATABRICKS_HOST={os.getenv('DATABRICKS_HOST')}, DATABRICKS_TOKEN={'*' * 10 if os.getenv('DATABRICKS_TOKEN') else 'Not found'}")

class ResourceManager:
    def __init__(self, config_path=None):
        self.config = self._load_config(config_path)
        
        # Force token-based authentication
        os.environ['DATABRICKS_AUTH_TYPE'] = 'pat'
        
        # Get and validate host URL
        host = os.getenv('DATABRICKS_HOST', '')
        if not host.startswith('http://') and not host.startswith('https://'):
            host = f'https://{host}'
        self.host = host
        
        self.token = os.getenv('DATABRICKS_TOKEN')
        
        if not self.host or not self.token:
            raise ValueError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN must be set in the .env file. "
                "Example .env file contents:\n"
                "DATABRICKS_HOST=https://your-workspace.cloud.databricks.com\n"
                "DATABRICKS_TOKEN=your-access-token"
            )
        
        # Ensure host URL ends with a slash
        if not self.host.endswith('/'):
            self.host = f"{self.host}/"
        
        logger.info(f"Using host URL: {self.host}")
        
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
    def _load_config(self, config_path):
        """Load configuration from JSON file or use defaults"""
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        return {
            "warehouse": {
                "name": "Gas Emissions Analytics Warehouse",
                "cluster_size": "X-Large",  # Maximum size for serverless
                "min_clusters": 1,
                "max_clusters": 2,  # Using 2 clusters for maximum compute power
                "auto_stop_mins": 480,  # 8 hours
                "tags": {
                    "Project": "Gas-Emissions",
                    "Environment": "Production"
                }
            }
        }

    def create_resources(self):
        """Create SQL Warehouse for the Gas Emissions project"""
        logger.info("Starting SQL Warehouse creation...")
        
        try:
            # Note: We are using serverless compute (PRO warehouse type) instead of classic compute
            # because the workspace lacks the necessary AWS IAM permissions to launch EC2 instances.
            # The error "UnauthorizedOperation: You are not authorized to perform this operation"
            # indicates that the Databricks workspace's AWS role doesn't have permissions to:
            # 1. Launch EC2 instances
            # 2. Create and manage VPC resources
            # 3. Manage security groups and network interfaces
            
            # Prepare warehouse creation payload
            payload = {
                "name": self.config["warehouse"]["name"],
                "cluster_size": self.config["warehouse"]["cluster_size"],
                "min_num_clusters": self.config["warehouse"]["min_clusters"],
                "max_num_clusters": self.config["warehouse"]["max_clusters"],
                "auto_stop_mins": self.config["warehouse"]["auto_stop_mins"],
                "warehouse_type": "PRO",
                "channel": {
                    "name": "CHANNEL_NAME_CURRENT"
                },
                "tags": self.config["warehouse"]["tags"]
            }
            
            # Create SQL Warehouse using REST API
            logger.info("Sending warehouse creation request...")
            api_url = f"{self.host}api/2.0/sql/warehouses"
            logger.info(f"API URL: {api_url}")
            logger.info(f"Payload: {json.dumps(payload, indent=2)}")  # Log the payload for debugging
            
            response = requests.post(
                api_url,
                headers=self.headers,
                json=payload
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to create warehouse: {response.text}")
            
            warehouse_data = response.json()
            warehouse_id = warehouse_data.get('id')
            
            if not warehouse_id:
                raise ValueError("No warehouse ID in response")
            
            logger.info(f"Created warehouse with ID: {warehouse_id}")

            # Save resource ID to a file for later cleanup
            resource_ids = {
                "warehouse_id": str(warehouse_id)
            }
            
            with open("resource_ids.json", "w") as f:
                json.dump(resource_ids, f)
            
            return resource_ids

        except Exception as e:
            logger.error(f"Error creating SQL Warehouse: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            logger.error(f"Error details: {e.__dict__ if hasattr(e, '__dict__') else 'No details available'}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise

    def cleanup_resources(self, resource_ids=None):
        """Clean up SQL Warehouse"""
        logger.info("Starting resource cleanup...")
        
        try:
            # Load resource ID from file if not provided
            if resource_ids is None and os.path.exists("resource_ids.json"):
                with open("resource_ids.json", "r") as f:
                    resource_ids = json.load(f)
            
            if not resource_ids or "warehouse_id" not in resource_ids:
                logger.error("No warehouse ID found for cleanup")
                return
            
            # Delete the warehouse using REST API
            api_url = f"{self.host}api/2.0/sql/warehouses/{resource_ids['warehouse_id']}"
            logger.info(f"API URL: {api_url}")
            
            response = requests.delete(
                api_url,
                headers=self.headers
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to delete warehouse: {response.text}")
            
            logger.info(f"Deleted warehouse: {resource_ids['warehouse_id']}")
            
            # Remove the resource IDs file
            if os.path.exists("resource_ids.json"):
                os.remove("resource_ids.json")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            raise

    def get_warehouse_status(self, warehouse_id):
        """Get the status of a SQL Warehouse"""
        try:
            api_url = f"{self.host}api/2.0/sql/warehouses/{warehouse_id}"
            response = requests.get(api_url, headers=self.headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to get warehouse status: {response.text}")
            
            warehouse_data = response.json()
            return {
                'id': warehouse_data.get('id'),
                'name': warehouse_data.get('name'),
                'state': warehouse_data.get('state'),
                'size': warehouse_data.get('size'),
                'num_clusters': warehouse_data.get('num_clusters'),
                'auto_stop_mins': warehouse_data.get('auto_stop_mins'),
                'creator_name': warehouse_data.get('creator_name'),
                'created_at': warehouse_data.get('created_at')
            }
        except Exception as e:
            logger.error(f"Error getting warehouse status: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Gas Emissions Resource Manager")
    parser.add_argument("--cleanup", action="store_true", help="Clean up resources instead of creating them")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--status", help="Check status of a specific warehouse ID")
    args = parser.parse_args()

    resource_manager = ResourceManager(config_path=args.config)
    
    try:
        if args.status:
            status = resource_manager.get_warehouse_status(args.status)
            print("\nWarehouse Status:")
            print(f"ID: {status['id']}")
            print(f"Name: {status['name']}")
            print(f"State: {status['state']}")
            print(f"Size: {status['size']}")
            print(f"Number of Clusters: {status['num_clusters']}")
            print(f"Auto-stop after: {status['auto_stop_mins']} minutes")
            print(f"Created by: {status['creator_name']}")
            print(f"Created at: {status['created_at']}")
        elif args.cleanup:
            resource_manager.cleanup_resources()
        else:
            resource_ids = resource_manager.create_resources()
            warehouse_id = resource_ids["warehouse_id"]
            print("\nCreated SQL Warehouse with ID:", warehouse_id)
            
            # Check and display initial status
            print("\nChecking warehouse status...")
            status = resource_manager.get_warehouse_status(warehouse_id)
            print(f"State: {status['state']}")
            print(f"Size: {status['size']}")
            print(f"Auto-stop after: {status['auto_stop_mins']} minutes")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 