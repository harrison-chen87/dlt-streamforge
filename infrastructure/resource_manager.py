import os
import json
import logging
import argparse
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ResourceManager:
    def __init__(self, config_path=None):
        self.workspace = WorkspaceClient()
        self.config = self._load_config(config_path)
        
    def _load_config(self, config_path):
        """Load configuration from JSON file or use defaults"""
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        return {
            "warehouse": {
                "name": "Gas Emissions Analytics Warehouse",
                "cluster_size": "X-Large",
                "min_clusters": 1,
                "max_clusters": 1,
                "auto_stop_mins": 60,
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
            # Create SQL Warehouse
            warehouse_response = self.workspace.warehouses.create(
                name=self.config["warehouse"]["name"],
                cluster_size=self.config["warehouse"]["cluster_size"],
                min_num_clusters=self.config["warehouse"]["min_clusters"],
                max_num_clusters=self.config["warehouse"]["max_clusters"],
                auto_stop_mins=self.config["warehouse"]["auto_stop_mins"],
                enable_photon=True,
                tags=self.config["warehouse"]["tags"]
            )
            
            # Extract warehouse ID from response
            warehouse_id = warehouse_response.get('id')
            if not warehouse_id:
                raise ValueError("No warehouse ID returned from creation request")
                
            logger.info(f"Created warehouse: {warehouse_id}")

            # Save resource ID to a file for later cleanup
            resource_ids = {
                "warehouse_id": warehouse_id
            }
            
            with open("resource_ids.json", "w") as f:
                json.dump(resource_ids, f)
            
            return resource_ids

        except Exception as e:
            logger.error(f"Error creating SQL Warehouse: {str(e)}")
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
            
            # Stop and delete the warehouse
            self.workspace.warehouses.delete(resource_ids["warehouse_id"])
            logger.info(f"Deleted warehouse: {resource_ids['warehouse_id']}")
            
            # Remove the resource IDs file
            if os.path.exists("resource_ids.json"):
                os.remove("resource_ids.json")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Gas Emissions Resource Manager")
    parser.add_argument("--cleanup", action="store_true", help="Clean up resources instead of creating them")
    parser.add_argument("--config", help="Path to configuration file")
    args = parser.parse_args()

    resource_manager = ResourceManager(config_path=args.config)
    
    try:
        if args.cleanup:
            resource_manager.cleanup_resources()
        else:
            resource_ids = resource_manager.create_resources()
            print("Created SQL Warehouse with ID:", resource_ids["warehouse_id"])
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 