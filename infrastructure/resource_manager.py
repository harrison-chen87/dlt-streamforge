import base64
import os
import json
import logging
import argparse
import requests
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ResourceManager:
    def __init__(self, config_path=None, databricks_host=None, databricks_token=None, warehouse_name=None):
        """
        Initialize the ResourceManager.
        
        Args:
            config_path (str): Path to the configuration file
            databricks_host (str): Databricks workspace URL
            databricks_token (str): Databricks access token
            warehouse_name (str): Custom name for the warehouse (optional)
        """
        self.config_path = config_path
        self.databricks_host = databricks_host
        self.databricks_token = databricks_token
        self.warehouse_name = warehouse_name
        self.resources_created = False
        self.warehouse_id = None
        self.cluster_id = None
        
        self.config = self._load_config(config_path)
        
        # Use provided credentials or fall back to environment variables
        self.host = databricks_host or os.getenv('DATABRICKS_HOST', '')
        self.token = databricks_token or os.getenv('DATABRICKS_TOKEN', '')
        
        if not self.host or not self.token:
            raise ValueError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN must be provided as parameters or set as environment variables."
            )
        
        # Ensure host URL is properly formatted
        if not self.host.startswith('http://') and not self.host.startswith('https://'):
            self.host = f'https://{self.host}'
        
        if not self.host.endswith('/'):
            self.host = f"{self.host}/"
        
        logger.info(f"Using host URL: {self.host}")
        
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
    def _get_current_timestamp(self):
        """Get current timestamp in readable format."""
        import datetime
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')

    def _load_config(self, config_path):
        """Load configuration from JSON file or use defaults"""
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        return {
            "warehouse": {
                "name": self.warehouse_name if self.warehouse_name else "Delta Drive Discovery Warehouse",
                "cluster_size": "X-Large",
                "min_clusters": 1,
                "max_clusters": 10,  # Updated to 10 clusters
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
            logger.info(f"Payload: {json.dumps(payload, indent=2)}")
            
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
            
            # Store the warehouse ID for cleanup tracking
            self.warehouse_id = str(warehouse_id)
            self.resources_created = True
            
            logger.info(f"Created warehouse with ID: {warehouse_id}")

            # Save resource ID to a file for later cleanup
            resource_ids = {
                "warehouse_id": str(warehouse_id),
                "created_at": warehouse_data.get('created_at')  # Store the creation timestamp
            }
            
            with open("resource_ids.json", "w") as f:
                json.dump(resource_ids, f)
            
            return resource_ids

        except Exception as e:
            logger.error(f"Error creating SQL Warehouse: {str(e)}")
            raise

    def _delete_warehouse(self, warehouse_id):
        """Delete a specific warehouse."""
        api_url = f"{self.host}api/2.0/sql/warehouses/{warehouse_id}"
        response = requests.delete(api_url, headers=self.headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to delete warehouse {warehouse_id}: {response.text}")
        
        logger.info(f"Deleted warehouse: {warehouse_id}")
    
    def _delete_cluster(self, cluster_id):
        """Delete a specific cluster."""
        api_url = f"{self.host}api/2.0/clusters/delete"
        payload = {"cluster_id": cluster_id}
        response = requests.post(api_url, headers=self.headers, json=payload)
        
        if response.status_code != 200:
            raise Exception(f"Failed to delete cluster {cluster_id}: {response.text}")
        
        logger.info(f"Deleted cluster: {cluster_id}")

    def cleanup_resources(self, warehouse_ids=None):
        """
        Clean up created resources.
        
        Args:
            warehouse_ids (list): Optional list of warehouse IDs to clean up. If None, cleans up all tracked resources.
        """
        try:
            if warehouse_ids:
                # Clean up specific warehouses
                for warehouse_id in warehouse_ids:
                    self._delete_warehouse(warehouse_id)
                    logger.info(f"Cleaned up warehouse: {warehouse_id}")
            else:
                # Clean up tracked resources
                if self.warehouse_id:
                    self._delete_warehouse(self.warehouse_id)
                    logger.info(f"Cleaned up warehouse: {self.warehouse_id}")
                
                if self.cluster_id:
                    self._delete_cluster(self.cluster_id)
                    logger.info(f"Cleaned up cluster: {self.cluster_id}")
                
                # Clean up DLT resources
                self.cleanup_dlt_resources()
                
                self.resources_created = False
                self.warehouse_id = None
                self.cluster_id = None
                
        except Exception as e:
            logger.error(f"Error cleaning up resources: {str(e)}")
            raise

    def get_warehouse_status(self, warehouse_id):
        """Get the status of a SQL Warehouse"""
        try:
            api_url = f"{self.host}api/2.0/sql/warehouses/{warehouse_id}"
            response = requests.get(api_url, headers=self.headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to get warehouse status: {response.text}")
            
            warehouse_data = response.json()
            
            # Debug: Log the full response to see what fields are available
            logger.debug(f"Warehouse API response: {json.dumps(warehouse_data, indent=2)}")
            
            # Extract and format the created_at timestamp
            created_at = warehouse_data.get('created_at')
            formatted_created_at = "N/A"
            
            if created_at:
                try:
                    # If it's a timestamp in milliseconds, convert to readable format
                    if isinstance(created_at, (int, float)):
                        import datetime
                        # Convert milliseconds to seconds if needed
                        if created_at > 1e10:  # Likely milliseconds
                            created_at = created_at / 1000
                        formatted_created_at = datetime.datetime.fromtimestamp(created_at).strftime('%Y-%m-%d %H:%M:%S UTC')
                    elif isinstance(created_at, str):
                        # If it's already a string, try to parse and format it
                        import datetime
                        try:
                            # Try parsing ISO format
                            dt = datetime.datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            formatted_created_at = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                        except:
                            # If parsing fails, use as-is
                            formatted_created_at = created_at
                except Exception as e:
                    logger.warning(f"Could not format created_at timestamp '{created_at}': {str(e)}")
                    formatted_created_at = str(created_at) if created_at else "N/A"
            
            return {
                'id': warehouse_data.get('id'),
                'name': warehouse_data.get('name'),
                'state': warehouse_data.get('state'),
                'size': warehouse_data.get('size'),
                'num_clusters': warehouse_data.get('num_clusters'),
                'auto_stop_mins': warehouse_data.get('auto_stop_mins'),
                'creator_name': warehouse_data.get('creator_name'),
                'created_at': formatted_created_at
            }
        except Exception as e:
            logger.error(f"Error getting warehouse status: {str(e)}")
            raise

    def create_dlt_pipeline(self, notebook_content, pipeline_name, volume_path, industry):
        """
        Create a DLT pipeline with file triggers in the customer's workspace.
        
        Args:
            notebook_content (str): Jupyter notebook content as JSON string
            pipeline_name (str): Name for the DLT pipeline
            volume_path (str): Path to the volume where data files arrive
            industry (str): Industry name for schema and tags
        
        Returns:
            dict: Response with pipeline ID, job ID, and status
        """
        try:
            import uuid
            import json
            
            # Step 1: Create/upload the notebook as a Python file
            notebook_api_url = f"{self.host}api/2.0/workspace/import"
            
            # Create a unique notebook path in the workspace
            notebook_path = f"/Workspace/Repos/StreamForge/{pipeline_name}_dlt_pipeline"
            
            # Convert notebook content to Python file content
            try:
                notebook_data = json.loads(notebook_content)
                python_content = ""
                
                # Extract Python code from notebook cells
                for cell in notebook_data.get('cells', []):
                    if cell.get('cell_type') == 'code':
                        source = cell.get('source', [])
                        if isinstance(source, list):
                            python_content += ''.join(source)
                        else:
                            python_content += str(source)
                        python_content += '\n\n'
                
                # If no Python content extracted, use the original notebook content
                if not python_content.strip():
                    python_content = notebook_content
                    
            except Exception as e:
                logger.warning(f"Could not parse notebook JSON, using as-is: {str(e)}")
                python_content = notebook_content
            
            notebook_payload = {
                "path": notebook_path,
                "format": "PYTHON",  # Upload as Python file, not JUPYTER
                "content": base64.b64encode(python_content.encode("utf-8")).decode("utf-8"),
                "overwrite": True
            }
            
            response = requests.post(notebook_api_url, headers=self.headers, json=notebook_payload)
            
            if response.status_code != 200:
                raise Exception(f"Failed to upload notebook: {response.text}")
            
            logger.info(f"Notebook uploaded successfully to: {notebook_path}")
            
            # Step 2: Create DLT pipeline with proper structure and file triggers
            pipeline_api_url = f"{self.host}api/2.0/pipelines"
            
            # Generate a unique pipeline ID
            pipeline_id = str(uuid.uuid4())
            
            # Create the pipeline payload with the structure specified
            pipeline_payload = {
                "id": pipeline_id,
                "pipeline_type": "WORKSPACE",
                "name": pipeline_name,
                "libraries": [
                    {
                        "glob": {
                            "include": notebook_path
                        }
                    }
                ],
                "schema": f"synthetic_{industry.lower().replace(' ', '_')}",
                "continuous": False,
                "development": True,
                "photon": True,
                "channel": "PREVIEW",
                "catalog": f"streamforge_{industry.lower().replace(' ', '_')}_catalog",
                "serverless": True,
                "tags": {
                    "Project": "StreamForge",
                    "Schema": f"{industry}-Generated",
                    "GeneratedBy": "DLT-StreamForge",
                    "VolumePath": volume_path
                },
                "root_path": f"/Workspace/Repos/StreamForge"
            }
            
            response = requests.post(pipeline_api_url, headers=self.headers, json=pipeline_payload)
            
            if response.status_code != 200:
                raise Exception(f"Failed to create DLT pipeline: {response.text}")
            
            pipeline_data = response.json()
            created_pipeline_id = pipeline_data.get('pipeline_id', pipeline_id)
            
            logger.info(f"DLT pipeline created successfully with ID: {created_pipeline_id}")
            
            # Step 3: Create a job to run the pipeline with file triggers
            job_api_url = f"{self.host}api/2.1/jobs/create"
            
            job_payload = {
                "name": f"{pipeline_name}_Job",
                "email_notifications": {
                    "on_success": [],
                    "on_failure": [],
                    "no_alert_for_skipped_runs": False
                },
                "timeout_seconds": 0,
                "max_concurrent_runs": 1,
                "tasks": [
                    {
                        "task_key": "dlt_pipeline_task",
                        "pipeline_task": {
                            "pipeline_id": created_pipeline_id
                        },
                        "timeout_seconds": 0,
                        "email_notifications": {},
                        "retry_on_timeout": False,
                        "max_retries": 0,
                        "min_retry_interval_millis": 0,
                        "retry_on_timeout": False
                    }
                ],
                "format": "MULTI_TASK",
                "tags": {
                    "Project": "StreamForge",
                    "Pipeline": pipeline_name,
                    "Industry": industry
                }
            }
            
            response = requests.post(job_api_url, headers=self.headers, json=job_payload)
            
            if response.status_code != 200:
                raise Exception(f"Failed to create job: {response.text}")
            
            job_data = response.json()
            job_id = job_data.get('job_id')
            
            logger.info(f"Job created successfully with ID: {job_id}")
            
            # Store the created resources for cleanup
            if not hasattr(self, 'dlt_resources'):
                self.dlt_resources = []
            
            self.dlt_resources.append({
                'pipeline_id': created_pipeline_id,
                'job_id': job_id,
                'notebook_path': notebook_path,
                'pipeline_name': pipeline_name
            })
            
            return {
                "status": "success",
                "pipeline_id": created_pipeline_id,
                "job_id": job_id,
                "notebook_path": notebook_path,
                "pipeline_name": pipeline_name,
                "volume_path": volume_path
            }
            
        except Exception as e:
            logger.error(f"Error creating DLT pipeline: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def cleanup_dlt_resources(self):
        """Clean up DLT pipelines, jobs, and notebooks."""
        try:
            if not hasattr(self, 'dlt_resources') or not self.dlt_resources:
                logger.info("No DLT resources to clean up")
                return
            
            for resource in self.dlt_resources:
                # Delete the job
                if 'job_id' in resource:
                    job_api_url = f"{self.host}api/2.1/jobs/delete"
                    job_payload = {"job_id": resource['job_id']}
                    response = requests.post(job_api_url, headers=self.headers, json=job_payload)
                    if response.status_code == 200:
                        logger.info(f"Deleted job: {resource['job_id']}")
                    else:
                        logger.warning(f"Failed to delete job {resource['job_id']}: {response.text}")
                
                # Delete the pipeline
                if 'pipeline_id' in resource:
                    pipeline_api_url = f"{self.host}api/2.0/pipelines/{resource['pipeline_id']}"
                    response = requests.delete(pipeline_api_url, headers=self.headers)
                    if response.status_code == 200:
                        logger.info(f"Deleted pipeline: {resource['pipeline_id']}")
                    else:
                        logger.warning(f"Failed to delete pipeline {resource['pipeline_id']}: {response.text}")
                
                # Delete the notebook (Python file)
                if 'notebook_path' in resource:
                    notebook_api_url = f"{self.host}api/2.0/workspace/delete"
                    notebook_payload = {"path": resource['notebook_path']}
                    response = requests.post(notebook_api_url, headers=self.headers, json=notebook_payload)
                    if response.status_code == 200:
                        logger.info(f"Deleted Python file: {resource['notebook_path']}")
                    else:
                        logger.warning(f"Failed to delete Python file {resource['notebook_path']}: {response.text}")
            
            # Clear the resources list
            self.dlt_resources = []
            logger.info("DLT resources cleanup completed")
            
        except Exception as e:
            logger.error(f"Error cleaning up DLT resources: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Gas Emissions Resource Manager")
    parser.add_argument("--cleanup", action="store_true", help="Clean up resources instead of creating them")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--host", help="Databricks host URL")
    parser.add_argument("--token", help="Databricks access token")
    parser.add_argument("--status", help="Check status of a specific warehouse ID")
    args = parser.parse_args()

    resource_manager = ResourceManager(
        databricks_host=args.host,
        databricks_token=args.token,
        config_path=args.config
    )
    
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