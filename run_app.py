#!/usr/bin/env python3
"""
Script to run the DLT StreamForge app with optimized debugging configuration.
This script helps avoid file watching loops and excessive debug output.
"""

import os
import sys
import logging

# Configure logging to reduce noise
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Suppress specific noisy loggers
logging.getLogger('watchdog').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('databricks.sdk').setLevel(logging.INFO)

# Set environment variables for better debugging
os.environ['DASH_DEBUG'] = 'true'
os.environ['FLASK_ENV'] = 'development'

# Import and run the app
if __name__ == "__main__":
    from app import app
    
    print("Starting DLT StreamForge App...")
    print("Debug mode enabled with optimized file watching")
    print("Access the app at: http://127.0.0.1:8050")
    print("Press Ctrl+C to stop the server")
    
    try:
        app.run(
            debug=True,
            host='0.0.0.0',
            port=8050,
            dev_tools_hot_reload=True,
            dev_tools_hot_reload_interval=2000,  # Increased interval
            dev_tools_hot_reload_watch_interval=2000,  # Increased interval
            dev_tools_hot_reload_max_retry=3,
            dev_tools_silence_routes_logging=True,
            dev_tools_ui=True,
            dev_tools_props_check=True,
            dev_tools_serve_dev_bundles=True,
            dev_tools_prune_errors=True,
        )
    except KeyboardInterrupt:
        print("\nShutting down server...")
    except Exception as e:
        print(f"Error starting server: {e}")
        sys.exit(1) 