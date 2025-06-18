#!/usr/bin/env python3
"""
Script to run the DLT StreamForge app in production mode.
This script runs without debug mode to avoid file watching issues.
"""

import os
import sys
import logging

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Set environment variables for production
os.environ['DASH_DEBUG'] = 'false'
os.environ['FLASK_ENV'] = 'production'

# Import and run the app
if __name__ == "__main__":
    from app import app
    
    print("Starting DLT StreamForge App in Production Mode...")
    print("Access the app at: http://127.0.0.1:8050")
    print("Press Ctrl+C to stop the server")
    
    try:
        app.run(
            debug=False,
            host='0.0.0.0',
            port=8050,
            dev_tools_hot_reload=False,
            dev_tools_ui=False,
            dev_tools_props_check=False,
            dev_tools_serve_dev_bundles=False
        )
    except KeyboardInterrupt:
        print("\nShutting down server...")
    except Exception as e:
        print(f"Error starting server: {e}")
        sys.exit(1) 