# DLT StreamForge

A comprehensive data generation and Delta Live Tables (DLT) pipeline deployment tool for Databricks. This application generates synthetic data for various industries and creates DLT pipelines with file arrival triggers.

## Features

- **Multi-Industry Data Generation**: Generate synthetic data for Energy, Manufacturing, Healthcare, and more
- **DLT Pipeline Creation**: Automatically create DLT pipelines with file arrival triggers
- **SQL Warehouse Management**: Create and manage multiple SQL warehouses
- **Real-time Data Streaming**: Generate continuous data streams with configurable intervals
- **Infrastructure Management**: Comprehensive resource creation and cleanup

## File Watching Issue Fix

The app previously experienced infinite debug loops due to Dash's file watcher monitoring the virtual environment directory. This has been resolved with:

1. **File Watcher Exclusions**: Configured to exclude `.venv/`, `__pycache__/`, and other problematic directories
2. **Optimized Debug Configuration**: Reduced file watching intervals and added comprehensive exclusions
3. **Multiple Run Modes**: Different scripts for development and production environments

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Application

### Option 1: Development Mode (with optimized debugging)
```bash
python run_app.py
```
- Includes hot reload for development
- Optimized file watching to avoid loops
- Reduced debug noise

### Option 2: Production Mode (no debugging)
```bash
python run_production.py
```
- No file watching
- No debug output
- Best for production deployments

### Option 3: Direct Run (legacy)
```bash
python app.py
```
- Basic debug mode
- May experience file watching issues

## Configuration

### Environment Variables
- `DASH_DEBUG`: Set to 'true' for debug mode, 'false' for production
- `FLASK_ENV`: Set to 'development' or 'production'

### File Watcher Exclusions
The app automatically excludes these directories from file watching:
- `.venv/`, `venv/`, `env/` (Virtual environments)
- `__pycache__/`, `*.pyc` (Python cache)
- `*.csv`, `*.parquet`, `*.json` (Generated data files)
- `schema/` (Large YAML schema files)
- Various system and IDE files

## Usage

1. **Select Industry**: Choose from available industries (Energy, Manufacturing, etc.)
2. **Configure Data Generation**: Set output path, duration, and DLT mode
3. **Generate Data**: Start the data generation process
4. **Manage Infrastructure**: Create SQL warehouses and deploy DLT pipelines
5. **Monitor**: Track generation progress and resource status

## Architecture

- **Frontend**: Dash web application with real-time updates
- **Backend**: Python data generators and Databricks SDK integration
- **Infrastructure**: Resource management for SQL warehouses and DLT pipelines
- **Data**: YAML-based schema definitions for different industries

## Troubleshooting

### File Watching Loops
If you experience infinite debug loops:
1. Use `run_production.py` for production environments
2. Use `run_app.py` for development with optimized settings
3. Check that `.dashignore` file is present
4. Ensure virtual environment is in excluded directories

### Databricks Connection Issues
1. Verify workspace ID and access token
2. Check network connectivity to Databricks workspace
3. Ensure proper permissions for SQL warehouse and pipeline creation

## File Structure

```
dlt-streamforge/
├── app.py                 # Main application file
├── run_app.py            # Development run script
├── run_production.py     # Production run script
├── .dashignore           # Dash file watcher exclusions
├── requirements.txt      # Python dependencies
├── infrastructure/       # Resource management
│   └── resource_manager.py
├── schema/              # Industry schema definitions
└── data_generators.py   # Data generation logic
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with both development and production modes
5. Submit a pull request

## License

This project is licensed under the MIT License.
