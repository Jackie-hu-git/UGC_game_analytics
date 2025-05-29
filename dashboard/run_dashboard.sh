#!/bin/bash

# Set environment variables
export DB_HOST="localhost"
export DB_NAME="steam"
export DB_USER="postgres"
export DB_PASSWORD="postgres"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Install requirements if not already installed
pip install -r requirements.txt
pip install dash dash-bootstrap-components pandas plotly psycopg2-binary python-dotenv

# Run the dashboard
python app.py 