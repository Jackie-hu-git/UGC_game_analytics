#!/bin/bash

# Set environment variables
export DB_HOST="localhost"
export DB_NAME="steam"
export DB_USER="postgres"
export DB_PASSWORD="postgres"
export DB_PORT="5432"
export STEAM_API_KEY="80E923AB774BA3FFF8ED8612C15631BB" 


# Create log directory if it doesn't exist
mkdir -p logs

# Start data collector
echo "Starting data collector..."
nohup python src/collect_data.py > logs/collector.log 2>&1 &
COLLECTOR_PID=$!
echo "Data collector started with PID $COLLECTOR_PID"

# Wait for data collector to initialize
sleep 5

# Start dashboard
echo "Starting dashboard..."
nohup python dashboard/app.py > logs/dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo "Dashboard started with PID $DASHBOARD_PID"

# Print instructions
echo ""
echo "Data collector and dashboard are running."
echo "Data collector log: logs/collector.log"
echo "Dashboard log: logs/dashboard.log"
echo "Dashboard URL: http://localhost:8051"
echo ""
echo "To stop the services, run: kill $COLLECTOR_PID $DASHBOARD_PID" 