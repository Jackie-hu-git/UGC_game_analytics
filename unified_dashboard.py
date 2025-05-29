import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, html, dcc, Input, Output
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import numpy as np
from datetime import datetime, timedelta
import sys
import pytz
import logging
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection with error handling
def get_db_connection():
    try:
        engine = create_engine(
            f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres')}@{os.getenv('DB_HOST', 'localhost')}/{os.getenv('DB_NAME', 'steam')}"
        )
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def get_latest_timestamp(platform):
    """Get the latest timestamp from the database for the specified platform"""
    engine = get_db_connection()
    try:
        table = "uefn_game_metrics" if platform == "uefn" else "games"
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT MAX(timestamp) as latest_timestamp 
                FROM {table}
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """))
            row = result.fetchone()
            if row and row[0]:
                return row[0].astimezone(pytz.UTC)
            return None
    except Exception as e:
        logging.error(f"Error getting latest timestamp: {str(e)}", exc_info=True)
        return None

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout
app.layout = dbc.Container([
    # Platform Selection
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Platform Selection", className="card-title"),
                    dbc.ButtonGroup([
                        dbc.Button("Steam", id="steam-btn", color="primary", className="me-2"),
                        dbc.Button("UEFN", id="uefn-btn", color="secondary")
                    ], className="mb-3"),
                    html.Div(id="platform-status", className="mb-2"),
                    html.Div(id="last-update-time", className="text-muted")
                ])
            ], className="mb-4")
        ], width=12)
    ]),
    
    # Header
    dbc.Row([
        dbc.Col(html.H1("Game Analytics Dashboard", className="text-center my-4"), width=12)
    ]),
    
    # Error message container
    dbc.Row([
        dbc.Col(html.Div(id='error-message', className="text-danger"), width=12)
    ]),
    
    # Content container
    html.Div(id='dashboard-content'),
    
    # Hidden interval component for updates
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,  # 5 minutes in milliseconds
        n_intervals=0
    ),
    
    # Hidden div to store current platform
    dcc.Store(id='current-platform', data='steam')
], fluid=True)

# Callback to update platform selection
@app.callback(
    [Output('current-platform', 'data'),
     Output('steam-btn', 'color'),
     Output('uefn-btn', 'color')],
    [Input('steam-btn', 'n_clicks'),
     Input('uefn-btn', 'n_clicks')],
    prevent_initial_call=True
)
def update_platform(steam_clicks, uefn_clicks):
    ctx = dash.callback_context
    if not ctx.triggered:
        return 'steam', 'primary', 'secondary'
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    if button_id == 'steam-btn':
        return 'steam', 'primary', 'secondary'
    else:
        return 'uefn', 'secondary', 'primary'

# Callback to update status
@app.callback(
    [Output("platform-status", "children"),
     Output("last-update-time", "children")],
    [Input("interval-component", "n_intervals"),
     Input("current-platform", "data")]
)
def update_status(n, platform):
    try:
        latest_timestamp = get_latest_timestamp(platform)
        if latest_timestamp:
            time_diff = datetime.now(pytz.UTC) - latest_timestamp
            if time_diff.total_seconds() <= 3600:  # Within last hour
                status = dbc.Alert(
                    f"{platform.upper()} data is up to date",
                    color="success",
                    className="mb-0"
                )
            else:
                status = dbc.Alert(
                    f"{platform.upper()} data is {int(time_diff.total_seconds() / 3600)} hours old",
                    color="warning",
                    className="mb-0"
                )
            last_update = f"Last updated: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        else:
            status = dbc.Alert(
                f"No recent {platform.upper()} data available",
                color="danger",
                className="mb-0"
            )
            last_update = "Last update: Never"
        
        return status, last_update
    except Exception as e:
        logging.error(f"Error updating status: {str(e)}", exc_info=True)
        return dbc.Alert("Error checking status", color="danger"), "Last update: Unknown"

# Callback to update dashboard content
@app.callback(
    Output('dashboard-content', 'children'),
    [Input('current-platform', 'data'),
     Input('interval-component', 'n_intervals')]
)
def update_dashboard_content(platform, n):
    if platform == 'steam':
        # Import Steam dashboard components
        from steam_dashboard.steam_app import (
            create_genre_analytics_tab,
            create_game_analytics_tab
        )
        return dbc.Tabs([
            dbc.Tab(create_genre_analytics_tab(), label="Genre Analytics"),
            dbc.Tab(create_game_analytics_tab(), label="Game Analytics")
        ])
    else:
        # Import UEFN dashboard components
        from fortnite_dashboard.uefn_app import (
            create_overview_tab,
            create_player_engagement_tab,
            create_game_performance_tab
        )
        return dbc.Tabs([
            dbc.Tab(create_overview_tab(), label="Overview"),
            dbc.Tab(create_player_engagement_tab(), label="Player Engagement"),
            dbc.Tab(create_game_performance_tab(), label="Game Performance")
        ])

if __name__ == '__main__':
    server = app.server
    app.run_server(debug=False, host='0.0.0.0', port=10000) 