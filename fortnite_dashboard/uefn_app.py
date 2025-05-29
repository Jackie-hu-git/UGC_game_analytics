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

def get_latest_timestamp():
    """Get the latest timestamp from the database"""
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT MAX(timestamp) as latest_timestamp 
                FROM uefn_game_metrics
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

# Create tab components
def create_overview_tab():
    return [
        # Top Games Overview
        dbc.Row([
            dbc.Col([
                html.H3("Top UEFN Games Overview", className="text-center"),
                dcc.Graph(id='top-games-graph')
            ], width=12)
        ])
    ]

def create_player_engagement_tab():
    return [
        # Player Engagement Metrics
        dbc.Row([
            dbc.Col([
                html.H3("Player Engagement Metrics", className="text-center"),
                dcc.Graph(id='player-engagement-graph')
            ], width=12)
        ]),
        
        # Session Analysis
        dbc.Row([
            dbc.Col([
                html.H3("Session Analysis", className="text-center"),
                dcc.Graph(id='session-analysis-graph')
            ], width=12)
        ]),
        
        # Retention Analysis
        dbc.Row([
            dbc.Col([
                html.H3("Player Retention Analysis", className="text-center"),
                dcc.Graph(id='retention-graph')
            ], width=12)
        ])
    ]

def create_game_performance_tab():
    return [
        # Game Performance Metrics
        dbc.Row([
            dbc.Col([
                html.H3("Game Performance Metrics", className="text-center"),
                dcc.Graph(id='performance-graph')
            ], width=12)
        ])
    ]

def create_raw_data_tab():
    return [
        # Raw Data Table
        dbc.Row([
            dbc.Col([
                html.H3("Raw Data Table", className="text-center"),
                html.Div(id='raw-data-table')
            ], width=12)
        ])
    ]

# Modify the layout to use the tab components
app.layout = dbc.Container([
    # Status Bar
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("UEFN Data Collection Status", className="card-title"),
                    html.Div(id="status-indicator", className="mb-2"),
                    html.Div(id="last-update-time", className="text-muted")
                ])
            ], className="mb-4")
        ], width=12)
    ]),
    
    # Header
    dbc.Row([
        dbc.Col(html.H1("UEFN Analytics Dashboard", className="text-center my-4"), width=12)
    ]),
    
    # Error message container
    dbc.Row([
        dbc.Col(html.Div(id='error-message', className="text-danger"), width=12)
    ]),
    
    # Tabs for different views
    dbc.Tabs([
        dbc.Tab(create_overview_tab(), label="Overview"),
        dbc.Tab(create_player_engagement_tab(), label="Player Engagement"),
        dbc.Tab(create_game_performance_tab(), label="Game Performance"),
        dbc.Tab(create_raw_data_tab(), label="Raw Data")
    ]),
    
    # Hidden interval component for updates
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,  # 5 minutes in milliseconds
        n_intervals=0
    )
], fluid=True)

# Callback for top games graph
@app.callback(
    Output('top-games-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_top_games(n):
    try:
        engine = get_db_connection()
        query = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (game_id) *
            FROM uefn_game_metrics
            ORDER BY game_id, timestamp DESC
        )
        SELECT 
            g.title,
            m.plays,
            m.unique_players,
            m.minutes_played,
            m.favorites,
            m.recommendations,
            m.peak_ccu
        FROM latest_metrics m
        JOIN uefn_top_games g ON m.game_id = g.game_id
        ORDER BY m.plays DESC
        LIMIT 50
        """
        
        df = pd.read_sql(query, engine)
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Top 50 UEFN Games by Player Engagement', 'Top 50 UEFN Games by Peak CCU'),
            vertical_spacing=0.15,
            shared_xaxes=True
        )
        
        # Add player engagement metrics to first subplot
        fig.add_trace(
            go.Bar(
                x=df['title'],
                y=df['plays'],
                name='Total Plays',
                marker_color='rgb(55, 83, 109)'
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(
                x=df['title'],
                y=df['unique_players'],
                name='Unique Players',
                marker_color='rgb(26, 118, 255)'
            ),
            row=1, col=1
        )
        
        # Add peak CCU to second subplot
        fig.add_trace(
            go.Bar(
                x=df['title'],
                y=df['peak_ccu'],
                name='Peak CCU',
                marker_color='rgb(255, 65, 54)'
            ),
            row=2, col=1
        )
        
        # Update layout
        fig.update_layout(
            template='plotly_white',
            barmode='group',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            height=1000,  # Increase height to accommodate more games
            xaxis=dict(tickangle=45)
        )
        
        # Update x-axis labels
        fig.update_xaxes(title_text='Game Title', row=1, col=1)
        fig.update_xaxes(title_text='Game Title', row=2, col=1)
        
        # Update y-axis labels
        fig.update_yaxes(title_text='Count', row=1, col=1)
        fig.update_yaxes(title_text='Peak CCU', row=2, col=1)
        
        return fig
    except Exception as e:
        logging.error(f"Error updating top games graph: {str(e)}", exc_info=True)
        return go.Figure()

# Callback for player engagement graph
@app.callback(
    Output('player-engagement-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_player_engagement(n):
    try:
        engine = get_db_connection()
        query = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (game_id) *
            FROM uefn_game_metrics
            ORDER BY game_id, timestamp DESC
        )
        SELECT 
            g.title,
            m.plays,
            m.unique_players,
            m.minutes_played
        FROM latest_metrics m
        JOIN uefn_top_games g ON m.game_id = g.game_id
        ORDER BY m.unique_players DESC
        LIMIT 50
        """
        
        df = pd.read_sql(query, engine)
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(
                x=df['title'],
                y=df['unique_players'],
                name='Unique Players',
                marker_color='rgb(26, 118, 255)'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Bar(
                x=df['title'],
                y=df['plays'],
                name='Total Plays',
                marker_color='rgb(55, 83, 109)'
            ),
            secondary_y=False
        )
        
        fig.update_layout(
            title='Player Engagement by Game',
            xaxis_title='Game Title',
            template='plotly_white',
            barmode='group',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            height=800,
            xaxis=dict(tickangle=45)
        )
        
        fig.update_yaxes(title_text="Count", secondary_y=False)
        
        return fig
    except Exception as e:
        logging.error(f"Error updating player engagement graph: {str(e)}", exc_info=True)
        return go.Figure()

# Callback for session analysis graph
@app.callback(
    Output('session-analysis-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_session_analysis(n):
    try:
        engine = get_db_connection()
        query = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (game_id) *
            FROM uefn_game_metrics
            ORDER BY game_id, timestamp DESC
        )
        SELECT 
            g.title,
            m.average_minutes_per_player,
            m.minutes_played,
            m.unique_players,
            ROUND(m.minutes_played::numeric / NULLIF(m.unique_players, 0), 2) as avg_session_length
        FROM latest_metrics m
        JOIN uefn_top_games g ON m.game_id = g.game_id
        ORDER BY m.average_minutes_per_player DESC
        LIMIT 50
        """
        
        df = pd.read_sql(query, engine)
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(
                x=df['title'],
                y=df['average_minutes_per_player'],
                name='Average Session Length',
                marker_color='rgb(255, 65, 54)'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=df['title'],
                y=df['avg_session_length'],
                name='Total Minutes per Player',
                mode='lines+markers',
                line=dict(color='rgb(26, 118, 255)', width=3),
                marker=dict(size=8)
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            title='Session Length Analysis',
            xaxis_title='Game Title',
            template='plotly_white',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            height=800,
            xaxis=dict(tickangle=45)
        )
        
        fig.update_yaxes(title_text="Average Minutes per Session", secondary_y=False)
        fig.update_yaxes(title_text="Total Minutes per Player", secondary_y=True)
        
        return fig
    except Exception as e:
        logging.error(f"Error updating session analysis graph: {str(e)}", exc_info=True)
        return go.Figure()

# Callback for retention graph
@app.callback(
    Output('retention-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_retention(n):
    try:
        engine = get_db_connection()
        query = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (game_id) *
            FROM uefn_game_metrics
            ORDER BY game_id, timestamp DESC
        )
        SELECT 
            g.title,
            m.retention_d1,
            m.retention_d7
        FROM latest_metrics m
        JOIN uefn_top_games g ON m.game_id = g.game_id
        ORDER BY m.retention_d1 DESC
        LIMIT 50
        """
        
        df = pd.read_sql(query, engine)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['title'],
            y=df['retention_d1'],
            name='Day 1 Retention',
            mode='lines+markers',
            line=dict(color='rgb(26, 118, 255)', width=3),
            marker=dict(size=8)
        ))
        fig.add_trace(go.Scatter(
            x=df['title'],
            y=df['retention_d7'],
            name='Day 7 Retention',
            mode='lines+markers',
            line=dict(color='rgb(255, 65, 54)', width=3),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            title='Player Retention by Game',
            xaxis_title='Game Title',
            yaxis_title='Retention Rate (%)',
            template='plotly_white',
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            height=800,
            xaxis=dict(tickangle=45)
        )
        
        return fig
    except Exception as e:
        logging.error(f"Error updating retention graph: {str(e)}", exc_info=True)
        return go.Figure()

# Callback for performance graph
@app.callback(
    Output('performance-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_performance(n):
    try:
        engine = get_db_connection()
        query = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (game_id) *
            FROM uefn_game_metrics
            ORDER BY game_id, timestamp DESC
        )
        SELECT 
            g.title,
            m.favorites,
            m.recommendations
        FROM latest_metrics m
        JOIN uefn_top_games g ON m.game_id = g.game_id
        ORDER BY m.favorites DESC
        LIMIT 50
        """
        
        df = pd.read_sql(query, engine)
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Top 50 Games by Favorites', 'Top 50 Games by Recommendations'),
            vertical_spacing=0.15,
            shared_xaxes=True
        )
        
        # Add favorites subplot
        fig.add_trace(
            go.Bar(
                x=df['title'],
                y=df['favorites'],
                name='Favorites',
                marker_color='rgb(26, 118, 255)'
            ),
            row=1, col=1
        )
        
        # Add recommendations subplot
        fig.add_trace(
            go.Bar(
                x=df['title'],
                y=df['recommendations'],
                name='Recommendations',
                marker_color='rgb(255, 65, 54)'
            ),
            row=2, col=1
        )
        
        # Update layout
        fig.update_layout(
            title='Game Performance Metrics',
            template='plotly_white',
            showlegend=False,
            height=1000,
            xaxis=dict(tickangle=45)
        )
        
        # Update x-axis labels
        fig.update_xaxes(title_text='Game Title', row=1, col=1)
        fig.update_xaxes(title_text='Game Title', row=2, col=1)
        
        # Update y-axis labels
        fig.update_yaxes(title_text='Number of Favorites', row=1, col=1)
        fig.update_yaxes(title_text='Number of Recommendations', row=2, col=1)
        
        return fig
    except Exception as e:
        logging.error(f"Error updating performance graph: {str(e)}", exc_info=True)
        return go.Figure()

# Callback for raw data table
@app.callback(
    Output('raw-data-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_raw_data(n):
    try:
        engine = get_db_connection()
        query = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (game_id) *
            FROM uefn_game_metrics
            ORDER BY game_id, timestamp DESC
        )
        SELECT 
            g.title,
            g.creator_name,
            m.plays,
            m.unique_players,
            m.minutes_played,
            m.favorites,
            m.recommendations,
            m.average_minutes_per_player,
            m.peak_ccu,
            m.retention_d1,
            m.retention_d7,
            m.timestamp
        FROM latest_metrics m
        JOIN uefn_top_games g ON m.game_id = g.game_id
        ORDER BY m.plays DESC
        """
        
        df = pd.read_sql(query, engine)
        
        # Format the data
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['retention_d1'] = df['retention_d1'].round(2)
        df['retention_d7'] = df['retention_d7'].round(2)
        
        return dbc.Table.from_dataframe(
            df,
            striped=True,
            bordered=True,
            hover=True,
            responsive=True
        )
    except Exception as e:
        logging.error(f"Error updating raw data table: {str(e)}", exc_info=True)
        return html.Div("Error loading data")

# Callback for status indicator
@app.callback(
    [Output("status-indicator", "children"),
     Output("last-update-time", "children")],
    [Input("interval-component", "n_intervals")]
)
def update_status(n):
    try:
        latest_timestamp = get_latest_timestamp()
        if latest_timestamp:
            time_diff = datetime.now(pytz.UTC) - latest_timestamp
            if time_diff.total_seconds() <= 3600:  # Within last hour
                status = dbc.Alert(
                    "Data is up to date",
                    color="success",
                    className="mb-0"
                )
            else:
                status = dbc.Alert(
                    f"Data is {int(time_diff.total_seconds() / 3600)} hours old",
                    color="warning",
                    className="mb-0"
                )
            last_update = f"Last updated: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        else:
            status = dbc.Alert(
                "No recent data available",
                color="danger",
                className="mb-0"
            )
            last_update = "Last update: Never"
        
        return status, last_update
    except Exception as e:
        logging.error(f"Error updating status: {str(e)}", exc_info=True)
        return dbc.Alert("Error checking status", color="danger"), "Last update: Unknown"

if __name__ == '__main__':
    server = app.server
    app.run_server(debug=False, host='0.0.0.0', port=10000) 