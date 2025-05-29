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
        # Create SQLAlchemy engine
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
                FROM games
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """))
            row = result.fetchone()
            if row and row[0]:
                # Convert to timezone-aware datetime in UTC
                return row[0].astimezone(pytz.UTC)
            return None
    except Exception as e:
        logging.error(f"Error getting latest timestamp: {str(e)}", exc_info=True)
        return None

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout
app.layout = dbc.Container([
    # Status Bar
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Data Collection Status", className="card-title"),
                    html.Div(id="status-indicator", className="mb-2"),
                    html.Div(id="last-update-time", className="text-muted")
                ])
            ], className="mb-4")
        ], width=12)
    ]),
    
    # Header
    dbc.Row([
        dbc.Col(html.H1("Steam Analytics Dashboard", className="text-center my-4"), width=12)
    ]),
    
    # Error message container
    dbc.Row([
        dbc.Col(html.Div(id='error-message', className="text-danger"), width=12)
    ]),
    
    # Tabs for different views
    dbc.Tabs([
        # Genre Analytics Tab
        dbc.Tab([
            # Genre Performance Metrics
            dbc.Row([
                dbc.Col([
                    html.H3("Genre Performance Metrics", className="text-center"),
                    dcc.Graph(id='genre-metrics-graph')
                ], width=12)
            ]),
            
            # Raw Data Tables
            dbc.Row([
                dbc.Col([
                    html.H4("Raw Data Tables", className="text-center mt-4"),
                    dbc.Tabs([
                        dbc.Tab([
                            dbc.Alert([
                                html.H5("Average Player Count Table", className="alert-heading"),
                                html.P([
                                    "This table shows the following metrics:",
                                    html.Br(),
                                    "• Total Games: Number of games in each genre",
                                    html.Br(),
                                    "• Total Players: Sum of peak players across all games in the genre",
                                    html.Br(),
                                    "• Average Player Count: Total players divided by number of games",
                                    html.Br(),
                                    "• Average Review Score: Weighted average of Metacritic scores (70%) and Steam review scores (30%)",
                                    html.Br(),
                                    "• Average Price: Mean price of all games in the genre"
                                ])
                            ], color="info", className="mb-3"),
                            html.Div(id='player-count-table')
                        ], label="Average Player Count"),
                        dbc.Tab([
                            dbc.Alert([
                                html.H5("DLC Adoption Rate Table", className="alert-heading"),
                                html.P([
                                    "This table shows the following metrics:",
                                    html.Br(),
                                    "• Total Games: Number of games in each genre",
                                    html.Br(),
                                    "• Total Players: Sum of peak players across all games in the genre",
                                    html.Br(),
                                    "• Average Player Count: Total players divided by number of games",
                                    html.Br(),
                                    "• Average Price: Mean price of all games in the genre",
                                    html.Br(),
                                    "• DLC Adoption Rate: Percentage of games in the genre that have DLC content"
                                ])
                            ], color="info", className="mb-3"),
                            html.Div(id='dlc-adoption-table')
                        ], label="DLC Adoption Rate"),
                        dbc.Tab([
                            dbc.Alert([
                                html.H5("Sentiment Score Table", className="alert-heading"),
                                html.P([
                                    "This table shows the following metrics:",
                                    html.Br(),
                                    "• Total Games: Number of games in each genre",
                                    html.Br(),
                                    "• Average Review Score: Weighted average of Metacritic scores (70%) and Steam review scores (30%)",
                                    html.Br(),
                                    "• Average Player Count: Total players divided by number of games",
                                    html.Br(),
                                    "• Sentiment Score: Weighted combination of review scores (80%) and player engagement (20%)"
                                ])
                            ], color="info", className="mb-3"),
                            html.Div(id='sentiment-score-table')
                        ], label="Sentiment Score")
                    ])
                ], width=12)
            ]),
            
            # Market Activity vs Community Engagement
            dbc.Row([
                dbc.Col([
                    html.H3("Market Activity vs Community Engagement", className="text-center"),
                    dcc.Graph(id='market-community-graph')
                ], width=12)
            ]),
            
            # Player Statistics by Genre
            dbc.Row([
                dbc.Col([
                    html.H3("Player Statistics by Genre", className="text-center"),
                    dcc.Graph(id='player-stats-graph')
                ], width=12)
            ]),
            
            # Review and Sentiment Analysis
            dbc.Row([
                dbc.Col([
                    html.H3("Review and Sentiment Analysis", className="text-center"),
                    dcc.Graph(id='review-sentiment-graph')
                ], width=12)
            ])
        ], label="Genre Analytics"),
        
        # Game Analytics Tab
        dbc.Tab([
            # Top Games by Current Players
            dbc.Row([
                dbc.Col([
                    html.H3("Top Games by Current Players", className="text-center"),
                    dcc.Graph(id='top-games-graph')
                ], width=12)
            ]),
            
            # Player Count Trends
            dbc.Row([
                dbc.Col([
                    html.H3("Player Count Trends", className="text-center"),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Select Game:"),
                            dcc.Dropdown(
                                id='game-selector',
                                placeholder="Select a game to view trends",
                                clearable=True
                            )
                        ], width=6)
                    ], className="mb-3"),
                    dcc.Graph(id='player-trends-graph')
                ], width=12)
            ]),
            
            # Game Statistics
            dbc.Row([
                dbc.Col([
                    html.H3("Game Statistics", className="text-center"),
                    dcc.Graph(id='game-stats-graph')
                ], width=12)
            ]),
            
            # Raw Data
            dbc.Row([
                dbc.Col([
                    html.H3("Raw Data", className="text-center"),
                    html.Div(id='raw-data-table')
                ], width=12)
            ])
        ], label="Game Analytics")
    ]),
    
    # Auto-update interval
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Update every minute
        n_intervals=0
    )
], fluid=True)

def clean_numeric_data(df, columns):
    """Clean numeric data by replacing nulls and converting to float"""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def execute_query(query, params=None):
    """Execute a database query with error handling"""
    try:
        engine = get_db_connection()
        df = pd.read_sql(query, engine, params=params)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()

# Callbacks for Genre Analytics
@app.callback(
    [Output('genre-metrics-graph', 'figure'),
     Output('error-message', 'children')],
    Input('interval-component', 'n_intervals')
)
def update_genre_metrics(n):
    query = """
        SELECT 
            genre,
            total_games,
            total_players,
            avg_player_count,
            avg_review_score,
            avg_price,
            market_activity_score,
            community_engagement_score,
            dlc_adoption_rate,
            sentiment_score
        FROM genre_benchmarks
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM genre_benchmarks
        )
    """
    df = execute_query(query)
    
    if df.empty:
        return go.Figure(), "Error: No genre benchmark data available"
    
    numeric_columns = ['total_games', 'total_players', 'avg_player_count', 
                      'avg_review_score', 'avg_price', 'market_activity_score',
                      'community_engagement_score', 'dlc_adoption_rate', 'sentiment_score']
    df = clean_numeric_data(df, numeric_columns)
    
    # Create a subplot for each metric
    metrics = ['avg_player_count', 'dlc_adoption_rate', 'sentiment_score']
    titles = ['Average Player Count', 'DLC Adoption Rate', 'Sentiment Score']
    
    # Create figure with subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=titles,
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )
    
    # Add traces for each metric
    for i, (metric, title) in enumerate(zip(metrics, titles)):
        row = i // 2 + 1
        col = i % 2 + 1
        
        fig.add_trace(
            go.Bar(
                x=df['genre'],
                y=df[metric],
                name=title,
                text=df[metric].round(2),
                textposition='auto',
                hovertemplate='<b>%{x}</b><br>' + title + ': %{y:.2f}<br>' +
                             '<extra>Metacritic scores are used in calculating these metrics:<br>' +
                             '• Market Activity Score (70% weight)<br>' +
                             '• Community Engagement Score (20% weight)<br>' +
                             '• Sentiment Score (80% weight)</extra>'
            ),
            row=row, col=col
        )
    
    # Update layout
    fig.update_layout(
        title='Genre Performance Metrics',
        height=800,  # Reduced height since we have fewer subplots
        showlegend=False,
        margin=dict(t=100)
    )
    
    # Update axes for each subplot
    for i, title in enumerate(titles):
        row = i // 2 + 1
        col = i % 2 + 1
        fig.update_xaxes(title_text='Genre', row=row, col=col)
        fig.update_yaxes(title_text=title, row=row, col=col)
    
    return fig, ""

@app.callback(
    Output('market-community-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_market_community(n):
    engine = get_db_connection()
    query = """
        SELECT 
            genre,
            market_activity_score,
            community_engagement_score,
            avg_card_market_price,
            avg_item_market_price,
            avg_workshop_items,
            avg_forum_posts
        FROM genre_benchmarks
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM genre_benchmarks
        )
    """
    df = pd.read_sql(query, engine)
    
    numeric_columns = ['market_activity_score', 'community_engagement_score',
                      'avg_card_market_price', 'avg_item_market_price',
                      'avg_workshop_items', 'avg_forum_posts']
    df = clean_numeric_data(df, numeric_columns)
    
    size_values = df['avg_card_market_price'].clip(lower=0.1)
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['market_activity_score'],
        y=df['community_engagement_score'],
        mode='markers+text',
        text=df['genre'],
        textposition='top center',
        marker=dict(
            size=size_values * 10,
            color=df['avg_item_market_price'].clip(lower=0.1),
            colorscale='Viridis',
            showscale=True,
            sizemode='area',
            sizemin=4
        )
    ))
    
    fig.update_layout(
        title='Market Activity vs Community Engagement by Genre',
        xaxis_title='Market Activity Score',
        yaxis_title='Community Engagement Score',
        height=600
    )
    
    return fig

@app.callback(
    Output('player-stats-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_player_stats(n):
    engine = get_db_connection()
    query = """
        SELECT 
            genre,
            total_players,
            avg_player_count,
            avg_dlc_count,
            dlc_adoption_rate
        FROM genre_benchmarks
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM genre_benchmarks
        )
    """
    df = pd.read_sql(query, engine)
    
    numeric_columns = ['total_players', 'avg_player_count', 'avg_dlc_count', 'dlc_adoption_rate']
    df = clean_numeric_data(df, numeric_columns)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['genre'],
        y=df['total_players'],
        name='Total Players',
        text=df['total_players'].round(0),
        textposition='auto',
    ))
    
    fig.add_trace(go.Scatter(
        x=df['genre'],
        y=df['avg_player_count'],
        name='Average Players',
        mode='lines+markers',
        yaxis='y2'
    ))
    
    fig.update_layout(
        title='Player Statistics by Genre',
        xaxis_title='Genre',
        yaxis_title='Total Players',
        yaxis2=dict(
            title='Average Players',
            overlaying='y',
            side='right'
        ),
        height=600
    )
    
    return fig

@app.callback(
    Output('review-sentiment-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_review_sentiment(n):
    engine = get_db_connection()
    query = """
        SELECT 
            genre,
            avg_review_score,
            positive_review_ratio,
            sentiment_score,
            avg_review_length
        FROM genre_benchmarks
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM genre_benchmarks
        )
    """
    df = pd.read_sql(query, engine)
    
    numeric_columns = ['avg_review_score', 'positive_review_ratio', 
                      'sentiment_score', 'avg_review_length']
    df = clean_numeric_data(df, numeric_columns)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['genre'],
        y=df['avg_review_score'],
        name='Average Review Score',
        text=df['avg_review_score'].round(2),
        textposition='auto',
    ))
    
    fig.add_trace(go.Bar(
        x=df['genre'],
        y=df['positive_review_ratio'] * 100,
        name='Positive Review Ratio (%)',
        text=(df['positive_review_ratio'] * 100).round(2),
        textposition='auto',
    ))
    
    fig.add_trace(go.Scatter(
        x=df['genre'],
        y=df['sentiment_score'],
        name='Sentiment Score',
        mode='lines+markers',
        yaxis='y2'
    ))
    
    fig.update_layout(
        title='Review and Sentiment Analysis by Genre',
        xaxis_title='Genre',
        yaxis_title='Score',
        yaxis2=dict(
            title='Sentiment Score',
            overlaying='y',
            side='right'
        ),
        height=600
    )
    
    return fig

# Callbacks for Game Analytics
@app.callback(
    Output('top-games-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_top_games(n):
    query = """
        SELECT name, current_players, peak_players, genres
        FROM games
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM games
        )
        ORDER BY peak_players DESC
        LIMIT 20
    """
    df = execute_query(query)
    
    if df.empty:
        return go.Figure()
    
    df = clean_numeric_data(df, ['current_players', 'peak_players'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['name'],
        y=df['current_players'],
        name='Current Players',
        text=df['current_players'].round(0),
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>Current Players: %{y}<br>Genres: %{customdata}<extra></extra>',
        customdata=df['genres']
    ))
    
    fig.add_trace(go.Bar(
        x=df['name'],
        y=df['peak_players'],
        name='Peak Players',
        text=df['peak_players'].round(0),
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>Peak Players: %{y}<br>Genres: %{customdata}<extra></extra>',
        customdata=df['genres']
    ))
    
    fig.update_layout(
        title='Top 20 Games by Peak Players',
        xaxis_title='Game',
        yaxis_title='Number of Players',
        barmode='group',
        height=600,
        xaxis_tickangle=45
    )
    
    return fig

@app.callback(
    Output('game-selector', 'options'),
    Input('interval-component', 'n_intervals')
)
def update_game_selector(n):
    query = """
        SELECT DISTINCT name
        FROM games
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY name
    """
    df = execute_query(query)
    
    if df.empty:
        return []
    
    return [{'label': name, 'value': name} for name in df['name']]

@app.callback(
    Output('player-trends-graph', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('game-selector', 'value')]
)
def update_player_trends(n, selected_game):
    if not selected_game:
        return go.Figure()
        
    query = """
        SELECT name, current_players, timestamp
        FROM games
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        AND name = %(game_name)s
        ORDER BY timestamp
    """
    df = execute_query(query, params={'game_name': selected_game})
    
    if df.empty:
        return go.Figure()
    
    df = clean_numeric_data(df, ['current_players'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['current_players'],
        name=selected_game,
        mode='lines+markers',
        hovertemplate='<b>%{text}</b><br>Time: %{x}<br>Players: %{y}<extra></extra>',
        text=[selected_game] * len(df)
    ))
    
    fig.update_layout(
        title=f'Player Count Trends for {selected_game} (Last 24 Hours)',
        xaxis_title='Time',
        yaxis_title='Current Players',
        height=600
    )
    
    return fig

@app.callback(
    Output('game-stats-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_game_stats(n):
    query = """
        SELECT name, current_players, peak_players, metacritic_score, price_usd, genres
        FROM games
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM games
        )
        ORDER BY peak_players DESC
        LIMIT 10
    """
    df = execute_query(query)
    
    if df.empty:
        return go.Figure()
    
    df = clean_numeric_data(df, ['current_players', 'peak_players', 'metacritic_score', 'price_usd'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['name'],
        y=df['current_players'],
        name='Current Players',
        text=df['current_players'].round(0),
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>Current Players: %{y}<br>Genres: %{customdata}<extra></extra>',
        customdata=df['genres']
    ))
    
    fig.add_trace(go.Bar(
        x=df['name'],
        y=df['peak_players'],
        name='Peak Players',
        text=df['peak_players'].round(0),
        textposition='auto',
        hovertemplate='<b>%{x}</b><br>Peak Players: %{y}<br>Genres: %{customdata}<extra></extra>',
        customdata=df['genres']
    ))
    
    fig.update_layout(
        title='Game Statistics for Top 10 Games by Peak Players',
        xaxis_title='Game',
        yaxis_title='Number of Players',
        barmode='group',
        height=600,
        xaxis_tickangle=45
    )
    
    return fig

@app.callback(
    Output('raw-data-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_raw_data(n):
    query = """
        SELECT name, current_players, peak_players, metacritic_score, price_usd, genres, timestamp
        FROM games
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM games
        )
        ORDER BY peak_players DESC
        LIMIT 50
    """
    df = execute_query(query)
    
    if df.empty:
        return html.Div("No data available")
    
    df = clean_numeric_data(df, ['current_players', 'peak_players', 'metacritic_score', 'price_usd'])
    
    return dbc.Table.from_dataframe(
        df,
        striped=True,
        bordered=True,
        hover=True,
        responsive=True
    )

# Add callback for status indicator
@app.callback(
    [Output("status-indicator", "children"),
     Output("last-update-time", "children")],
    [Input("interval-component", "n_intervals")]
)
def update_status(n):
    latest_timestamp = get_latest_timestamp()
    if latest_timestamp:
        # Get current time in UTC
        current_time = datetime.now(pytz.UTC)
        time_diff = current_time - latest_timestamp
        minutes_ago = int(time_diff.total_seconds() / 60)
        
        if minutes_ago < 5:
            status_color = "success"
            status_text = "Active"
        elif minutes_ago < 15:
            status_color = "warning"
            status_text = "Warning"
        else:
            status_color = "danger"
            status_text = "Inactive"
            
        status = dbc.Badge(status_text, color=status_color, className="me-2")
        time_text = f"Last updated: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')} ({minutes_ago} minutes ago)"
    else:
        status = dbc.Badge("No Data", color="secondary", className="me-2")
        time_text = "No data available in the last 24 hours"
        
    return status, time_text

@app.callback(
    Output('player-count-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_player_count_table(n):
    query = """
        SELECT 
            genre,
            total_games,
            total_players,
            avg_player_count,
            avg_review_score,
            avg_price
        FROM genre_benchmarks
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM genre_benchmarks
        )
        ORDER BY avg_player_count DESC
    """
    df = execute_query(query)
    
    if df.empty:
        return "No data available"
    
    # Format the numbers
    df['total_players'] = df['total_players'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "N/A")
    df['avg_player_count'] = df['avg_player_count'].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "N/A")
    df['avg_review_score'] = df['avg_review_score'].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "N/A")
    df['avg_price'] = df['avg_price'].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else "N/A")
    
    return dbc.Table.from_dataframe(
        df,
        striped=True,
        bordered=True,
        hover=True,
        className="mt-3"
    )

@app.callback(
    Output('dlc-adoption-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_dlc_adoption_table(n):
    query = """
        SELECT 
            genre,
            total_games,
            total_players,
            avg_player_count,
            avg_price,
            dlc_adoption_rate
        FROM genre_benchmarks
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM genre_benchmarks
        )
        ORDER BY dlc_adoption_rate DESC
    """
    df = execute_query(query)
    
    if df.empty:
        return "No data available"
    
    # Format the numbers
    df['total_players'] = df['total_players'].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "N/A")
    df['avg_player_count'] = df['avg_player_count'].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "N/A")
    df['avg_price'] = df['avg_price'].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else "N/A")
    df['dlc_adoption_rate'] = df['dlc_adoption_rate'].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "N/A")
    
    return dbc.Table.from_dataframe(
        df,
        striped=True,
        bordered=True,
        hover=True,
        className="mt-3"
    )

@app.callback(
    Output('sentiment-score-table', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_sentiment_score_table(n):
    query = """
        SELECT 
            genre,
            total_games,
            avg_review_score,
            avg_player_count,
            sentiment_score
        FROM genre_benchmarks
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM genre_benchmarks
        )
        ORDER BY sentiment_score DESC
    """
    df = execute_query(query)
    
    if df.empty:
        return "No data available"
    
    # Format the numbers
    df['avg_review_score'] = df['avg_review_score'].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "N/A")
    df['avg_player_count'] = df['avg_player_count'].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "N/A")
    df['sentiment_score'] = df['sentiment_score'].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "N/A")
    
    return dbc.Table.from_dataframe(
        df,
        striped=True,
        bordered=True,
        hover=True,
        className="mt-3"
    )

if __name__ == '__main__':
    # Try different ports if 8051 is in use
    port = 8051
    while port < 8060:
        try:
            app.run_server(host='0.0.0.0', port=port, debug=True)
            break
        except OSError:
            print(f"Port {port} is in use, trying next port...")
            port += 1 