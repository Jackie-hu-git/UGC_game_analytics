import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Dict, Any
import json
from pathlib import Path
import time
from dotenv import load_dotenv
import base64

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API Configuration
API_BASE_URL = "https://api.fortnite.com/ecosystem/v1"
CLIENT_ID = os.getenv("EPIC_CLIENT_ID")
CLIENT_SECRET = os.getenv("EPIC_CLIENT_SECRET")

def get_access_token() -> str:
    """Get Epic Games API access token."""
    try:
        auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
        auth_bytes = auth_string.encode('ascii')
        base64_auth = base64.b64encode(auth_bytes).decode('ascii')

        headers = {
            'Authorization': f'Basic {base64_auth}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'grant_type': 'client_credentials'
        }

        response = requests.post(
            'https://api.epicgames.dev/epic/oauth/v1/token',
            headers=headers,
            data=data
        )
        response.raise_for_status()
        
        return response.json()['access_token']
    
    except Exception as e:
        logger.error(f"Error getting access token: {e}")
        raise

def get_db_connection():
    """Create a database connection."""
    try:
        # Get database credentials from environment variables
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        
        if not all([db_user, db_password, db_host, db_name]):
            raise ValueError("Missing database credentials in environment variables")
        
        # Complete the host name with Render's domain
        full_host = f"{db_host}.oregon-postgres.render.com"
        
        # Create connection
        conn = psycopg2.connect(
            host=full_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=5432
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def init_db():
    """Initialize the database by creating necessary tables."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Read and execute the SQL file
        with open('fortnite_src/uefn_create_tables.sql', 'r') as f:
            sql_commands = f.read()
            cur.execute(sql_commands)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database tables created successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def get_top_games(limit: int = 500) -> List[Dict[str, Any]]:
    """
    Fetch top UEFN games from the Fortnite API.
    
    Args:
        limit (int): Number of games to fetch (default: 500)
    
    Returns:
        List[Dict]: List of game data dictionaries
    """
    try:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        all_games = []
        seen_codes = set()  # Track unique game codes
        seen_cursors = set()  # Track seen cursors to prevent loops
        
        # First, get the initial page
        logger.info(f"Fetching first page of games from {API_BASE_URL}/islands")
        response = requests.get(
            f"{API_BASE_URL}/islands",
            headers=headers,
            params={"limit": 100}
        )
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"API Response: {json.dumps(data, indent=2)}")
        
        total_games = data.get("meta", {}).get("count", 0)
        games = data.get("data", [])
        logger.info(f"Found {total_games} total games, got {len(games)} in first page")
        
        # Add games from first page
        for game in games:
            if game["code"] not in seen_codes:
                all_games.append(game)
                seen_codes.add(game["code"])
        
        # Get the cursor from the last game in the page
        current_cursor = games[-1]["meta"]["page"]["cursor"] if games else None
        seen_cursors.add(current_cursor)
        
        # Fetch all pages using cursor-based pagination
        while current_cursor and len(all_games) < limit:
            logger.info(f"Fetching next page with cursor: {current_cursor}")
            response = requests.get(
                f"{API_BASE_URL}/islands",
                headers=headers,
                params={"limit": 100, "cursor": current_cursor}
            )
            response.raise_for_status()
            
            page_data = response.json()
            page_games = page_data.get("data", [])
            logger.info(f"Got {len(page_games)} games in this page")
            
            if not page_games:
                logger.info("No more games to fetch")
                break
                
            # Add only new games
            new_games_added = 0
            for game in page_games:
                if game["code"] not in seen_codes:
                    all_games.append(game)
                    seen_codes.add(game["code"])
                    new_games_added += 1
            
            if new_games_added == 0:
                logger.info("No new games found in this page, stopping pagination")
                break
            
            # Get the cursor from the last game in this page
            current_cursor = page_games[-1]["meta"]["page"]["cursor"]
            
            # Check if we've seen this cursor before
            if current_cursor in seen_cursors:
                logger.info("Detected cursor loop, stopping pagination")
                break
                
            seen_cursors.add(current_cursor)
            time.sleep(1)  # Rate limiting
            
        logger.info(f"Total unique games collected: {len(all_games)}")
        return all_games[:limit]
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching top games: {e}")
        if hasattr(e.response, 'text'):
            logger.error(f"Response text: {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_top_games: {e}")
        raise

def get_game_metrics(game_id: str) -> Dict[str, Any]:
    """
    Fetch detailed metrics for a specific game.
    
    Args:
        game_id (str): The ID of the game
    
    Returns:
        Dict: Game metrics data
    """
    try:
        headers = {
            "Content-Type": "application/json"
        }
        
        response = requests.get(
            f"{API_BASE_URL}/islands/{game_id}/metrics",
            headers=headers
        )
        response.raise_for_status()
        
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching game metrics for {game_id}: {e}")
        raise

def extract_latest_metric_value(metric_data: List[Dict[str, Any]]) -> Any:
    """Extract the latest value from a metric array."""
    if not metric_data:
        return 0
    return metric_data[-1]["value"]

def extract_latest_retention(retention_data: List[Dict[str, Any]]) -> Dict[str, float]:
    """Extract the latest retention values."""
    if not retention_data:
        return {"d1": 0, "d7": 0}
    latest = retention_data[-1]
    return {
        "d1": latest.get("d1", 0),
        "d7": latest.get("d7", 0)
    }

def save_games_to_db(games: List[Dict[str, Any]]) -> None:
    """
    Save game data to the database.
    
    Args:
        games (List[Dict]): List of game data dictionaries
    """
    try:
        if not games:
            logger.warning("No games to save to database")
            return
            
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Prepare data for bulk insert
        game_data = []
        for game in games:
            try:
                game_data.append((
                    game["code"],
                    game["title"],
                    game.get("creatorCode"),
                    game.get("description"),
                    game.get("thumbnailUrl"),
                    None,  # created_at not available in API
                    None,  # updated_at not available in API
                ))
            except KeyError as e:
                logger.error(f"Missing required field in game data: {e}")
                logger.error(f"Game data: {json.dumps(game, indent=2)}")
                continue
        
        if not game_data:
            logger.warning("No valid game data to insert")
            return
            
        # Bulk insert games
        execute_values(cur, """
            INSERT INTO uefn_top_games (
                game_id, title, creator_name, description,
                thumbnail_url, created_at, updated_at
            ) VALUES %s
            ON CONFLICT (game_id) DO UPDATE SET
                title = EXCLUDED.title,
                creator_name = EXCLUDED.creator_name,
                description = EXCLUDED.description,
                thumbnail_url = EXCLUDED.thumbnail_url,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                timestamp = CURRENT_TIMESTAMP
        """, game_data)
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Successfully saved {len(game_data)} games to database")
    
    except Exception as e:
        logger.error(f"Error saving games to database: {e}")
        raise

def save_game_metrics_to_db(game_id: str, metrics: Dict[str, Any]) -> None:
    """
    Save game metrics to the database.
    
    Args:
        game_id (str): The ID of the game
        metrics (Dict): Game metrics data
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Extract latest values from each metric
        latest_plays = extract_latest_metric_value(metrics.get("plays", []))
        latest_unique_players = extract_latest_metric_value(metrics.get("uniquePlayers", []))
        latest_minutes_played = extract_latest_metric_value(metrics.get("minutesPlayed", []))
        latest_favorites = extract_latest_metric_value(metrics.get("favorites", []))
        latest_recommendations = extract_latest_metric_value(metrics.get("recommendations", []))
        latest_avg_minutes = extract_latest_metric_value(metrics.get("averageMinutesPerPlayer", []))
        latest_peak_ccu = extract_latest_metric_value(metrics.get("peakCCU", []))
        
        # Extract retention metrics
        retention = extract_latest_retention(metrics.get("retention", []))
        
        # Get the timestamp from the metrics (use the first available metric's timestamp)
        metrics_timestamp = None
        for metric_name in ["plays", "uniquePlayers", "minutesPlayed"]:
            if metrics.get(metric_name):
                metrics_timestamp = metrics[metric_name][-1]["timestamp"]
                break
        
        # Prepare metrics data
        metrics_data = [(
            game_id,
            datetime.now().date(),
            latest_plays,
            latest_unique_players,
            latest_minutes_played,
            latest_favorites,
            latest_recommendations,
            latest_avg_minutes,
            latest_peak_ccu,
            retention["d1"],
            retention["d7"],
            metrics_timestamp
        )]
        
        # Insert metrics
        execute_values(cur, """
            INSERT INTO uefn_game_metrics (
                game_id, date, plays, unique_players, minutes_played,
                favorites, recommendations, average_minutes_per_player,
                peak_ccu, retention_d1, retention_d7, metrics_timestamp
            ) VALUES %s
            ON CONFLICT (game_id, date) DO UPDATE SET
                plays = EXCLUDED.plays,
                unique_players = EXCLUDED.unique_players,
                minutes_played = EXCLUDED.minutes_played,
                favorites = EXCLUDED.favorites,
                recommendations = EXCLUDED.recommendations,
                average_minutes_per_player = EXCLUDED.average_minutes_per_player,
                peak_ccu = EXCLUDED.peak_ccu,
                retention_d1 = EXCLUDED.retention_d1,
                retention_d7 = EXCLUDED.retention_d7,
                metrics_timestamp = EXCLUDED.metrics_timestamp,
                timestamp = CURRENT_TIMESTAMP
        """, metrics_data)
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Successfully saved metrics for game {game_id}")
    
    except Exception as e:
        logger.error(f"Error saving metrics for game {game_id}: {e}")
        raise

def collect_and_save():
    """Main function to collect and save all data"""
    try:
        # Initialize database
        logger.info("Initializing database...")
        init_db()
        
        # Get top games
        logger.info("Fetching top games...")
        games = get_top_games()
        logger.info(f"Found {len(games)} games")
        
        # Save games to database
        logger.info("Saving games to database...")
        save_games_to_db(games)
        
        # Get and save metrics for each game
        logger.info("Fetching and saving game metrics...")
        for game in games:
            try:
                metrics = get_game_metrics(game["code"])
                save_game_metrics_to_db(game["code"], metrics)
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Error processing metrics for game {game['code']}: {e}")
                continue
        
        logger.info("Data collection completed successfully")
    
    except Exception as e:
        logger.error(f"Error in data collection: {e}")
        raise

if __name__ == "__main__":
    collect_and_save() 