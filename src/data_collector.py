import requests
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
import schedule
import time
import json
from pathlib import Path
import logging
from tqdm import tqdm

# Cache for game names
CACHE_FILE = "game_names_cache.json"

def load_cache() -> Dict[str, str]:
    """Load game names from cache file"""
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading cache: {e}")
    return {}

def save_cache(cache: Dict[str, str]) -> None:
    """Save game names to cache file"""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f)
    except Exception as e:
        print(f"Error saving cache: {e}")

# Initialize cache
game_names_cache = load_cache()

def get_db_connection():
    """
    Create a connection to the PostgreSQL database.
    
    Returns:
        psycopg2.connection: Database connection object
        
    Raises:
        psycopg2.Error: If connection fails
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "steam"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            port=os.getenv("DB_PORT", "5432")
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        raise

def init_db():
    """
    Initialize the database by creating necessary tables if they don't exist.
    
    Raises:
        psycopg2.Error: If table creation fails
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Read and execute the SQL file
        with open('src/create_tables.sql', 'r') as f:
            sql_commands = f.read()
            cur.execute(sql_commands)
        
        conn.commit()
        cur.close()
        conn.close()
        print("Database tables created successfully")
        
    except psycopg2.Error as e:
        print(f"Error initializing database: {e}")
        raise
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        raise

def get_game_details(appid, api_key):
    """Get detailed game information from Steam API with caching and rate limiting."""
    # Check cache first
    cache_file = 'game_details_cache.json'
    try:
        with open(cache_file, 'r') as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cache = {}
    
    if str(appid) in cache:
        cached_data = cache[str(appid)]
        if isinstance(cached_data, dict):
            return cached_data
        else:
            # Remove invalid cache entry
            del cache[str(appid)]
    
    url = f"https://store.steampowered.com/api/appdetails"
    params = {
        'appids': appid,
        'key': api_key,
        'cc': 'us',
        'l': 'en'
    }
    
    # Add delay to respect rate limits
    time.sleep(1.2)
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 429:  # Too Many Requests
            print(f"Rate limited, waiting 60 seconds before retrying...")
            time.sleep(60)
            response = requests.get(url, params=params)
        
        response.raise_for_status()
        data = response.json()
        
        if not data or str(appid) not in data:
            return {
                'name': 'Unknown Game',
                'release_date': None,
                'developer': [],
                'publisher': [],
                'genres': [],
                'categories': [],
                'metacritic_score': None,
                'price_usd': 0.0,
                'supported_languages': []
            }
        
        game_data = data[str(appid)]
        if not game_data['success']:
            return {
                'name': 'Unknown Game',
                'release_date': None,
                'developer': [],
                'publisher': [],
                'genres': [],
                'categories': [],
                'metacritic_score': None,
                'price_usd': 0.0,
                'supported_languages': []
            }
        
        details = game_data['data']
        result = {
            'name': details.get('name', 'Unknown Game'),
            'release_date': details.get('release_date', {}).get('date'),
            'developer': details.get('developers', []),
            'publisher': details.get('publishers', []),
            'genres': [g['description'] for g in details.get('genres', [])],
            'categories': [c['description'] for c in details.get('categories', [])],
            'metacritic_score': details.get('metacritic', {}).get('score'),
            'price_usd': float(details.get('price_overview', {}).get('final', 0)) / 100.0,
            'supported_languages': details.get('supported_languages', [])
        }
        
        # Cache the results
        cache[str(appid)] = result
        with open(cache_file, 'w') as f:
            json.dump(cache, f)
        
        return result
        
    except Exception as e:
        print(f"Error fetching game details for appid {appid}: {str(e)}")
        # Cache default details to avoid repeated failed requests
        default_details = {
            'name': 'Unknown Game',
            'release_date': None,
            'developer': [],
            'publisher': [],
            'genres': [],
            'categories': [],
            'metacritic_score': None,
            'price_usd': 0.0,
            'supported_languages': []
        }
        cache[str(appid)] = default_details
        with open(cache_file, 'w') as f:
            json.dump(cache, f)
        return default_details

def get_top_games(api_key: str, num_games: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch top games from Steam API.
    
    Args:
        api_key (str): Steam API key
        num_games (int): Number of games to fetch (default: 100)
        
    Returns:
        List[Dict[str, Any]]: List of game data dictionaries
    """
    url = "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/"
    params = {
        "key": api_key,
        "format": "json"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        print("API Response:", json.dumps(data, indent=2))  # Debug print
        
        # Extract games data
        games = data.get("response", {}).get("ranks", [])[:num_games]
        
        # Process each game's data
        processed_games = []
        for game in games:
            appid = game.get("appid")
            if not appid:
                continue
                
            # Get game details with caching and rate limiting
            details = get_game_details(appid, api_key)
            if not details:
                print(f"No details found for appid {appid}")
                continue
                
            processed_game = {
                "appid": appid,
                "name": details.get("name", f"Unknown Game ({appid})"),
                "current_players": game.get("concurrent_in_game", 0),
                "peak_players": game.get("peak_in_game", 0),
                "release_date": details.get("release_date"),
                "developer": details.get("developer"),
                "publisher": details.get("publisher"),
                "genres": details.get("genres", []),
                "categories": details.get("categories", []),
                "metacritic_score": details.get("metacritic_score"),
                "price_usd": details.get("price_usd", 0),
                "supported_languages": details.get("supported_languages", []),
                "timestamp": datetime.now()
            }
            processed_games.append(processed_game)
            
        if not processed_games:
            print("No valid games found in the API response")
            return []
            
        return processed_games
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Steam API: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error processing API response: {e}")
        return []

def save_to_db(games):
    """
    Save game data to the database.
    
    Args:
        games (List[Dict]): List of game data dictionaries
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        for game in games:
            # Clean and format supported_languages
            supported_languages = game.get('supported_languages', [])
            if isinstance(supported_languages, str):
                supported_languages = supported_languages.replace('<br>', ',')
                supported_languages = supported_languages.replace('<strong>', '')
                supported_languages = supported_languages.replace('</strong>', '')
                supported_languages = [lang.strip() for lang in supported_languages.split(',') if lang.strip()]
            
            # Clean and format categories
            categories = game.get('categories', [])
            if isinstance(categories, str):
                categories = [cat.strip() for cat in categories.split(',') if cat.strip()]
            
            # Handle release date
            release_date = game.get('release_date', None)
            if release_date and isinstance(release_date, str) and release_date.lower() in ['to be announced', 'coming soon', 'tba']:
                release_date = None
            
            # Insert into games table
            cur.execute("""
                INSERT INTO games (
                    appid, name, current_players, peak_players, release_date,
                    developer, publisher, genres, categories, metacritic_score,
                    price_usd, supported_languages, timestamp
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, CURRENT_TIMESTAMP
                ) ON CONFLICT (appid, timestamp) DO UPDATE SET
                    name = EXCLUDED.name,
                    current_players = EXCLUDED.current_players,
                    peak_players = EXCLUDED.peak_players,
                    release_date = EXCLUDED.release_date,
                    developer = EXCLUDED.developer,
                    publisher = EXCLUDED.publisher,
                    genres = EXCLUDED.genres,
                    categories = EXCLUDED.categories,
                    metacritic_score = EXCLUDED.metacritic_score,
                    price_usd = EXCLUDED.price_usd,
                    supported_languages = EXCLUDED.supported_languages
            """, (
                game.get('appid'),
                game.get('name'),
                game.get('current_players', 0),
                game.get('peak_players', 0),
                release_date,
                (game.get('developer', [None])[0] if isinstance(game.get('developer'), list) and game.get('developer') else game.get('developer')),
                (game.get('publisher', [None])[0] if isinstance(game.get('publisher'), list) and game.get('publisher') else game.get('publisher')),
                game.get('genres', []),
                categories,
                game.get('metacritic_score'),
                game.get('price_usd', 0),
                supported_languages
            ))
            # Save genre relationships
            for genre in game.get('genres', []):
                # Ensure genre exists in genres table
                cur.execute("""
                    INSERT INTO genres (name) VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                """, (genre,))
                # Get genre_id
                cur.execute("SELECT id FROM genres WHERE name = %s", (genre,))
                genre_id = cur.fetchone()
                # Get game_id
                cur.execute("SELECT id FROM games WHERE appid = %s ORDER BY timestamp DESC LIMIT 1", (game.get('appid'),))
                game_id = cur.fetchone()
                if genre_id and game_id:
                    cur.execute("""
                        INSERT INTO game_genres (game_id, genre_id, timestamp)
                        VALUES (%s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (game_id, genre_id, timestamp) DO NOTHING
                    """, (game_id[0], genre_id[0]))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_achievement_stats(appid: int, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch achievement statistics for a game.
    """
    url = f"https://api.steampowered.com/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/"
    params = {
        "gameid": appid,
        "key": api_key
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        achievements = []
        for achievement in data.get("achievementpercentages", {}).get("achievements", []):
            achievements.append({
                "appid": appid,
                "achievement_name": achievement.get("name"),
                "achievement_description": achievement.get("description"),
                "global_percentage": achievement.get("percent"),
                "timestamp": datetime.now()
            })
        return achievements
    except Exception as e:
        print(f"Error fetching achievements for appid {appid}: {e}")
        return []

def get_player_history(appid: int, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch player count history for a game.
    """
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    params = {
        "appid": appid,
        "key": api_key
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return [{
            "appid": appid,
            "player_count": data.get("response", {}).get("player_count", 0),
            "timestamp": datetime.now()
        }]
    except Exception as e:
        print(f"Error fetching player history for appid {appid}: {e}")
        return []

def get_game_news(appid: int, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch latest news for a game.
    """
    url = f"https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
    params = {
        "appid": appid,
        "key": api_key,
        "count": 5,  # Get 5 most recent news items
        "maxlength": 300  # Limit content length
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        news_items = []
        for item in data.get("appnews", {}).get("newsitems", []):
            news_items.append({
                "appid": appid,
                "news_title": item.get("title"),
                "news_content": item.get("contents"),
                "news_url": item.get("url"),
                "publish_date": datetime.fromtimestamp(item.get("date", 0)),
                "timestamp": datetime.now()
            })
        return news_items
    except Exception as e:
        print(f"Error fetching news for appid {appid}: {e}")
        return []

def get_system_requirements(appid: int, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch system requirements for a game.
    """
    url = f"https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "key": api_key,
        "filters": "basic"
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        requirements = []
        if str(appid) in data and data[str(appid)]["success"]:
            game_data = data[str(appid)]["data"]
            platforms = ["windows", "mac", "linux"]
            
            for platform in platforms:
                if platform in game_data.get("platforms", {}):
                    reqs = game_data["platforms"][platform]
                    requirements.append({
                        "appid": appid,
                        "platform": platform,
                        "minimum_requirements": reqs.get("minimum", ""),
                        "recommended_requirements": reqs.get("recommended", ""),
                        "timestamp": datetime.now()
                    })
        return requirements
    except Exception as e:
        print(f"Error fetching system requirements for appid {appid}: {e}")
        return []

def get_user_reviews(appid: int, api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch user review statistics for a game.
    """
    url = f"https://store.steampowered.com/appreviews/{appid}"
    params = {
        "json": 1,
        "language": "all",
        "purchase_type": "all",
        "key": api_key
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get("success", 0) == 1:
            query_summary = data.get("query_summary", {})
            return [{
                "appid": appid,
                "review_score": query_summary.get("review_score"),
                "review_score_desc": query_summary.get("review_score_desc"),
                "total_positive": query_summary.get("total_positive"),
                "total_negative": query_summary.get("total_negative"),
                "total_reviews": query_summary.get("total_reviews"),
                "timestamp": datetime.now()
            }]
        return []
    except Exception as e:
        print(f"Error fetching user reviews for appid {appid}: {e}")
        return []

def get_extended_game_details(appid: int, api_key: str) -> Dict[str, Any]:
    """
    Fetch extended game details including descriptions, images, and support information.
    """
    url = f"https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "key": api_key,
        "cc": "us"  # US region for consistent pricing
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if str(appid) in data and data[str(appid)]["success"]:
            game_data = data[str(appid)]["data"]
            return {
                "appid": appid,
                "detailed_description": game_data.get("detailed_description"),
                "short_description": game_data.get("short_description"),
                "header_image_url": game_data.get("header_image"),
                "background_image_url": game_data.get("background"),
                "website_url": game_data.get("website"),
                "support_url": game_data.get("support_info", {}).get("url"),
                "support_email": game_data.get("support_info", {}).get("email"),
                "age_ratings": json.dumps(game_data.get("age_ratings", {})),
                "controller_support": game_data.get("controller_support", []),
                "dlc_list": [dlc["id"] for dlc in game_data.get("dlc", [])],
                "timestamp": datetime.now()
            }
        return {}
    except Exception as e:
        print(f"Error fetching extended details for appid {appid}: {e}")
        return {}

def get_price_history(appid: int, api_key: str) -> Dict[str, Any]:
    """
    Fetch current price information and discounts.
    """
    url = f"https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "key": api_key,
        "cc": "us"  # US region for consistent pricing
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if str(appid) in data and data[str(appid)]["success"]:
            price_data = data[str(appid)]["data"].get("price_overview", {})
            return {
                "appid": appid,
                "initial_price": price_data.get("initial"),
                "final_price": price_data.get("final"),
                "discount_percent": price_data.get("discount_percent"),
                "currency": price_data.get("currency"),
                "timestamp": datetime.now()
            }
        return {}
    except Exception as e:
        print(f"Error fetching price history for appid {appid}: {e}")
        return {}

def get_community_stats(appid: int, api_key: str) -> Dict[str, Any]:
    """
    Fetch community statistics including workshop items, trading cards, and forum activity.
    """
    url = f"https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "key": api_key
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if str(appid) in data and data[str(appid)]["success"]:
            game_data = data[str(appid)]["data"]
            return {
                "appid": appid,
                "workshop_items_count": game_data.get("workshop_items_count", 0),
                "trading_cards_count": len(game_data.get("trading_cards", [])),
                "forum_topics_count": game_data.get("forum_topics_count", 0),
                "forum_posts_count": game_data.get("forum_posts_count", 0),
                "group_members_count": game_data.get("group_members_count", 0),
                "timestamp": datetime.now()
            }
        return {}
    except Exception as e:
        print(f"Error fetching community stats for appid {appid}: {e}")
        return {}

def get_market_data(appid: int, api_key: str) -> Dict[str, Any]:
    """
    Fetch market data including trading cards and item prices.
    """
    url = f"https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "key": api_key
    }
    
    try:
        time.sleep(1.2)
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if str(appid) in data and data[str(appid)]["success"]:
            game_data = data[str(appid)]["data"]
            cards = game_data.get("trading_cards", [])
            
            # Calculate average card price and total volume
            card_prices = []
            card_volumes = []
            for card in cards:
                if "market_price" in card:
                    card_prices.append(float(card["market_price"]))
                if "market_volume" in card:
                    card_volumes.append(int(card["market_volume"]))
            
            avg_card_price = sum(card_prices) / len(card_prices) if card_prices else None
            total_card_volume = sum(card_volumes) if card_volumes else None
            
            return {
                "appid": appid,
                "card_market_price": avg_card_price,
                "card_market_volume": total_card_volume,
                "item_market_price": game_data.get("market_price"),
                "item_market_volume": game_data.get("market_volume"),
                "market_trend": game_data.get("market_trend"),
                "timestamp": datetime.now()
            }
        return {}
    except Exception as e:
        print(f"Error fetching market data for appid {appid}: {e}")
        return {}

def calculate_genre_benchmarks(api_key: str) -> List[Dict[str, Any]]:
    """
    Calculate and save genre benchmarks.
    
    Args:
        api_key (str): Steam API key
        
    Returns:
        List[Dict]: List of genre benchmark data
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get all games with their genres from the last 24 hours
        cur.execute("""
            WITH latest_games AS (
                SELECT DISTINCT ON (appid) *
                FROM games
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
                ORDER BY appid, timestamp DESC
            )
            SELECT g.appid, g.name, g.peak_players,
                   g.metacritic_score, g.price_usd, gen.name as genre
            FROM latest_games g
            JOIN game_genres gg ON g.id = gg.game_id
            JOIN genres gen ON gg.genre_id = gen.id
        """)
        
        games_data = cur.fetchall()
        
        # Group games by genre
        genre_data = {}
        for game in games_data:
            genre = game[5]  # genre name
            if genre not in genre_data:
                genre_data[genre] = {
                    'total_games': 0,
                    'total_players': 0,
                    'player_counts': [],
                    'review_scores': [],
                    'prices': [],
                    'market_scores': [],
                    'community_scores': [],
                    'dlc_rates': [],
                    'sentiment_scores': []
                }
            
            genre_data[genre]['total_games'] += 1
            if game[2]:  # peak_players
                genre_data[genre]['total_players'] += game[2]
                genre_data[genre]['player_counts'].append(game[2])
            if game[3]:  # metacritic_score
                genre_data[genre]['review_scores'].append(game[3])
            if game[4]:  # price_usd
                genre_data[genre]['prices'].append(game[4])
        
        # Calculate and save benchmarks
        benchmarks = []
        for genre, data in genre_data.items():
            # Calculate averages
            avg_player_count = sum(data['player_counts']) / len(data['player_counts']) if data['player_counts'] else 0
            avg_review_score = sum(data['review_scores']) / len(data['review_scores']) if data['review_scores'] else 0
            avg_price = sum(data['prices']) / len(data['prices']) if data['prices'] else 0
            
            # Normalize values to 0-100 scale
            normalized_player_count = min(avg_player_count / 1000000, 1.0) * 100  # Assuming max 1M players
            normalized_review_score = avg_review_score  # Already on 0-100 scale
            normalized_price = min(avg_price / 100, 1.0) * 100  # Assuming max $100 price
            
            # Calculate market activity score (weighted average of review score and price)
            market_activity_score = (normalized_review_score * 0.7 + normalized_price * 0.3) if (normalized_review_score or normalized_price) else 0
            
            # Calculate community engagement score (weighted average of player count and review score)
            community_engagement_score = (normalized_player_count * 0.8 + normalized_review_score * 0.2) if (normalized_player_count or normalized_review_score) else 0
            
            # Calculate DLC adoption rate (weighted average of player count and price)
            dlc_adoption_rate = (normalized_player_count * 0.5 + normalized_price * 0.5) if (normalized_player_count or normalized_price) else 0
            
            # Calculate sentiment score (weighted average of review score and player count)
            sentiment_score = (normalized_review_score * 0.8 + normalized_player_count * 0.2) if (normalized_review_score or normalized_player_count) else 0
            
            benchmark = {
                'genre': genre,
                'total_games': data['total_games'],
                'total_players': data['total_players'],
                'avg_player_count': avg_player_count,
                'avg_review_score': avg_review_score,
                'avg_price': avg_price,
                'market_activity_score': market_activity_score,
                'community_engagement_score': community_engagement_score,
                'dlc_adoption_rate': dlc_adoption_rate,
                'sentiment_score': sentiment_score
            }
            
            # Save to database
            cur.execute("""
                INSERT INTO genre_benchmarks (
                    genre, total_games, total_players, avg_player_count,
                    avg_review_score, avg_price, market_activity_score,
                    community_engagement_score, dlc_adoption_rate,
                    sentiment_score, timestamp
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (genre, timestamp) DO UPDATE SET
                    total_games = EXCLUDED.total_games,
                    total_players = EXCLUDED.total_players,
                    avg_player_count = EXCLUDED.avg_player_count,
                    avg_review_score = EXCLUDED.avg_review_score,
                    avg_price = EXCLUDED.avg_price,
                    market_activity_score = EXCLUDED.market_activity_score,
                    community_engagement_score = EXCLUDED.community_engagement_score,
                    dlc_adoption_rate = EXCLUDED.dlc_adoption_rate,
                    sentiment_score = EXCLUDED.sentiment_score
            """, (
                benchmark['genre'],
                benchmark['total_games'],
                benchmark['total_players'],
                benchmark['avg_player_count'],
                benchmark['avg_review_score'],
                benchmark['avg_price'],
                benchmark['market_activity_score'],
                benchmark['community_engagement_score'],
                benchmark['dlc_adoption_rate'],
                benchmark['sentiment_score']
            ))
            
            benchmarks.append(benchmark)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return benchmarks
        
    except Exception as e:
        print(f"Error calculating genre benchmarks: {e}")
        if conn:
            conn.rollback()
        raise

def calculate_market_activity_score(card_price: float, item_price: float, review_score: float) -> float:
    """
    Calculate a normalized market activity score based on card and item prices,
    weighted by game quality (review score).
    """
    if not card_price or not item_price or not review_score:
        return 0.0
    
    # Normalize prices to 0-1 range (assuming max prices)
    normalized_card_price = min(card_price / 10.0, 1.0)
    normalized_item_price = min(item_price / 100.0, 1.0)
    normalized_review_score = review_score / 100.0
    
    # Weighted average with review score as quality indicator
    return (normalized_card_price * 0.4 + normalized_item_price * 0.4 + normalized_review_score * 0.2) * 100

def calculate_community_engagement_score(workshop_items: float, forum_posts: float, group_members: float) -> float:
    """
    Calculate a normalized community engagement score based on various metrics.
    """
    if not workshop_items or not forum_posts or not group_members:
        return 0.0
    
    # Normalize metrics to 0-1 range
    normalized_workshop = min(workshop_items / 1000.0, 1.0)
    normalized_forum = min(forum_posts / 10000.0, 1.0)
    normalized_members = min(group_members / 10000.0, 1.0)
    
    # Weighted average
    return (normalized_workshop * 0.4 + normalized_forum * 0.3 + normalized_members * 0.3) * 100

def calculate_dlc_adoption_rate(dlc_count: float, player_count: float) -> float:
    """
    Calculate DLC adoption rate based on number of DLCs and player count.
    """
    if not dlc_count or not player_count:
        return 0.0
    
    # Normalize to 0-1 range
    normalized_dlc = min(dlc_count / 10.0, 1.0)  # Assuming max 10 DLCs
    normalized_players = min(player_count / 100000.0, 1.0)  # Assuming max 100k players
    
    return (normalized_dlc * normalized_players) * 100

def calculate_sentiment_score(positive_ratio: float, review_score: float) -> float:
    """
    Calculate a sentiment score based on positive review ratio and review score.
    """
    if not positive_ratio or not review_score:
        return 0.0
    
    # Normalize to 0-1 range
    normalized_ratio = positive_ratio
    normalized_score = review_score / 100.0
    
    # Weighted average
    return (normalized_ratio * 0.6 + normalized_score * 0.4) * 100

def save_additional_data(appid: int, api_key: str) -> None:
    """
    Save additional game data to the database.
    
    Args:
        appid: Steam app ID
        api_key: Steam API key
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get extended game details
        try:
            details = get_extended_game_details(appid, api_key)
            if details:
                # Convert lists to PostgreSQL arrays and handle empty values
                controller_support = details.get('controller_support', []) or []
                dlc_list = details.get('dlc_list', []) or []
                
                # Convert age ratings to JSONB
                age_ratings = details.get('age_ratings', {})
                if not isinstance(age_ratings, dict):
                    age_ratings = {}
                
                cur.execute("""
                    INSERT INTO extended_game_details (
                        appid, detailed_description, short_description,
                        header_image_url, background_image_url,
                        website_url, support_url, support_email,
                        age_ratings, controller_support, dlc_list
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    ) ON CONFLICT (appid, timestamp) DO UPDATE SET
                        detailed_description = EXCLUDED.detailed_description,
                        short_description = EXCLUDED.short_description,
                        header_image_url = EXCLUDED.header_image_url,
                        background_image_url = EXCLUDED.background_image_url,
                        website_url = EXCLUDED.website_url,
                        support_url = EXCLUDED.support_url,
                        support_email = EXCLUDED.support_email,
                        age_ratings = EXCLUDED.age_ratings,
                        controller_support = EXCLUDED.controller_support,
                        dlc_list = EXCLUDED.dlc_list
                """, (
                    appid,
                    details.get('detailed_description'),
                    details.get('short_description'),
                    details.get('header_image_url'),
                    details.get('background_image_url'),
                    details.get('website_url'),
                    details.get('support_url'),
                    details.get('support_email'),
                    json.dumps(age_ratings),
                    controller_support,
                    dlc_list
                ))
                conn.commit()
        except Exception as e:
            print(f"Error fetching extended details for appid {appid}: {str(e)}")
            conn.rollback()
        
        # Get achievement stats
        try:
            achievements = get_achievement_stats(appid, api_key)
            if achievements:
                for achievement in achievements:
                    cur.execute("""
                        INSERT INTO achievements (
                            appid, achievement_name, achievement_description,
                            global_percentage
                        ) VALUES (
                            %s, %s, %s,
                            %s
                        ) ON CONFLICT (appid, achievement_name, timestamp) DO UPDATE SET
                            achievement_description = EXCLUDED.achievement_description,
                            global_percentage = EXCLUDED.global_percentage
                    """, (
                        appid,
                        achievement.get('name'),
                        achievement.get('description'),
                        achievement.get('percent')
                    ))
                conn.commit()
        except Exception as e:
            print(f"Error fetching achievements for appid {appid}: {str(e)}")
            conn.rollback()
        
        # Get player history
        try:
            player_history = get_player_history(appid, api_key)
            if player_history:
                for history in player_history:
                    cur.execute("""
                        INSERT INTO player_history (
                            appid, player_count
                        ) VALUES (
                            %s, %s
                        ) ON CONFLICT (appid, timestamp) DO UPDATE SET
                            player_count = EXCLUDED.player_count
                    """, (appid, history.get('players')))
                conn.commit()
        except Exception as e:
            print(f"Error fetching player history for appid {appid}: {str(e)}")
            conn.rollback()
        
        # Get game news
        try:
            news = get_game_news(appid, api_key)
            if news:
                for article in news:
                    cur.execute("""
                        INSERT INTO game_news (
                            appid, news_title, news_content,
                            news_url, publish_date
                        ) VALUES (
                            %s, %s, %s,
                            %s, %s
                        ) ON CONFLICT (appid, news_url, timestamp) DO UPDATE SET
                            news_title = EXCLUDED.news_title,
                            news_content = EXCLUDED.news_content,
                            publish_date = EXCLUDED.publish_date
                    """, (
                        appid,
                        article.get('title'),
                        article.get('contents'),
                        article.get('url'),
                        article.get('date')
                    ))
                conn.commit()
        except Exception as e:
            print(f"Error fetching game news for appid {appid}: {str(e)}")
            conn.rollback()
        
        # Get system requirements
        try:
            requirements = get_system_requirements(appid, api_key)
            if requirements:
                for req in requirements:
                    cur.execute("""
                        INSERT INTO system_requirements (
                            appid, platform, minimum_requirements,
                            recommended_requirements
                        ) VALUES (
                            %s, %s, %s,
                            %s
                        ) ON CONFLICT (appid, platform, timestamp) DO UPDATE SET
                            minimum_requirements = EXCLUDED.minimum_requirements,
                            recommended_requirements = EXCLUDED.recommended_requirements
                    """, (
                        appid,
                        req.get('platform'),
                        req.get('minimum'),
                        req.get('recommended')
                    ))
                conn.commit()
        except Exception as e:
            print(f"Error fetching system requirements for appid {appid}: {str(e)}")
            conn.rollback()
        
        # Get user reviews
        try:
            reviews = get_user_reviews(appid, api_key)
            if reviews:
                cur.execute("""
                    INSERT INTO user_reviews (
                        appid, review_score, review_score_desc,
                        total_positive, total_negative, total_reviews
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s
                    ) ON CONFLICT (appid, timestamp) DO UPDATE SET
                        review_score = EXCLUDED.review_score,
                        review_score_desc = EXCLUDED.review_score_desc,
                        total_positive = EXCLUDED.total_positive,
                        total_negative = EXCLUDED.total_negative,
                        total_reviews = EXCLUDED.total_reviews
                """, (
                    appid,
                    reviews.get('review_score'),
                    reviews.get('review_score_desc'),
                    reviews.get('total_positive'),
                    reviews.get('total_negative'),
                    reviews.get('total_reviews')
                ))
                conn.commit()
        except Exception as e:
            print(f"Error fetching user reviews for appid {appid}: {str(e)}")
            conn.rollback()
        
        # Get price history
        try:
            price_history = get_price_history(appid, api_key)
            if price_history:
                cur.execute("""
                    INSERT INTO price_history (
                        appid, initial_price, final_price,
                        discount_percent, currency
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s
                    ) ON CONFLICT (appid, timestamp) DO UPDATE SET
                        initial_price = EXCLUDED.initial_price,
                        final_price = EXCLUDED.final_price,
                        discount_percent = EXCLUDED.discount_percent,
                        currency = EXCLUDED.currency
                """, (
                    appid,
                    price_history.get('initial_price'),
                    price_history.get('final_price'),
                    price_history.get('discount_percent'),
                    price_history.get('currency')
                ))
                conn.commit()
        except Exception as e:
            print(f"Error fetching price history for appid {appid}: {str(e)}")
            conn.rollback()
        
        # Get community stats
        try:
            stats = get_community_stats(appid, api_key)
            if stats:
                cur.execute("""
                    INSERT INTO community_stats (
                        appid, workshop_items_count, trading_cards_count,
                        forum_topics_count, forum_posts_count, group_members_count
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s
                    ) ON CONFLICT (appid, timestamp) DO UPDATE SET
                        workshop_items_count = EXCLUDED.workshop_items_count,
                        trading_cards_count = EXCLUDED.trading_cards_count,
                        forum_topics_count = EXCLUDED.forum_topics_count,
                        forum_posts_count = EXCLUDED.forum_posts_count,
                        group_members_count = EXCLUDED.group_members_count
                """, (
                    appid,
                    stats.get('workshop_items_count'),
                    stats.get('trading_cards_count'),
                    stats.get('forum_topics_count'),
                    stats.get('forum_posts_count'),
                    stats.get('group_members_count')
                ))
                conn.commit()
        except Exception as e:
            print(f"Error fetching community stats for appid {appid}: {str(e)}")
            conn.rollback()
        
        # Get market data
        try:
            market_data = get_market_data(appid, api_key)
            if market_data:
                cur.execute("""
                    INSERT INTO market_data (
                        appid, card_market_price, card_market_volume,
                        item_market_price, item_market_volume, market_trend
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s
                    ) ON CONFLICT (appid, timestamp) DO UPDATE SET
                        card_market_price = EXCLUDED.card_market_price,
                        card_market_volume = EXCLUDED.card_market_volume,
                        item_market_price = EXCLUDED.item_market_price,
                        item_market_volume = EXCLUDED.item_market_volume,
                        market_trend = EXCLUDED.market_trend
                """, (
                    appid,
                    market_data.get('card_market_price'),
                    market_data.get('card_market_volume'),
                    market_data.get('item_market_price'),
                    market_data.get('item_market_volume'),
                    market_data.get('market_trend')
                ))
                conn.commit()
        except Exception as e:
            print(f"Error fetching market data for appid {appid}: {str(e)}")
            conn.rollback()
        
        print(f"Successfully saved additional data for appid {appid}")
        
    except Exception as e:
        print(f"Error saving additional data for appid {appid}: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def collect_and_save():
    """Main function to collect and save all data"""
    try:
        # Initialize database
        logging.info("Initializing database...")
        init_db()
        
        # Get top games
        logging.info("Fetching top games...")
        top_games = get_top_games()
        logging.info(f"Found {len(top_games)} top games")
        
        # Process each game with progress bar
        logging.info("Processing game data...")
        for game in tqdm(top_games, desc="Processing games"):
            try:
                appid = game['appid']
                name = game['name']
                
                # Get basic game details
                logging.debug(f"Processing game: {name} (ID: {appid})")
                game_details = get_game_details(appid)
                if game_details:
                    save_to_db(game_details)
                
                # Get additional data
                logging.debug(f"Fetching additional data for {name}")
                save_additional_data(appid, game_details)
                
            except Exception as e:
                logging.error(f"Error processing game {game.get('name', 'Unknown')}: {str(e)}", exc_info=True)
                continue
        
        # Calculate genre benchmarks
        logging.info("Calculating genre benchmarks...")
        calculate_genre_benchmarks()
        logging.info("Genre benchmarks calculated successfully")
        
        logging.info("Data collection completed successfully")
        
    except Exception as e:
        logging.error(f"Error in data collection: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    collect_and_save() 