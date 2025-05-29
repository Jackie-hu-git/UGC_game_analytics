import os
import psycopg2
import pandas as pd

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "steam"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

def check_tables():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get list of all tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = cur.fetchall()
    print("\nAvailable tables:")
    for table in tables:
        print(f"- {table[0]}")
    
    # Check data in genre_benchmarks
    print("\nChecking genre_benchmarks data:")
    cur.execute("""
        SELECT COUNT(*), MAX(timestamp) 
        FROM genre_benchmarks
    """)
    count, latest_timestamp = cur.fetchone()
    print(f"Total records: {count}")
    print(f"Latest timestamp: {latest_timestamp}")
    
    if count > 0:
        print("\nSample data from genre_benchmarks:")
        cur.execute("""
            SELECT genre, total_games, total_players, avg_player_count
            FROM genre_benchmarks
            WHERE timestamp = (
                SELECT MAX(timestamp) FROM genre_benchmarks
            )
        """)
        df = pd.DataFrame(cur.fetchall(), columns=['genre', 'total_games', 'total_players', 'avg_player_count'])
        print(df)
    
    # Check data in games table
    print("\nChecking games data:")
    cur.execute("""
        SELECT COUNT(*), MAX(timestamp) 
        FROM games
    """)
    count, latest_timestamp = cur.fetchone()
    print(f"Total records: {count}")
    print(f"Latest timestamp: {latest_timestamp}")
    
    if count > 0:
        print("\nSample data from games:")
        cur.execute("""
            SELECT name, current_players, peak_players, release_date
            FROM games
            WHERE timestamp = (
                SELECT MAX(timestamp) FROM games
            )
            LIMIT 5
        """)
        df = pd.DataFrame(cur.fetchall(), columns=['name', 'current_players', 'peak_players', 'release_date'])
        print(df)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_tables() 