import schedule
import time
from data_collector import get_top_games, save_to_db, calculate_genre_benchmarks, init_db, get_db_connection
import os
from datetime import datetime
import sys
import traceback
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('collector.log')
    ]
)

def collect_and_save():
    """Collect data and save to database"""
    conn = None
    try:
        logging.info(f"Starting data collection at {datetime.now()}")
        api_key = os.getenv("STEAM_API_KEY")
        if not api_key:
            logging.error("STEAM_API_KEY environment variable not set")
            return
        
        # Initialize database
        logging.info("Initializing database...")
        try:
            init_db()
            logging.info("Database initialized successfully")
        except Exception as e:
            logging.error(f"Database initialization failed: {str(e)}")
            logging.error(traceback.format_exc())
            return
        
        # Get top games and save basic data
        logging.info("Collecting top games...")
        try:
            games = get_top_games(api_key)
            logging.info(f"Found {len(games)} games")
            save_to_db(games)
            logging.info("Basic game data saved")
        except Exception as e:
            logging.error(f"Error collecting or saving game data: {str(e)}")
            logging.error(traceback.format_exc())
            return
        
        # Calculate and save genre benchmarks
        logging.info("Calculating genre benchmarks...")
        try:
            benchmarks = calculate_genre_benchmarks(api_key)
            logging.info(f"Calculated benchmarks for {len(benchmarks)} genres")
        except Exception as e:
            logging.error(f"Error calculating genre benchmarks: {str(e)}")
            logging.error(traceback.format_exc())
            return
        
        logging.info(f"Data collection completed successfully at {datetime.now()}")
        
    except Exception as e:
        logging.error(f"Unexpected error in collect_and_save: {str(e)}")
        logging.error(traceback.format_exc())
    finally:
        if conn:
            try:
                conn.close()
                logging.info("Database connection closed")
            except Exception as e:
                logging.error(f"Error closing database connection: {str(e)}")

def main():
    logging.info("Starting data collection process")
    cycle_count = 0
    
    try:
        # Run first collection immediately
        cycle_count += 1
        logging.info(f"\n=== Starting Initial Collection ===")
        collect_and_save()
        logging.info("Initial collection completed")
        
        # Schedule hourly collections
        while True:
            cycle_count += 1
            start_time = time.time()
            logging.info(f"\n=== Starting Collection Cycle {cycle_count} ===")
            
            try:
                # Collect and save data
                logging.info("Initializing data collection...")
                collect_and_save()
                
                # Calculate duration
                duration = time.time() - start_time
                logging.info(f"=== Collection Cycle {cycle_count} Completed ===")
                logging.info(f"Duration: {duration:.2f} seconds")
                logging.info(f"Next collection in 1 hour")
                
            except Exception as e:
                logging.error(f"Error in collection cycle {cycle_count}: {str(e)}", exc_info=True)
                logging.error("Stack trace:", exc_info=True)
            
            # Wait for 1 hour before next collection
            logging.info("Waiting for next collection cycle...")
            time.sleep(3600)
            
    except KeyboardInterrupt:
        logging.info("Data collection process stopped by user")
    except Exception as e:
        logging.error(f"Fatal error in data collection process: {str(e)}", exc_info=True)
    finally:
        logging.info("Data collection process terminated")

if __name__ == "__main__":
    main() 