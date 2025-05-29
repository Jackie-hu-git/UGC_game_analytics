import os
import sys
from sqlalchemy import create_engine, text
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_database():
    try:
        # Get database credentials from environment variables
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        
        # Debug: Print environment variables (without sensitive data)
        logger.info(f"DB_HOST: {db_host}")
        logger.info(f"DB_NAME: {db_name}")
        logger.info(f"DB_USER exists: {bool(db_user)}")
        logger.info(f"DB_PASSWORD exists: {bool(db_password)}")
        
        if not all([db_user, db_password, db_host, db_name]):
            logger.error("Missing database credentials in environment variables")
            sys.exit(1)
        
        # Complete the host name with Render's domain
        full_host = f"{db_host}.oregon-postgres.render.com"
        
        # Create database URL with port 5432
        database_url = f"postgresql://{db_user}:{db_password}@{full_host}:5432/{db_name}"
        logger.info(f"Attempting to connect to database at {full_host}")
        
        # Create engine
        engine = create_engine(database_url)
        
        # Read and execute SQL file
        with open('fortnite_src/uefn_create_tables.sql', 'r') as file:
            sql_commands = file.read()
            
        with engine.connect() as conn:
            # Execute each command separately
            for command in sql_commands.split(';'):
                if command.strip():
                    try:
                        conn.execute(text(command))
                        conn.commit()
                        logger.info(f"Successfully executed SQL command: {command[:100]}...")
                    except Exception as e:
                        logger.error(f"Error executing SQL command: {str(e)}")
                        logger.error(f"Command was: {command}")
        
        logger.info("Database setup completed successfully")
        
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    setup_database() 