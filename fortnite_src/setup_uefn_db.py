import os
import sys
from sqlalchemy import create_engine, text
import logging

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
        
        if not all([db_user, db_password, db_host, db_name]):
            logger.error("Missing database credentials in environment variables")
            sys.exit(1)
        
        # Create database URL
        database_url = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
        
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