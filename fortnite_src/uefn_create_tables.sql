-- Drop existing tables if they exist
DROP TABLE IF EXISTS uefn_game_tags;
DROP TABLE IF EXISTS uefn_game_metrics;
DROP TABLE IF EXISTS uefn_top_games;

-- Create UEFN tables
CREATE TABLE IF NOT EXISTS uefn_top_games (
    game_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    creator_name VARCHAR(255),
    display_name VARCHAR(255),
    created_in VARCHAR(50),
    category VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS uefn_game_metrics (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(255) REFERENCES uefn_top_games(game_id),
    date DATE NOT NULL,
    plays INTEGER DEFAULT 0,
    unique_players INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,
    favorites INTEGER DEFAULT 0,
    recommendations INTEGER DEFAULT 0,
    average_minutes_per_player FLOAT DEFAULT 0,
    peak_ccu INTEGER DEFAULT 0,
    retention_d1 FLOAT DEFAULT 0,
    retention_d7 FLOAT DEFAULT 0,
    metrics_timestamp TIMESTAMP,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, date)
);

-- Create UEFN_game_tags table for game categorization
CREATE TABLE IF NOT EXISTS uefn_game_tags (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(255) REFERENCES uefn_top_games(game_id),
    tag_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, tag_name)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_uefn_game_metrics_timestamp ON uefn_game_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_uefn_game_metrics_game_id ON uefn_game_metrics(game_id);
CREATE INDEX IF NOT EXISTS idx_uefn_game_tags_game_id ON uefn_game_tags(game_id); 