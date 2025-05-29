-- Create UEFN tables
CREATE TABLE IF NOT EXISTS uefn_top_games (
    game_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    creator_name VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS uefn_game_metrics (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(255) REFERENCES uefn_top_games(game_id),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    plays INTEGER,
    unique_players INTEGER,
    minutes_played INTEGER,
    favorites INTEGER,
    recommendations INTEGER,
    average_minutes_per_player FLOAT,
    peak_ccu INTEGER,
    retention_d1 FLOAT,
    retention_d7 FLOAT,
    UNIQUE(game_id, timestamp)
);

-- Create UEFN_game_tags table for game categorization
CREATE TABLE IF NOT EXISTS uefn_game_tags (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(100) NOT NULL,
    tag_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, tag_name)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_uefn_game_metrics_timestamp ON uefn_game_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_uefn_game_metrics_game_id ON uefn_game_metrics(game_id);
CREATE INDEX IF NOT EXISTS idx_uefn_game_tags_game_id ON uefn_game_tags(game_id); 