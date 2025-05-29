-- Create UEFN_top_games table
CREATE TABLE IF NOT EXISTS uefn_top_games (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(100) NOT NULL UNIQUE,
    title VARCHAR(255) NOT NULL,
    creator_name VARCHAR(255),
    creator_id VARCHAR(100),
    description TEXT,
    thumbnail_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create UEFN_game_metrics table for storing daily metrics
CREATE TABLE IF NOT EXISTS uefn_game_metrics (
    id SERIAL PRIMARY KEY,
    game_id VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    -- Core metrics
    plays INTEGER DEFAULT 0,
    unique_players INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,
    favorites INTEGER DEFAULT 0,
    recommendations INTEGER DEFAULT 0,
    average_minutes_per_player DECIMAL(10,2) DEFAULT 0,
    peak_ccu INTEGER DEFAULT 0,
    -- Retention metrics
    retention_d1 DECIMAL(5,2) DEFAULT 0,
    retention_d7 DECIMAL(5,2) DEFAULT 0,
    -- Timestamps
    metrics_timestamp TIMESTAMP WITH TIME ZONE,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, date)
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
CREATE INDEX IF NOT EXISTS idx_uefn_top_games_game_id ON uefn_top_games(game_id);
CREATE INDEX IF NOT EXISTS idx_uefn_game_metrics_game_id ON uefn_game_metrics(game_id);
CREATE INDEX IF NOT EXISTS idx_uefn_game_metrics_date ON uefn_game_metrics(date);
CREATE INDEX IF NOT EXISTS idx_uefn_game_tags_game_id ON uefn_game_tags(game_id); 