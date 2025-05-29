-- Create genres table
CREATE TABLE IF NOT EXISTS genres (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create games table
CREATE TABLE IF NOT EXISTS games (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    current_players INTEGER,
    peak_players INTEGER,
    release_date DATE,
    developer VARCHAR(255),
    publisher VARCHAR(255),
    categories TEXT[],
    metacritic_score INTEGER,
    price_usd DECIMAL(10,2),
    supported_languages TEXT[],
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, timestamp)
);

-- Create game_genres junction table
CREATE TABLE IF NOT EXISTS game_genres (
    id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES games(id),
    genre_id INTEGER REFERENCES genres(id),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, genre_id, timestamp)
);

-- Create achievements table
CREATE TABLE IF NOT EXISTS achievements (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    achievement_name VARCHAR(255) NOT NULL,
    achievement_description TEXT,
    global_percentage DECIMAL(5,2),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, achievement_name, timestamp)
);

-- Create player_history table
CREATE TABLE IF NOT EXISTS player_history (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    player_count INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, timestamp)
);

-- Create game_news table
CREATE TABLE IF NOT EXISTS game_news (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    news_title TEXT,
    news_content TEXT,
    news_url TEXT,
    publish_date TIMESTAMP WITH TIME ZONE,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, news_url, timestamp)
);

-- Create system_requirements table
CREATE TABLE IF NOT EXISTS system_requirements (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    platform VARCHAR(50) NOT NULL,
    minimum_requirements TEXT,
    recommended_requirements TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, platform, timestamp)
);

-- Create user_reviews table
CREATE TABLE IF NOT EXISTS user_reviews (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    review_score INTEGER,
    review_score_desc VARCHAR(50),
    total_positive INTEGER,
    total_negative INTEGER,
    total_reviews INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, timestamp)
);

-- Create market_data table
CREATE TABLE IF NOT EXISTS market_data (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    card_market_price DECIMAL(10,2),
    card_market_volume INTEGER,
    item_market_price DECIMAL(10,2),
    item_market_volume INTEGER,
    market_trend VARCHAR(50),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, timestamp)
);

-- Create extended_game_details table
CREATE TABLE IF NOT EXISTS extended_game_details (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    detailed_description TEXT,
    short_description TEXT,
    header_image_url TEXT,
    background_image_url TEXT,
    website_url TEXT,
    support_url TEXT,
    support_email TEXT,
    age_ratings JSONB,
    controller_support TEXT[],
    dlc_list INTEGER[],
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, timestamp)
);

-- Create community_stats table
CREATE TABLE IF NOT EXISTS community_stats (
    id SERIAL PRIMARY KEY,
    appid INTEGER NOT NULL,
    workshop_items_count INTEGER,
    trading_cards_count INTEGER,
    forum_topics_count INTEGER,
    forum_posts_count INTEGER,
    group_members_count INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(appid, timestamp)
);

-- Create genre_benchmarks table
CREATE TABLE IF NOT EXISTS genre_benchmarks (
    id SERIAL PRIMARY KEY,
    genre VARCHAR(100) NOT NULL,
    avg_player_count DECIMAL(10,2),
    avg_review_score DECIMAL(5,2),
    avg_price DECIMAL(10,2),
    avg_achievement_completion DECIMAL(5,2),
    total_games INTEGER,
    total_players INTEGER,
    -- Market Trends
    avg_card_market_price DECIMAL(10,2),
    avg_item_market_price DECIMAL(10,2),
    market_activity_score DECIMAL(5,2),
    -- Community Engagement
    avg_workshop_items DECIMAL(10,2),
    avg_forum_posts DECIMAL(10,2),
    avg_group_members DECIMAL(10,2),
    community_engagement_score DECIMAL(5,2),
    -- DLC Performance
    avg_dlc_count DECIMAL(5,2),
    dlc_adoption_rate DECIMAL(5,2),
    -- Update Frequency
    avg_update_frequency DECIMAL(5,2),
    days_since_last_update INTEGER,
    -- Review Sentiment
    positive_review_ratio DECIMAL(5,2),
    avg_review_length INTEGER,
    sentiment_score DECIMAL(5,2),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(genre, timestamp)
);

-- Remove genres column from games table if it exists
ALTER TABLE games DROP COLUMN IF EXISTS genres; 