-- Create item_types table
CREATE TABLE IF NOT EXISTS item_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create items table
CREATE TABLE IF NOT EXISTS items (
    id SERIAL PRIMARY KEY,
    item_id VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    rarity VARCHAR(50),
    price_vbucks INTEGER,
    release_date DATE,
    description TEXT,
    image_url TEXT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(item_id, timestamp)
);

-- Create item_types junction table
CREATE TABLE IF NOT EXISTS item_type_relations (
    id SERIAL PRIMARY KEY,
    item_id INTEGER REFERENCES items(id),
    type_id INTEGER REFERENCES item_types(id),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(item_id, type_id, timestamp)
);

-- Create shop_history table
CREATE TABLE IF NOT EXISTS shop_history (
    id SERIAL PRIMARY KEY,
    item_id INTEGER REFERENCES items(id),
    price_vbucks INTEGER,
    featured BOOLEAN,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(item_id, timestamp)
);

-- Create item_stats table
CREATE TABLE IF NOT EXISTS item_stats (
    id SERIAL PRIMARY KEY,
    item_id INTEGER REFERENCES items(id),
    popularity_score DECIMAL(5,2),
    usage_rate DECIMAL(5,2),
    win_rate DECIMAL(5,2),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(item_id, timestamp)
);

-- Create item_benchmarks table
CREATE TABLE IF NOT EXISTS item_benchmarks (
    id SERIAL PRIMARY KEY,
    item_type VARCHAR(100) NOT NULL,
    avg_price_vbucks DECIMAL(10,2),
    avg_popularity_score DECIMAL(5,2),
    avg_usage_rate DECIMAL(5,2),
    avg_win_rate DECIMAL(5,2),
    total_items INTEGER,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(item_type, timestamp)
); 