# Steam Analytics Dashboard

A analytics dashboard for Steam games, providing insights into player counts, game metrics, and genre performance.

## Features

- Data collection from Steam API
- Interactive dashboard with multiple visualizations
- Genre performance metrics and analysis
- Game-specific analytics and trends
- DLC adoption and pricing analysis
- Sentiment analysis based on reviews and player engagement

## Prerequisites

- Python 3.8+
- PostgreSQL
- Steam Web API key

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/steam_analytics.git
cd steam_analytics
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
Create a `.env` file in the project root with the following variables:
```
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_NAME=steam
STEAM_API_KEY=your_steam_api_key
```

5. Initialize the database:
```bash
python src/init_db.py
```

## Usage

1. Start the data collector:
```bash
python steam_src/steam_data_collector.py
```

2. Start the dashboard:
```bash
python steam_dashboard/steam_app.py
```

3. Access the dashboard at `http://localhost:8051`

## Project Structure

```
steam_analytics/
├── steam_dashboard/
│   └── steam_app.py              # Dash application
├── steam_src/
│   ├── steam_data_collector.py   # Data collection from Steam API
│   ├── steam_init_db.py         # Database initialization
│   └── steam_utils.py           # Utility functions
├── logs/                   # Log files
├── requirements.txt        # Python dependencies
├── run_all.sh             # Script to start all services
└── README.md              # This file
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -am 'Add your feature'`
4. Push to the branch: `git push origin feature/your-feature`
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Steam Web API for providing game data
- Dash and Plotly for the interactive dashboard
- PostgreSQL for data storage 