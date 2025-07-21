# PostgreSQL Performance Profiler

A simple tool to collect and report on PostgreSQL database performance metrics.

## Features

- Collects key database metrics (queries, connections, resource usage)
- Generates performance reports
- Configurable for any PostgreSQL database
- Easy setup and execution

## Prerequisites

- Python 3.x
- PostgreSQL with `pg_stat_statements` extension enabled

### PostgreSQL Setup

1. In `postgresql.conf`:
   ```
   shared_preload_libraries = 'pg_stat_statements'
   ```
   Then restart PostgreSQL.

2. In your database:
   ```sql
   CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
   ```

## Project Structure

```
pg_profiler/
├── config/
│   └── config.ini
├── scripts/
│   ├── collect_metrics.py
│   └── analyze_data.py
├── data/
│   └── raw/
├── reports/
├── logs/
├── main.py
└── requirements.txt
```

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd pg_profiler
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Edit `config/config.ini` with your PostgreSQL connection details:

```ini
[postgresql]
host = localhost
port = 5432
database = mydatabase
user = myuser
password = mypassword
```

Adjust other settings in the configuration file as needed.

## Usage

1. Ensure your PostgreSQL server is running and has some activity.
2. Run the profiler:
   ```bash
   python main.py
   ```

## Output

- **Reports**: Generated in `reports/` directory (e.g., `baseline_report_*.md`)
- **Raw Data**: CSV files stored in `data/raw/`
- **Logs**: Script execution logs in `logs/`

## Requirements

Listed in `requirements.txt`:
- psycopg2-binary
- pandas
- tabulate
