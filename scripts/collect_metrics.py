# scripts/collect_metrics.py
import psycopg2
import configparser
import os
import csv
from datetime import datetime

def get_db_connection(config):
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=config['postgresql']['host'],
            port=config['postgresql']['port'],
            database=config['postgresql']['database'],
            user=config['postgresql']['user'],
            password=config['postgresql']['password'],
            connect_timeout=5
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def collect_query_stats(conn):
    """Collects query statistics using pg_stat_statements."""
    try:
        with conn.cursor() as cur:
            # Check if pg_stat_statements extension is enabled
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
            if not cur.fetchone():
                print("Warning: pg_stat_statements extension is not enabled. Skipping query stats.")
                return []

            # Get current timestamp for the report
            import time
            start_time = time.time()
            
            print("\n=== Query Collection Started ===")
            print("1. First, we'll reset the query statistics")
            cur.execute("SELECT pg_stat_statements_reset()")
            
            print("2. Now, please run your application queries for the next 60 seconds...")
            print("   (This gives you time to trigger the custom queries you want to profile)")
            print("   Press Ctrl+C to stop early if you've run your queries\n")
            
            try:
                # Wait for 60 seconds to capture queries
                for i in range(60, 0, -1):
                    print(f"\rTime remaining: {i} seconds (or press Ctrl+C to stop) ", end="")
                    time.sleep(1)
                print("\n")
            except KeyboardInterrupt:
                print("\n\nCapture stopped early. Processing collected data...\n")
            
            # Get the top 100 queries by total execution time
            cur.execute("""
                SELECT
                    query,
                    calls,
                    ROUND(total_exec_time::numeric, 2) as total_ms,
                    ROUND(mean_exec_time::numeric, 2) as avg_ms,
                    ROUND(min_exec_time::numeric, 2) as min_ms,
                    ROUND(max_exec_time::numeric, 2) as max_ms,
                    ROUND((total_exec_time / NULLIF(calls, 0) / 1000)::numeric, 4) as avg_seconds
                FROM
                    pg_stat_statements
                WHERE
                    -- Exclude PostgreSQL internal queries and system tables
                    query NOT LIKE '%pg_%' 
                    AND query NOT LIKE '%information_schema%'
                    AND query NOT LIKE '%pg_catalog%'
                    -- Include only SELECT, INSERT, UPDATE, DELETE queries
                    AND (
                        query ILIKE 'SELECT%' 
                        OR query ILIKE 'INSERT%' 
                        OR query ILIKE 'UPDATE%' 
                        OR query ILIKE 'DELETE%'
                    )
                ORDER BY
                    total_exec_time DESC
                LIMIT 100;
            """)
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error collecting query stats: {e}")
        return []

def collect_database_stats(conn):
    """Collects basic database statistics."""
    try:
        with conn.cursor() as cur:
            # Get cache hit ratio
            cur.execute("""
                SELECT
                    ROUND((blks_hit * 100.0) / NULLIF((blks_read + blks_hit), 0), 2) AS cache_hit_ratio
                FROM
                    pg_stat_database
                WHERE
                    datname = current_database();
            """)
            cache_hit_ratio = cur.fetchone()[0] or 0
            
            # Get database size
            cur.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database()))
            """)
            db_size = cur.fetchone()[0]
            
            return {
                "cache_hit_ratio": cache_hit_ratio,
                "database_size": db_size
            }
    except psycopg2.Error as e:
        print(f"Error collecting database stats: {e}")
        return {"cache_hit_ratio": 0, "database_size": "N/A"}





def collect_table_sizes(conn):
    """Collects sizes of user tables and their indexes."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    n.nspname as schema_name,
                    c.relname as table_name,
                    pg_size_pretty(pg_relation_size(c.oid)) as table_size,
                    pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
                    pg_size_pretty(pg_indexes_size(c.oid)) as index_size,
                    pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid)) as toast_size
                FROM
                    pg_class c
                LEFT JOIN 
                    pg_namespace n ON n.oid = c.relnamespace
                WHERE 
                    c.relkind = 'r'
                    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                    AND n.nspname !~ '^pg_toast'
                ORDER BY 
                    pg_total_relation_size(c.oid) DESC;
            """)
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error collecting table sizes: {e}")
        return []

def save_to_csv(data, filename, header):
    """Saves collected data to a CSV file."""
    filepath = os.path.join(os.path.dirname(__file__), '..', 'reports', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(data)
        print(f"Report saved to {filepath}")
    except IOError as e:
        print(f"Error saving report: {e}")

def generate_markdown_report(query_stats, db_stats, table_sizes, timestamp):
    """Generates a markdown report with the collected metrics."""
    report_path = os.path.join(os.path.dirname(__file__), '..', 'reports', f'performance_report_{timestamp}.md')
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, 'w') as f:
        # Database Summary
        f.write("# Database Performance Report\n\n")
        f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Database Summary\n")
        f.write(f"- **Database Size:** {db_stats['database_size']}\n")
        f.write(f"- **Cache Hit Ratio:** {db_stats['cache_hit_ratio']}%\n\n")
        
        # Query Performance
        if query_stats:
            f.write("## Top Slow Queries\n")
            f.write("| Query | Calls | Total Time (ms) | Avg (ms) | Min (ms) | Max (ms) |\n")
            f.write("|-------|-------|----------------|----------|----------|----------|\n")
            for row in query_stats:
                query = row[0].replace('|', '│')  # Replace pipe in query to avoid markdown issues
                query = query[:100] + '...' if len(query) > 100 else query
                f.write(f"| `{query}` | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |\n")
            f.write("\n")
        
        # Table Sizes
        if table_sizes:
            f.write("## Table Sizes\n")
            f.write("| Schema | Table | Table Size | Total Size | Index Size | TOAST Size |\n")
            f.write("|--------|-------|------------|------------|------------|------------|\n")
            for row in table_sizes:
                f.write(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |\n")
    
    print(f"Generated report: {report_path}")

def main():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_path)
    
    print("Starting database performance analysis...")
    
    conn = get_db_connection(config)
    if not conn:
        print("Failed to connect to the database. Check your configuration in config.ini")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Collect database statistics
        print("Collecting database statistics...")
        db_stats = collect_database_stats(conn)
        
        # Collect query statistics
        print("Collecting query performance data...")
        query_stats = collect_query_stats(conn)
        
        # Collect table sizes
        print("Collecting table size information...")
        table_sizes = collect_table_sizes(conn)
        
        # Generate report
        print("Generating report...")
        generate_markdown_report(query_stats, db_stats, table_sizes, timestamp)
        
        # Save raw data for reference
        if query_stats:
            save_to_csv(
                query_stats, 
                f"query_stats_{timestamp}.csv",
                ['query', 'calls', 'total_ms', 'avg_ms', 'min_ms', 'max_ms', 'avg_seconds']
            )
            
        if table_sizes:
            save_to_csv(
                table_sizes,
                f"table_sizes_{timestamp}.csv",
                ['schema', 'table_name', 'table_size', 'total_size', 'index_size', 'toast_size']
            )
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
        print("Analysis completed.")

if __name__ == "__main__":
    main()