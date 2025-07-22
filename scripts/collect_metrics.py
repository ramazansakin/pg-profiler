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
            password=config['postgresql']['password']
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

            cur.execute("""
                SELECT
                    query,
                    calls,
                    total_exec_time,
                    mean_exec_time,
                    min_exec_time,
                    max_exec_time,
                    blk_read_time,
                    blk_write_time
                FROM
                    pg_stat_statements
                ORDER BY
                    total_exec_time DESC
                LIMIT 100;
            """)
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error collecting query stats: {e}")
        return []

def collect_resource_usage(conn):
    """Collects general resource usage from pg_stat_database and pg_stat_bgwriter."""
    try:
        with conn.cursor() as cur:
            # pg_stat_database for cache hit ratio
            cur.execute("""
                SELECT
                    datname,
                    blks_read,
                    blks_hit,
                    (blks_hit * 100) / (blks_read + blks_hit) AS hit_ratio
                FROM
                    pg_stat_database
                WHERE
                    datname = current_database();
            """)
            db_stats = cur.fetchone()

            # pg_stat_bgwriter for buffer activity
            cur.execute("""
                SELECT
                    checkpoints_timed,
                    checkpoints_req,
                    buffers_alloc,
                    buffers_backend,
                    buffers_backend_fsync,
                    buffers_checkpoint,
                    buffers_clean,
                    maxwritten_clean,
                    (buffers_alloc / (EXTRACT(EPOCH FROM (now() - stats_reset))))::numeric(10,2) AS buff_alloc_rate,
                    ((checkpoints_timed + checkpoints_req) / (EXTRACT(EPOCH FROM (now() - stats_reset))))::numeric(10,2) AS checkpoints_rate
                FROM
                    pg_stat_bgwriter;
            """)
            bgwriter_stats = cur.fetchone()

            return {
                "db_stats": db_stats,
                "bgwriter_stats": bgwriter_stats
            }
    except psycopg2.Error as e:
        print(f"Error collecting resource usage: {e}")
        return {}

def collect_connection_info(conn):
    """Collects information about active connections."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    datname,
                    usename,
                    application_name,
                    client_addr,
                    state,
                    query,
                    backend_start,
                    query_start,
                    state_change
                FROM
                    pg_stat_activity
                WHERE
                    pid <> pg_backend_pid();
            """)
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error collecting connection info: {e}")
        return []

def collect_lock_info(conn):
    """Collects information about database locks."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pg_locks.pid,
                    pg_locks.mode,
                    pg_locks.granted,
                    pg_locks.waitstart,
                    pg_class.relname AS relation,
                    pg_stat_activity.query AS blocking_query
                FROM
                    pg_locks
                JOIN
                    pg_stat_activity ON pg_locks.pid = pg_stat_activity.pid
                LEFT JOIN
                    pg_class ON pg_locks.relation = pg_class.oid
                WHERE
                    pg_locks.mode IS NOT NULL AND pg_locks.pid <> pg_backend_pid();
            """)
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error collecting lock info: {e}")
        return []

def collect_table_sizes(conn):
    """Collects sizes of tables and indexes."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    relname AS table_name,
                    pg_size_pretty(pg_relation_size(oid)) AS table_size,
                    pg_size_pretty(pg_total_relation_size(oid)) AS total_size,
                    pg_size_pretty(pg_indexes_size(oid)) AS indexes_size
                FROM
                    pg_class
                WHERE
                    relkind = 'r'
                ORDER BY
                    pg_total_relation_size(oid) DESC;
            """)
            return cur.fetchall()
    except psycopg2.Error as e:
        print(f"Error collecting table sizes: {e}")
        return []

def save_to_csv(data, filename, header):
    """Saves collected data to a CSV file."""
    filepath = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(data)
        print(f"Data saved to {filepath}")
    except IOError as e:
        print(f"Error saving data to CSV: {e}")

def main():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_path)

    conn = get_db_connection(config)
    if not conn:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    metrics_to_collect = [m.strip() for m in config['collection']['metrics_to_collect'].split(',') if m.strip()]

    if 'query_stats' in metrics_to_collect:
        query_stats = collect_query_stats(conn)
        if query_stats:
            query_stats_header = ['query', 'calls', 'total_exec_time', 'mean_exec_time', 'min_exec_time', 'max_exec_time', 'blk_read_time', 'blk_write_time']
            save_to_csv(query_stats, f"query_stats_{timestamp}.csv", query_stats_header)

    if 'resource_usage' in metrics_to_collect:
        resource_usage = collect_resource_usage(conn)
        if resource_usage:
            # Flatten dict for CSV saving
            db_stats_data = [list(resource_usage["db_stats"])] if resource_usage["db_stats"] else []
            db_stats_header = ['datname', 'blks_read', 'blks_hit', 'hit_ratio']
            if db_stats_data: # Only save if data exists
                save_to_csv(db_stats_data, f"db_stats_{timestamp}.csv", db_stats_header)

            bgwriter_stats_data = [list(resource_usage["bgwriter_stats"])] if resource_usage["bgwriter_stats"] else []
            bgwriter_stats_header = ['checkpoints_timed', 'checkpoints_req', 'buffers_alloc', 'buffers_backend',
                                     'buffers_backend_fsync', 'buffers_checkpoint', 'buffers_clean',
                                     'maxwritten_clean', 'buff_alloc_rate', 'checkpoints_rate']
            if bgwriter_stats_data: # Only save if data exists
                save_to_csv(bgwriter_stats_data, f"bgwriter_stats_{timestamp}.csv", bgwriter_stats_header)

    if 'connection_info' in metrics_to_collect:
        connection_info = collect_connection_info(conn)
        if connection_info:
            connection_info_header = ['datname', 'usename', 'application_name', 'client_addr', 'state', 'query', 'backend_start', 'query_start', 'state_change']
            save_to_csv(connection_info, f"connection_info_{timestamp}.csv", connection_info_header)

    if 'lock_info' in metrics_to_collect:
        lock_info = collect_lock_info(conn)
        if lock_info:
            lock_info_header = ['pid', 'mode', 'granted', 'waitstart', 'relation', 'blocking_query']
            save_to_csv(lock_info, f"lock_info_{timestamp}.csv", lock_info_header)

    if 'table_sizes' in metrics_to_collect:
        table_sizes = collect_table_sizes(conn)
        if table_sizes:
            table_sizes_header = ['table_name', 'table_size', 'total_size', 'indexes_size']
            save_to_csv(table_sizes, f"table_sizes_{timestamp}.csv", table_sizes_header)

    conn.close()

if __name__ == "__main__":
    main()