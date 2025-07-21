# scripts/analyze_data.py
import pandas as pd
import os
import glob
import configparser
from datetime import datetime

def generate_report(config):
    raw_data_dir = os.path.join(os.path.dirname(__file__), '..', config['collection']['output_dir_raw'])
    report_dir = os.path.join(os.path.dirname(__file__), '..', config['collection']['report_dir'])
    os.makedirs(report_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = os.path.join(report_dir, f"baseline_report_{timestamp}.md")

    with open(report_filename, 'w') as f:
        f.write(f"# PostgreSQL Performance Baseline Report - {timestamp}\n\n")
        f.write("This report provides a snapshot of key PostgreSQL performance metrics.\n\n")

        # --- Query Performance Analysis ---
        query_stats_files = sorted(glob.glob(os.path.join(raw_data_dir, "query_stats_*.csv")))
        if query_stats_files:
            latest_query_stats_file = query_stats_files[-1]
            f.write("## 1. Query Performance Analysis (Top Queries by Total Execution Time)\n\n")
            try:
                df_queries = pd.read_csv(latest_query_stats_file)
                if not df_queries.empty:
                    top_n = int(config['analysis'].get('top_n_queries', 10))
                    f.write(f"Showing top {top_n} queries:\n\n")
                    f.write(df_queries.head(top_n).to_markdown(index=False))
                    f.write("\n\n")
                    f.write(f"Total queries collected: {len(df_queries)}\n")
                    f.write(f"Total execution time for collected queries: {df_queries['total_exec_time'].sum():.2f} ms\n\n")
                else:
                    f.write("No query statistics data found or pg_stat_statements is not enabled/populated.\n\n")
            except Exception as e:
                f.write(f"Error processing query stats: {e}\n\n")
        else:
            f.write("## 1. Query Performance Analysis\n\nNo query statistics data found.\n\n")

        # --- Resource Usage Analysis ---
        f.write("## 2. Resource Usage Analysis\n\n")
        db_stats_files = sorted(glob.glob(os.path.join(raw_data_dir, "db_stats_*.csv")))
        if db_stats_files:
            latest_db_stats_file = db_stats_files[-1]
            try:
                df_db_stats = pd.read_csv(latest_db_stats_file)
                if not df_db_stats.empty:
                    f.write("### Database Statistics (Cache Hit Ratio)\n\n")
                    f.write(df_db_stats.to_markdown(index=False))
                    f.write("\n\n")
                else:
                    f.write("No database statistics data found.\n\n")
            except Exception as e:
                f.write(f"Error processing DB stats: {e}\n\n")
        else:
            f.write("No database statistics data found.\n\n")

        bgwriter_stats_files = sorted(glob.glob(os.path.join(raw_data_dir, "bgwriter_stats_*.csv")))
        if bgwriter_stats_files:
            latest_bgwriter_stats_file = bgwriter_stats_files[-1]
            try:
                df_bgwriter_stats = pd.read_csv(latest_bgwriter_stats_file)
                if not df_bgwriter_stats.empty:
                    f.write("### Background Writer Statistics\n\n")
                    f.write(df_bgwriter_stats.to_markdown(index=False))
                    f.write("\n\n")
                else:
                    f.write("No background writer statistics data found.\n\n")
            except Exception as e:
                f.write(f"Error processing BGWriter stats: {e}\n\n")
        else:
            f.write("No background writer statistics data found.\n\n")


        # --- Connection Information ---
        f.write("## 3. Connection Information\n\n")
        conn_info_files = sorted(glob.glob(os.path.join(raw_data_dir, "connection_info_*.csv")))
        if conn_info_files:
            latest_conn_info_file = conn_info_files[-1]
            try:
                df_conn = pd.read_csv(latest_conn_info_file)
                if not df_conn.empty:
                    f.write(f"Total active connections: {len(df_conn[df_conn['state'] == 'active'])}\n")
                    f.write(f"Total idle connections: {len(df_conn[df_conn['state'] == 'idle'])}\n")
                    f.write(f"Total idle in transaction connections: {len(df_conn[df_conn['state'] == 'idle in transaction'])}\n\n")
                    f.write("Top 10 Active Connections (by query start time):\n\n")
                    # Convert to datetime objects for sorting
                    df_conn['query_start'] = pd.to_datetime(df_conn['query_start'])
                    f.write(df_conn[df_conn['state'] == 'active'].sort_values(by='query_start', ascending=True).head(10).to_markdown(index=False))
                    f.write("\n\n")
                else:
                    f.write("No connection information found.\n\n")
            except Exception as e:
                f.write(f"Error processing connection info: {e}\n\n")
        else:
            f.write("No connection information found.\n\n")

        # --- Lock Information ---
        f.write("## 4. Lock Information\n\n")
        lock_info_files = sorted(glob.glob(os.path.join(raw_data_dir, "lock_info_*.csv")))
        if lock_info_files:
            latest_lock_info_file = lock_info_files[-1]
            try:
                df_locks = pd.read_csv(latest_lock_info_file)
                if not df_locks.empty:
                    f.write(f"Total active locks: {len(df_locks)}\n")
                    f.write(f"Total granted locks: {len(df_locks[df_locks['granted'] == True])}\n")
                    f.write(f"Total waiting locks: {len(df_locks[df_locks['granted'] == False])}\n\n")
                    if not df_locks[df_locks['granted'] == False].empty:
                        f.write("Waiting Locks:\n\n")
                        f.write(df_locks[df_locks['granted'] == False].to_markdown(index=False))
                        f.write("\n\n")
                    else:
                        f.write("No waiting locks identified.\n\n")
                else:
                    f.write("No lock information found.\n\n")
            except Exception as e:
                f.write(f"Error processing lock info: {e}\n\n")
        else:
            f.write("No lock information found.\n\n")

        # --- Table and Index Sizes ---
        f.write("## 5. Table and Index Sizes\n\n")
        table_sizes_files = sorted(glob.glob(os.path.join(raw_data_dir, "table_sizes_*.csv")))
        if table_sizes_files:
            latest_table_sizes_file = table_sizes_files[-1]
            try:
                df_table_sizes = pd.read_csv(latest_table_sizes_file)
                if not df_table_sizes.empty:
                    f.write("Largest Tables by Total Size:\n\n")
                    # For sorting, convert size strings to bytes if needed. For simplicity here, assume pretty print is just for display.
                    # Or just sort by table_size string directly which might not be numerically correct for `pg_size_pretty`.
                    # For a real solution, you'd store raw bytes and convert for display.
                    # As a workaround, if `pg_size_pretty` uses K, M, G, you could define a custom sort key.
                    # For this PoC, we'll just display.
                    f.write(df_table_sizes.head(10).to_markdown(index=False))
                    f.write("\n\n")
                else:
                    f.write("No table size information found.\n\n")
            except Exception as e:
                f.write(f"Error processing table sizes: {e}\n\n")
        else:
            f.write("No table size information found.\n\n")

        f.write("\n---\n*End of Report*")
    print(f"Report generated: {report_filename}")


def main():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_path)
    generate_report(config)

if __name__ == "__main__":
    main()