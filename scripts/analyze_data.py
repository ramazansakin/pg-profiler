# scripts/analyze_data.py
import pandas as pd
import os
import glob
import configparser
from datetime import datetime

def analyze_query_patterns(df_queries):
    """Analyze query patterns and generate optimization suggestions."""
    suggestions = []
    
    for _, row in df_queries.iterrows():
        query = row['query']
        analysis = []
        
        # Check for common performance issues
        if 'SELECT *' in query.upper():
            analysis.append("⚠️ **Avoid SELECT ***: Specify columns explicitly to reduce network traffic and improve query plan caching.")
            
        if 'LIKE "%term%"' in query or "LIKE '%term%'" in query:
            analysis.append("🔍 **Leading Wildcard LIKE**: Using LIKE with a leading wildcard can't use indexes. Consider full-text search or alternative patterns.")
            
        if 'ORDER BY' in query.upper() and 'LIMIT' not in query.upper() and row['calls'] > 10:
            analysis.append("⏱️ **Missing LIMIT with ORDER BY**: Consider adding LIMIT to avoid unnecessary sorting of large result sets.")
            
        if 'JOIN' in query.upper() and 'ON' not in query.upper():
            analysis.append("🔗 **Implicit JOIN**: Use explicit JOIN syntax with ON clause for better readability and performance.")
            
        if 'OR' in query.upper() and 'INDEX' not in query.upper() and 'UNION' not in query.upper():
            analysis.append("🔎 **OR conditions**: OR conditions can prevent index usage. Consider rewriting with UNION ALL.")
            
        if row['avg_ms'] > 100 and 'WHERE' not in query.upper() and 'JOIN' not in query.upper() and 'calls' in row and row['calls'] > 10:
            analysis.append("🐌 **No WHERE clause**: Full table scans detected. Consider adding appropriate WHERE conditions.")
            
        if analysis:
            # Truncate long queries for better readability
            truncated_query = query[:200] + ('...' if len(query) > 200 else '')
            suggestions.append({
                'query': truncated_query,
                'calls': row.get('calls', 'N/A'),
                'avg_ms': f"{row.get('avg_ms', 'N/A'):.2f}",
                'suggestions': '\n'.join(f"  - {s}" for s in analysis)
            })
    
    return suggestions

def analyze_table_sizes(df_tables):
    """Analyze table sizes and provide optimization suggestions."""
    suggestions = []
    
    if df_tables.empty:
        return suggestions
        
    # Check for large tables without primary keys
    large_tables = df_tables[df_tables['table_size_mb'] > 100]  # Tables > 100MB
    for _, row in large_tables.iterrows():
        if 'pkey' not in row['indexes'].lower():
            suggestions.append(
                f"🔑 **Missing Primary Key**: Table `{row['schema_name']}.{row['table_name']}` is large ({row['table_size_mb']:.2f}MB) "
                f"but has no primary key. Consider adding one to improve query performance."
            )
    
    # Check for tables with many indexes
    if 'index_count' in df_tables.columns:
        high_index_tables = df_tables[df_tables['index_count'] > 5]  # More than 5 indexes
        for _, row in high_index_tables.iterrows():
            suggestions.append(
                f"📊 **Multiple Indexes**: Table `{row['schema_name']}.{row['table_name']}` has {row['index_count']} indexes. "
                f"Consider consolidating or removing unused indexes to reduce write overhead."
            )
    
    return suggestions

def generate_report(config):
    raw_data_dir = os.path.join(os.path.dirname(__file__), '..', config['collection']['output_dir_raw'])
    report_dir = os.path.join(os.path.dirname(__file__), '..', config['collection']['report_dir'])
    os.makedirs(report_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = os.path.join(report_dir, f"performance_report_{timestamp}.md")

    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(f"# PostgreSQL Performance Analysis Report - {timestamp}\n\n")
        f.write("This report provides performance metrics and optimization suggestions for your PostgreSQL database.\n\n")

        # --- Query Performance Analysis ---
        query_stats_files = sorted(glob.glob(os.path.join(raw_data_dir, "query_stats_*.csv")))
        if query_stats_files:
            latest_query_stats_file = query_stats_files[-1]
            f.write("## 1. Query Performance Analysis\n\n")
            try:
                df_queries = pd.read_csv(latest_query_stats_file)
                if not df_queries.empty:
                    # Basic query stats
                    top_n = int(config['analysis'].get('top_n_queries', 10))
                    f.write(f"### Top {top_n} Queries by Total Execution Time\n\n")
                    
                    # Format the queries for better readability
                    display_queries = df_queries.head(top_n).copy()
                    display_queries['query'] = display_queries['query'].apply(
                        lambda x: x[:100] + '...' if len(x) > 100 else x
                    )
                    
                    f.write(display_queries[['query', 'calls', 'total_ms', 'avg_ms', 'min_ms', 'max_ms']].to_markdown(index=False))
                    f.write("\n\n")
                    
                    # Add summary statistics
                    f.write("### Query Performance Summary\n\n")
                    f.write(f"- Total queries collected: {len(df_queries)}\n")
                    f.write(f"- Total execution time: {df_queries['total_ms'].sum()/1000:.2f} seconds\n")
                    f.write(f"- Average query time: {df_queries['avg_ms'].mean():.2f} ms\n")
                    f.write(f"- Slowest query: {df_queries['max_ms'].max():.2f} ms\n\n")
                    
                    # Generate optimization suggestions
                    f.write("## 2. Optimization Recommendations\n\n")
                    
                    # Query pattern analysis
                    query_suggestions = analyze_query_patterns(df_queries)
                    if query_suggestions:
                        f.write("### Query Optimization Opportunities\n\n")
                        f.write("The following queries might benefit from optimization:\n\n")
                        for i, suggestion in enumerate(query_suggestions[:10], 1):  # Limit to top 10
                            f.write(f"{i}. **Query**: `{suggestion['query']}`\n")
                            f.write(f"   - **Calls**: {suggestion['calls']}")
                            f.write(f" | **Avg Time**: {suggestion['avg_ms']} ms\n")
                            f.write(f"{suggestion['suggestions']}\n\n")
                    else:
                        f.write("No obvious query optimization opportunities detected.\n\n")
                    
                else:
                    f.write("No query statistics data found or pg_stat_statements is not enabled/populated.\n\n")
            except Exception as e:
                f.write(f"Error processing query stats: {e}\n\n")
        else:
            f.write("## 1. Query Performance Analysis\n\nNo query statistics data found.\n\n")

        # --- Table Analysis ---
        f.write("## 3. Table Analysis\n\n")
        table_size_files = sorted(glob.glob(os.path.join(raw_data_dir, "table_sizes_*.csv")))
        if table_size_files:
            latest_table_size_file = table_size_files[-1]
            try:
                df_tables = pd.read_csv(latest_table_size_file)
                if not df_tables.empty:
                    # Show largest tables
                    f.write("### Largest Tables\n\n")
                    df_tables['table_size_mb'] = df_tables['total_size_bytes'] / (1024 * 1024)
                    top_tables = df_tables.nlargest(10, 'table_size_mb')
                    f.write(top_tables[['schema_name', 'table_name', 'table_size_mb']]
                           .rename(columns={'schema_name': 'Schema', 'table_name': 'Table', 'table_size_mb': 'Size (MB)'})
                           .to_markdown(index=False, floatfmt=".2f"))
                    
                    # Table optimization suggestions
                    table_suggestions = analyze_table_sizes(df_tables)
                    if table_suggestions:
                        f.write("\n### Table Optimization Opportunities\n\n")
                        for suggestion in table_suggestions[:5]:  # Limit to top 5 table suggestions
                            f.write(f"- {suggestion}\n")
                    
                else:
                    f.write("No table size data available.\n")
            except Exception as e:
                f.write(f"Error processing table sizes: {e}\n")
        else:
            f.write("No table size data files found.\n")
            
        # --- Resource Usage Analysis ---
        f.write("\n## 4. Resource Usage Analysis\n\n")
        db_stats_files = sorted(glob.glob(os.path.join(raw_data_dir, "db_stats_*.csv")))
        if db_stats_files:
            latest_db_stats_file = db_stats_files[-1]
            try:
                df_db_stats = pd.read_csv(latest_db_stats_file)
                if not df_db_stats.empty:
                    f.write("### Database Statistics\n\n")
                    # Add cache hit ratio analysis
                    if 'cache_hit_ratio' in df_db_stats.columns:
                        cache_ratio = df_db_stats['cache_hit_ratio'].iloc[0]
                        f.write(f"- **Cache Hit Ratio**: {cache_ratio:.2f}%\n")
                        if cache_ratio < 90:
                            f.write("  - ⚠️ **Low cache hit ratio**. Consider increasing shared_buffers if you have available RAM.\n")
                        else:
                            f.write("  - ✅ Good cache hit ratio.\n")
                    
                    # Add more resource metrics if available
                    if 'database_size_mb' in df_db_stats.columns:
                        db_size = df_db_stats['database_size_mb'].iloc[0]
                        f.write(f"- **Database Size**: {db_size:,.2f} MB\n")
                    
                    f.write("\n")
                    f.write("\n\n")
                else:
                    f.write("No database statistics data found.\n\n")
            except Exception as e:
                f.write(f"Error processing DB stats: {e}\n\n")
        
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