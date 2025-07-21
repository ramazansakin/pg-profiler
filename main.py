# main.py
import subprocess
import os
from datetime import datetime

def run_script(script_path, script_name):
    """Helper function to run a Python script and log its output."""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{script_name}_{timestamp}.log")

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running {script_name}...")
    with open(log_file, 'w') as f:
        process = subprocess.Popen(['python', script_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in iter(process.stdout.readline, ''):
            print(line, end='') # Print to console
            f.write(line)       # Write to log file
        process.wait()
        if process.returncode != 0:
            print(f"Error: {script_name} exited with code {process.returncode}. Check {log_file} for details.")
        else:
            print(f"{script_name} completed successfully. Logs in {log_file}")

def main():
    print("Starting PostgreSQL Performance Profiling and Baseline Analysis PoC...")

    # Define script paths
    collect_script = os.path.join(os.path.dirname(__file__), 'scripts', 'collect_metrics.py')
    analyze_script = os.path.join(os.path.dirname(__file__), 'scripts', 'analyze_data.py')

    # Step 1: Collect Metrics
    run_script(collect_script, "collect_metrics")

    # Step 2: Analyze Data and Generate Report
    run_script(analyze_script, "analyze_data")

    print("\nPoC execution completed.")

if __name__ == "__main__":
    main()
