import snowflake.connector
import time
import csv
from datetime import datetime, timedelta
import json

# Load configuration
try:
    with open('app_config.json', 'r') as file:
        config = json.load(file)
except FileNotFoundError:
    raise Exception("Configuration file 'app_config.json' not found. Please ensure it exists in the correct path.")

# Snowflake connection parameters
user = config['snowflake']['user']
password = config['snowflake']['password']
account = config['snowflake']['account']
warehouse = config['snowflake']['warehouse']
database = config['snowflake']['database']
schema = config['snowflake']['schema']

# Application settings
metadata_table_name = config['app_settings']['metadata_table_name']
refresh_interval_hrs = config['app_settings']['refresh_interval_hrs']

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)

def fetch_jobs():
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, proc_to_call, job_frequency, day_of_week, day_of_month, daily_time FROM {metadata_table_name} WHERE is_active = TRUE")
        rows = cur.fetchall()

    # Write to CSV
    with open('jobs_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['id', 'proc_to_call', 'job_frequency', 'day_of_week', 'day_of_month', 'daily_time'])
        writer.writerows(rows)

def read_jobs_from_csv():
    with open('jobs_data.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        return list(reader)

def parse_interval(time_str):
    # Parses a time interval in format "HH:MM" and returns total minutes
    hours, minutes = map(int, time_str.split(':'))
    return hours * 60 + minutes

def check_and_run_jobs(jobs):
    current_time = datetime.now()
    day_of_week = current_time.weekday()
    day_of_month = current_time.day
    current_minutes = current_time.hour * 60 + current_time.minute

    for job in jobs:
        job_frequency, job_day_of_week, job_day_of_month, job_time = job[2], int(job[3]), int(job[4]), job[5]

        if job_frequency == 'every':
            interval = parse_interval(job_time)
            if current_minutes % interval == 0:
                run_job(job[1])

        elif (job_frequency == 'daily' and job_time == current_time.strftime('%H:%M')) or \
             (job_frequency == 'weekly' and job_day_of_week == day_of_week and job_time == current_time.strftime('%H:%M')) or \
             (job_frequency == 'monthly' and job_day_of_month == day_of_month and job_time == current_time.strftime('%H:%M')):
            run_job(job[1])

def run_job(proc_name):
    # Run the stored procedure in Snowflake
    try:
        with conn.cursor() as cur:
            call_statement = f"CALL {proc_name}()"  # Assuming the procedure takes no arguments
            cur.execute(call_statement)
            print(f"Successfully ran job: {proc_name}")
    except Exception as e:
        print(f"Error running job {proc_name}: {e}")

# Fetch jobs data and write to CSV
fetch_jobs()

try:
    while True:
        current_time = datetime.now()
        if current_time.hour % refresh_interval_hrs == 0 and current_time.minute == 0:
            fetch_jobs()  # Refresh the job data at the specified interval

        jobs = read_jobs_from_csv()
        check_and_run_jobs(jobs)

        # Sleep for a shorter time interval, like 1 minute, to ensure timely execution of jobs
        time.sleep(60)

except KeyboardInterrupt:
    print("Script stopped by user")

finally:
    conn.close()
