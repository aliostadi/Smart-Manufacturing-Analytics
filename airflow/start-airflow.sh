#!/bin/bash
set -e  # stop on error

echo "Initializing Airflow DB..."
airflow db init

# Check if admin user already exists
if airflow users list | grep -q admin; then
  echo "Admin user already exists, skipping creation."
else
  echo "Creating admin user..."
  airflow users create \
    --username admin \
    --password admin \
    --firstname Yasaman \
    --lastname Admin \
    --role Admin \
    --email yasaman@example.com
fi

echo "Starting Airflow scheduler and webserver..."
airflow scheduler &
exec airflow webserver
