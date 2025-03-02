# Phishing Data ETL Pipeline

This project implements an Apache Airflow DAG to collect, transform, and store phishing data from the PhishStats API. The pipeline runs every 90 minutes and handles data deduplication and geolocation enrichment.

## Features

- Fetches phishing data from PhishStats API
- Transforms and cleanses the data
- Generates geohash for geographic data
- Handles data deduplication using hash values
- Stores data in a normalized PostgreSQL database
- Runs automatically every 90 minutes

## Prerequisites

- Python 3.8+
- Apache Airflow 2.7+
- PostgreSQL database
- Required Python packages (see requirements.txt)

## Database Schema

The data is stored in three main tables:

1. `phishing_data`: Core phishing entry information
   - Includes URL, IP, location data, and basic metadata
   - Uses hash as a unique constraint to prevent duplicates
   - Includes geohash for efficient geographic queries

2. `phishing_metadata`: Additional metadata for each entry
   - Contains extended information like screenshots, SSL data
   - Linked to main table via foreign key

3. `phishing_tags`: Tags associated with phishing entries
   - Allows multiple tags per entry
   - Linked to main table via foreign key

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure Airflow connection for PostgreSQL:
   ```bash
   airflow connections add 'postgres_default' \
       --conn-type 'postgres' \
       --conn-host 'your_host' \
       --conn-login 'your_user' \
       --conn-password 'your_password' \
       --conn-port 5432 \
       --conn-schema 'your_database'
   ```

3. Place the DAG file in your Airflow dags folder:
   ```bash
   cp dags/phishing_data_etl.py $AIRFLOW_HOME/dags/
   ```

## DAG Structure

1. `create_tables`: Creates necessary database tables if they don't exist
2. `fetch_phishing_data`: Retrieves data from PhishStats API
3. `transform_phishing_data`: Cleanses and transforms the data
4. `load_to_database`: Loads data into PostgreSQL with deduplication

## Error Handling

- Each task includes comprehensive error handling
- Failed records are logged but don't stop the pipeline
- Retries are configured for temporary failures

## Monitoring

Monitor the pipeline through Airflow's web interface:
- Task success/failure status
- Execution times
- Log output for debugging

## License

MIT License 