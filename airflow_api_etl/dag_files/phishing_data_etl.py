from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import geohash
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from typing import Dict, Any, List
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_phishing_data(**context) -> None:
    """Fetch data from PhishStats API"""
    url = "https://phishstats.info:2096/api/phishing"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Store the raw data for the next task
        context['task_instance'].xcom_push(key='raw_phishing_data', value=data)
        logging.info(f"Successfully fetched {len(data)} records")
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        raise

def transform_phishing_data(**context) -> None:
    """Transform the raw phishing data"""
    raw_data = context['task_instance'].xcom_pull(key='raw_phishing_data')
    transformed_data = []
    
    for record in raw_data:
        try:
            # Generate geohash if coordinates are available
            lat = float(record.get('latitude', 0))
            lon = float(record.get('longitude', 0))
            gh = geohash.encode(lat, lon) if lat != 0 and lon != 0 else None
            
            transformed_record = {
                'external_id': record.get('id'),
                'url': record.get('url'),
                'ip': record.get('ip'),
                'country_code': record.get('countrycode'),
                'country_name': record.get('countryname'),
                'region_code': record.get('regioncode'),
                'region_name': record.get('regionname'),
                'city': record.get('city'),
                'zipcode': record.get('zipcode'),
                'latitude': lat,
                'longitude': lon,
                'geohash': gh,
                'asn': record.get('asn'),
                'bgp': record.get('bgp'),
                'isp': record.get('isp'),
                'title': record.get('title'),
                'detection_date': record.get('date'),
                'update_date': record.get('date_update'),
                'hash': record.get('hash'),
                'score': record.get('score'),
                'host': record.get('host'),
                'domain': record.get('domain'),
                'tld': record.get('tld'),
                'domain_age_days': record.get('domain_registered_n_days_ago')
            }
            
            # Metadata record
            metadata = {
                'hash': record.get('hash'),  # For joining
                'screenshot_url': record.get('screenshot'),
                'abuse_contact': record.get('abuse_contact'),
                'ssl_issuer': record.get('ssl_issuer'),
                'ssl_subject': record.get('ssl_subject'),
                'alexa_rank_host': record.get('alexa_rank_host'),
                'alexa_rank_domain': record.get('alexa_rank_domain'),
                'times_seen_ip': record.get('n_times_seen_ip'),
                'times_seen_host': record.get('n_times_seen_host'),
                'times_seen_domain': record.get('n_times_seen_domain'),
                'http_code': record.get('http_code'),
                'http_server': record.get('http_server'),
                'google_safebrowsing': record.get('google_safebrowsing'),
                'virus_total': record.get('virus_total'),
                'abuse_ch_malware': record.get('abuse_ch_malware')
            }
            
            # Tags
            tags = []
            if record.get('tags'):
                tags = [{'hash': record.get('hash'), 'tag': tag} for tag in record['tags']]
            
            transformed_data.append({
                'main': transformed_record,
                'metadata': metadata,
                'tags': tags
            })
            
        except Exception as e:
            logging.error(f"Error transforming record: {str(e)}")
            continue
    
    context['task_instance'].xcom_push(key='transformed_data', value=transformed_data)
    logging.info(f"Successfully transformed {len(transformed_data)} records")

def load_to_database(**context) -> None:
    """Load the transformed data into PostgreSQL"""
    transformed_data = context['task_instance'].xcom_pull(key='transformed_data')
    
    for record in transformed_data:
        try:
            # Insert main record
            main_data = record['main']
            main_sql = """
                INSERT INTO phishing_data (
                    external_id, url, ip, country_code, country_name, region_code,
                    region_name, city, zipcode, latitude, longitude, geohash,
                    asn, bgp, isp, title, detection_date, update_date, hash,
                    score, host, domain, tld, domain_age_days
                ) VALUES (
                    %(external_id)s, %(url)s, %(ip)s, %(country_code)s, %(country_name)s,
                    %(region_code)s, %(region_name)s, %(city)s, %(zipcode)s,
                    %(latitude)s, %(longitude)s, %(geohash)s, %(asn)s, %(bgp)s,
                    %(isp)s, %(title)s, %(detection_date)s, %(update_date)s,
                    %(hash)s, %(score)s, %(host)s, %(domain)s, %(tld)s,
                    %(domain_age_days)s
                )
                ON CONFLICT (hash) 
                DO UPDATE SET
                    update_date = EXCLUDED.update_date,
                    score = EXCLUDED.score
                RETURNING internal_id;
            """
            result = pg_hook.get_records(main_sql, parameters=main_data)
            phishing_id = result[0][0] if result else None
            
            if phishing_id:
                # Insert metadata
                metadata = record['metadata']
                metadata['phishing_id'] = phishing_id
                metadata_sql = """
                    INSERT INTO phishing_metadata (
                        phishing_id, screenshot_url, abuse_contact, ssl_issuer,
                        ssl_subject, alexa_rank_host, alexa_rank_domain,
                        times_seen_ip, times_seen_host, times_seen_domain,
                        http_code, http_server, google_safebrowsing,
                        virus_total, abuse_ch_malware
                    ) VALUES (
                        %(phishing_id)s, %(screenshot_url)s, %(abuse_contact)s,
                        %(ssl_issuer)s, %(ssl_subject)s, %(alexa_rank_host)s,
                        %(alexa_rank_domain)s, %(times_seen_ip)s, %(times_seen_host)s,
                        %(times_seen_domain)s, %(http_code)s, %(http_server)s,
                        %(google_safebrowsing)s, %(virus_total)s, %(abuse_ch_malware)s
                    );
                """
                pg_hook.run(metadata_sql, parameters=metadata)
                
                # Insert tags
                for tag in record['tags']:
                    tag['phishing_id'] = phishing_id
                    tag_sql = """
                        INSERT INTO phishing_tags (phishing_id, tag)
                        VALUES (%(phishing_id)s, %(tag)s);
                    """
                    pg_hook.run(tag_sql, parameters=tag)
        
        except Exception as e:
            logging.error(f"Error loading record: {str(e)}")
            continue

# Create the DAG
dag = DAG(
    'phishing_data_etl',
    default_args=default_args,
    description='ETL DAG for PhishStats data',
    schedule_interval=timedelta(minutes=90),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['phishing', 'security']
)

# Create tables
create_tables = SQLExecuteQueryOperator(
    task_id='create_tables',
    conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS phishing_data (
            internal_id BIGSERIAL PRIMARY KEY,
            external_id BIGINT NOT NULL,
            url TEXT NOT NULL,
            ip TEXT NOT NULL,
            country_code CHAR(2),
            country_name VARCHAR(100),
            region_code VARCHAR(50),
            region_name VARCHAR(100),
            city VARCHAR(100),
            zipcode VARCHAR(20),
            latitude DECIMAL(10,6),
            longitude DECIMAL(10,6),
            geohash VARCHAR(12),
            asn VARCHAR(50),
            bgp VARCHAR(100),
            isp TEXT,
            title TEXT,
            detection_date TIMESTAMP NOT NULL,
            update_date TIMESTAMP,
            hash VARCHAR(64) NOT NULL UNIQUE,
            score DECIMAL(5,2),
            host TEXT NOT NULL,
            domain TEXT,
            tld VARCHAR(50),
            domain_age_days INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT unique_phishing_entry UNIQUE (hash)
        );

        CREATE TABLE IF NOT EXISTS phishing_metadata (
            id BIGSERIAL PRIMARY KEY,
            phishing_id BIGINT REFERENCES phishing_data(internal_id),
            screenshot_url TEXT,
            abuse_contact TEXT,
            ssl_issuer TEXT,
            ssl_subject TEXT,
            alexa_rank_host INTEGER,
            alexa_rank_domain INTEGER,
            times_seen_ip INTEGER,
            times_seen_host INTEGER,
            times_seen_domain INTEGER,
            http_code INTEGER,
            http_server TEXT,
            google_safebrowsing TEXT,
            virus_total TEXT,
            abuse_ch_malware TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS phishing_tags (
            id BIGSERIAL PRIMARY KEY,
            phishing_id BIGINT REFERENCES phishing_data(internal_id),
            tag TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag
)

# Define tasks
fetch_data = PythonOperator(
    task_id='fetch_phishing_data',
    python_callable=fetch_phishing_data,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_phishing_data',
    python_callable=transform_phishing_data,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag
)

# Set task dependencies
create_tables >> fetch_data >> transform_data >> load_data 