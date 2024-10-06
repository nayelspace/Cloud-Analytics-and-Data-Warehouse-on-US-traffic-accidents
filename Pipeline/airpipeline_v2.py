from datetime import timedelta, datetime


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator



GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID=""
GS_PATH = ""
BUCKET_NAME = ''
STAGING_DATASET = "staging_dataset"
DATASET = "bidataset"
LOCATION = "us-central1"

default_args = {
    'owner': '',
    'depends_on_past': False,
    'email_on_failure': [],
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(1),
    'retry_delay': timedelta(minutes=1),
}

with DAG('pipeline-v2', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    begin = DummyOperator(
        task_id = 'begin',
        dag = dag
        )

    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
        )

    load_dataset_fact = GCSToBigQueryOperator(
        task_id = 'load_dataset_fact',
        bucket = BUCKET_NAME,
        source_objects = ['starschematables/Accidents_fact_table.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_fact',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'ID', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Severity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Start_Time', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'End_Time', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'Start_Lat', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Start_Lng', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'End_Lat', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'End_Lng', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Distance_mi', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Number', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Street', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Side', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Location_ID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Airport_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Weather_Timestamp', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'Temperature_F', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Wind_Chill_F', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Humidity', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Pressure_in', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Visibility_mi', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Wind_Direction', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Wind_Speed_mph', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Precipitation_in', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'Weather_Condition', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'POI_ID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Sunrise_Sunset', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Civil_Twilight', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Nautical_Twilight', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Astronomical_Twilight', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )

    load_dataset_dim_location = GCSToBigQueryOperator(
        task_id = 'load_dataset_dim_location',
        bucket = BUCKET_NAME,
        source_objects = ['starschematables/Location_dimension_table.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_dim_location',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'Location_ID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'County', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Zipcode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Timezone', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )

    load_dataset_dim_poi = GCSToBigQueryOperator(
        task_id = 'load_dataset_dim_poi',
        bucket = BUCKET_NAME,
        source_objects = ['starschematables/POI_dimension_table.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_dim_poi',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'POI_ID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Amenity', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Bump', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Crossing', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Give_Way', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Junction', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'No_Exit', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Railway', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Roundabout', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Station', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Stop', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Traffic_Calming', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Traffic_Signal', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Turning_Loop', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )

    check_dataset_fact = BigQueryCheckOperator(
        task_id = 'check_dataset_fact',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_fact`'
        )

    check_dataset_dim_location = BigQueryCheckOperator(
        task_id = 'check_dataset_dim_location',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_dim_location`'
        )

    check_dataset_dim_poi = BigQueryCheckOperator(
        task_id = 'check_dataset_dim_poi',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_dim_poi`'
        )

    create_bi_tables = DummyOperator(
        task_id = 'Create_bi_tables',
        dag = dag
        )

    create_bi_dataset_fact = BigQueryOperator(
        task_id = 'create_bi_dataset_fact',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql2/bi_dataset_fact.sql'
        )

    create_bi_dataset_dim_location = BigQueryOperator(
        task_id = 'create_bi_dataset_dim_location',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql2/bi_dataset_dim_location.sql'
        )

    create_bi_dataset_dim_poi = BigQueryOperator(
        task_id = 'create_bi_dataset_dim_poi',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql2/bi_dataset_dim_poi.sql'
        )

    check_bi_dataset_fact = BigQueryCheckOperator(
        task_id = 'check_bi_dataset_fact',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.bi_dataset_fact`'
        )

    check_bi_dataset_dim_location = BigQueryCheckOperator(
        task_id = 'check_bi_dataset_dim_location',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.bi_dataset_dim_location`'
        )

    check_bi_dataset_dim_poi = BigQueryCheckOperator(
        task_id = 'check_bi_dataset_dim_poi',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.bi_dataset_dim_poi`'
        )

    end = DummyOperator(
        task_id = 'end',
        dag = dag
        )

begin >> load_staging_dataset

load_staging_dataset >> [load_dataset_fact, load_dataset_dim_location, load_dataset_dim_poi]

load_dataset_fact >> check_dataset_fact
load_dataset_dim_location >> check_dataset_dim_location
load_dataset_dim_poi >> check_dataset_dim_poi

[check_dataset_fact, check_dataset_dim_location, check_dataset_dim_poi] >> create_bi_tables

create_bi_tables >> [create_bi_dataset_fact, create_bi_dataset_dim_location, create_bi_dataset_dim_poi]

create_bi_dataset_fact >> check_bi_dataset_fact
create_bi_dataset_dim_location >> check_bi_dataset_dim_location
create_bi_dataset_dim_poi >> check_bi_dataset_dim_poi

[check_bi_dataset_fact, check_bi_dataset_dim_location, check_bi_dataset_dim_poi] >> end
