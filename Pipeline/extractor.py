import os
import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': '',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('extract_table_from_cloudsq_bucket', default_args=default_args)
dag.doc_md = __doc__

begin = DummyOperator(task_id="begin", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

class TableConfig:
    """
    Holds config for the export/import job we are creating.
    """
    STANDARD_EXPORT_QUERY = None
    _STANDARD_EXPORT_QUERY = "SELECT * from {}"

    def __init__(self,
                 cloud_sql_instance,
                 export_bucket,
                 export_database,
                 export_table,
                 export_query,
                 gcp_project,
                 #stage_dataset,
                 #stage_table,
                 #stage_final_query,
                 #bq_location
                 ):

        self.params = {
            'export_table': export_table,
            'export_bucket': export_bucket,
            'export_database': export_database,
            'export_query': export_query or self._STANDARD_EXPORT_QUERY.format(export_table),
            'gcp_project': gcp_project,
            #'stage_dataset': stage_dataset,
            #'stage_table': stage_table or export_table,
            #'stage_final_query': stage_final_query,
            'cloud_sql_instance': cloud_sql_instance,
            #'bq_location': bq_location or "us-central1",
        }


def get_tables():
    """
    return a list of tables that should go from cloud sql to bigquery
    In this example all 3 tables reside in the same cloud sql instance.
    :return:
    """
    #dim_tables = ["DimAge", "DimPerson"]
    #fact_tables = ["FactPerson"]

    export_tables = ["US_Accidents_Dec21_updated"]
    tables = []
    for dim in export_tables:
        tables.append(TableConfig(cloud_sql_instance='oltpdb',
                                  export_table=dim,
                                  export_bucket='',
                                  export_database='',
                                  export_query=TableConfig.STANDARD_EXPORT_QUERY,
                                  gcp_project="",
                                  #stage_dataset="YOUR_STAGING_DATASET",
                                  #stage_table=None,
                                  #stage_final_query=None,
                                  #bq_location="us-central1"
                                ))
    return tables


def gen_export_table_task(table_config):
    """
    Create a export table task for the current table.  preserves table_config parameters for later use.
    :return: a task to be used.
    """
    export_script = BashOperator(
                                task_id='export_{}_with_gcloud'.format(table_config.params['export_table']),
                                params=table_config.params,
                                bash_command="""
gcloud --project {{ params.gcp_project }} sql export csv {{ params.cloud_sql_instance }} \
                            gs://{{ params.export_bucket }}/{{ params.export_table }}_{{ ds_nodash }} \
                            --database={{ params.export_database }} --query="{{ params.export_query }}"
""",
                                dag=dag)
    export_script.doc_md = """\
    #### Export table from cloudsql to cloud storage
    task documentation
    """

    return export_script


for table_config in get_tables():
    export_script = gen_export_table_task(table_config)

    begin >> export_script >> end