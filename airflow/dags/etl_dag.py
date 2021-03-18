import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (PostgresOperator, FlatCategoryJSONOperator, StageJSONToRedshiftOperator,
                               StageCSVToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)


default_args = {
    'owner': 'uda_onur',
    'start_date': datetime(2021, 2, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

template_search_path = '/home/workspace/airflow/'

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          template_searchpath=[template_search_path],
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

flat_category_tasks = []
f = open("./category_source_meta.json", "r")
category_json_meta = json.load(f)
for continental in category_json_meta.keys():
    for country_meta in category_json_meta[continental]:
        flat_category_tasks.append(FlatCategoryJSONOperator(task_id="flat_" + continental + "_" + country_meta["country"] + "_json",
                                                            dag=dag,
                                                            source_s3_bucket=country_meta["bucket"],
                                                            source_s3_key=country_meta["key"],
                                                            aws_credential_conn_id="aws_credential"))

collect_operator = DummyOperator(task_id='collect_list_tasks',  dag=dag)

stage_category_tasks = []
continental_category_manfiest = [("s3://onur-uda-america/america_category.manifest", "us-east-1"),
                                 ("s3://onur-uda-asia/asia_category.manifest", "ap-southeast-1"),
                                 ("s3://onur-uda-europe/europe_category.manifest", "eu-central-1")]
for manifest in continental_category_manfiest:
    stage_category_tasks.append(StageJSONToRedshiftOperator(task_id='stage_' + manifest[1] + '_video_category',
                                                            dag=dag,
                                                            target_table="staging_category",
                                                            source_s3_path=manifest[0],
                                                            aws_credential_conn_id="aws_credential",
                                                            redshift_conn_id="redshift",
                                                            aws_region=manifest[1],
                                                            json_option="auto ignorecase",
                                                            manifest="manifest"
                                                            ))


stage_trend_tasks = []
continental_trend_manfiest = [("s3://onur-uda-america/america_trend.manifest", "us-east-1", "video_id,title,publishedat,channelid,channeltitle,categoryid,trending_date,tags,view_count,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,description"),
                              ("s3://onur-uda-asia/asia_trend.manifest", "ap-southeast-1", "video_id,title,publishedat,channelid,channeltitle,categoryid,trending_date,tags,view_count,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,description"),
                              ("s3://onur-uda-europe/europe_trend.manifest", "eu-central-1", "video_id,title,publishedat,channelid,channeltitle,categoryid,trending_date,tags,view_count,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,description")]
for manifest in continental_trend_manfiest:
    stage_trend_tasks.append(StageCSVToRedshiftOperator(task_id='stage_' + manifest[1] + '_video_trends',
                                                        dag=dag,
                                                        target_table="staging_video_trend_log",
                                                        columns=manifest[2],
                                                        source_s3_path=manifest[0],
                                                        aws_credential_conn_id="aws_credential",
                                                        redshift_conn_id="redshift",
                                                        aws_region=manifest[1],
                                                        manifest="manifest",
                                                        ignore_header="IGNOREHEADER 1"
                                                        ))

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_table >> flat_category_tasks >> collect_operator
collect_operator >> stage_category_tasks >> end_operator
collect_operator >> stage_trend_tasks >> end_operator


