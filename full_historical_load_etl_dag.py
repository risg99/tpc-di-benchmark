import os
import airflow
from datetime import timedelta, datetime
from airflow import DAG
# from airflow.operators.postgres_operator import PostgresOperator #deprecated
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from customermgmt_conversion import customermgmt_convert


# Default arguments for dag
default_args = {
    'owner': 'airflow',
    # 'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 30)
}

# Create dag
dag_psql = DAG(
    dag_id = "dw_sf_3",
    default_args = default_args,	
    #dagrun_timeout = timedelta(minutes=60),
    #description = 'TPC-DI project',
    #schedule = None,
    schedule_interval = None,
    # catchup = False
)

# Task1 - Create staging schema
create_staging_schema = PostgresOperator(
    task_id = "create_staging_schema",
    postgres_conn_id = "pg_sf_3",
    sql = "create_staging_schema.sql",
	dag = dag_psql
)

# Task2 - Load txt and csv sources to staging
load_txt_csv_sources_to_staging = PostgresOperator(
    task_id = "load_txt_csv_sources_to_staging",
    postgres_conn_id = "pg_sf_3",
    sql = "staging_data_commands.sql",
	dag = dag_psql
)

# Task3 - Load finwire source to staging
load_finwire_to_staging = PostgresOperator(
    task_id = "load_finwire_to_staging",
    postgres_conn_id = "pg_sf_3",
    sql = "staging_finwire_load1.sql",
	dag = dag_psql
)

# Task4 - Parse finwire and load to seperate tables
parse_finwire = PostgresOperator(
    task_id = "parse_finwire",
    postgres_conn_id = "pg_sf_3",
    sql = "load_staging_finwire_db.sql",
	dag = dag_psql
)

# Task5 - Convert customer management source from xml to csv
convert_customermgmt_xml_to_csv = PythonOperator(
    task_id = "convert_customermgmt_xml_to_csv",
    python_callable = customermgmt_convert,
    dag = dag_psql,
)

# Task6 - Load customer management source to staging
load_customer_mgmt_to_staging = PostgresOperator(
    task_id = "load_customer_mgmt_to_staging",
    postgres_conn_id = "pg_sf_3",
    sql = "load_staging_customermgmt_db.sql",
	dag = dag_psql
)

# Task7 - Create master schema
create_master_schema = PostgresOperator(
    task_id = "create_master_schema",
    postgres_conn_id = "pg_sf_3",
    sql = "create_master_schema.sql",
	dag = dag_psql
)

# Task8 - Direct load master.tradetype
load_master_tradetype = PostgresOperator(
    task_id = "load_master_tradetype",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/1_load_master_tradetype.sql",
	dag = dag_psql
)

# Task9 - Direct load master.statustype
load_master_statustype = PostgresOperator(
    task_id = "load_master_statustype",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/2_load_master_statustype.sql",
	dag = dag_psql
)

# Task10 - Direct load master.taxrate
load_master_taxrate = PostgresOperator(
    task_id = "load_master_taxrate",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/3_load_master_taxrate.sql",
	dag = dag_psql
)

# Task11 - Direct load master.industry
load_master_industry = PostgresOperator(
    task_id = "load_master_industry",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/4_load_master_industry.sql",
	dag = dag_psql
)

# Task12 - Transform & load master.dimdate
transform_load_master_dimdate = PostgresOperator(
    task_id = "transform_load_master_dimdate",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/5_transform_load_master_dimdate.sql",
	dag = dag_psql
)

# Task13 - Transform & load master.dimtime
transform_load_master_dimtime = PostgresOperator(
    task_id = "transform_load_master_dimtime",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/6_transform_load_master_dimtime.sql",
	dag = dag_psql
)

# Task14 - Transform & load master.dimcompany
transform_load_master_dimcompany = PostgresOperator(
    task_id = "transform_load_master_dimcompany",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/7_transform_load_master_dimcompany.sql",
	dag = dag_psql
)

# Task15 - Load master.dimessages with alert from master.dimcompany
load_master_dimessages_dimcompany = PostgresOperator(
    task_id = "load_master_dimessages_dimcompany",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/8_load_master_dimessages_dimcompany.sql",
	dag = dag_psql
)

# Task16 - Transform & load master.dimbroker
transform_load_master_dimbroker = PostgresOperator(
    task_id = "transform_load_master_dimbroker",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/9_transform_load_master_dimbroker.sql",
	dag = dag_psql
)

# Task17 - Transform & load master.prospect
transform_load_master_prospect = PostgresOperator(
    task_id = "transform_load_master_prospect",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/10_transform_load_master_prospect.sql",
	dag = dag_psql
)

# Task18 - Transform & load master.dimcustomer
transform_load_master_dimcustomer = PostgresOperator(
    task_id = "transform_load_master_dimcustomer",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/11_transform_load_master_dimcustomer.sql",
	dag = dag_psql
)

# Task19 - Load master.dimessages with alert from master.dimcustomer
load_master_dimessages_dimcustomer = PostgresOperator(
    task_id = "load_master_dimessages_dimcustomer",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/12_load_master_dimessages_dimcustomer.sql",
	dag = dag_psql
)

# Task20 - Update master.prospect
update_master_prospect = PostgresOperator(
    task_id = "update_master_prospect",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/13_update_master_prospect.sql",
	dag = dag_psql
)

# Task21 - Transform & load master.dimaccount
transform_load_master_dimaccount = PostgresOperator(
    task_id = "transform_load_master_dimaccount",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/14_transform_load_master_dimaccount.sql",
	dag = dag_psql
)

# Task22 - Transform & load master.dimsecurity
transform_load_master_dimsecurity = PostgresOperator(
    task_id = "transform_load_master_dimsecurity",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/15_transform_load_master_dimsecurity.sql",
	dag = dag_psql
)

# Task23 - Transform & load master.dimtrade
transform_load_master_dimtrade = PostgresOperator(
    task_id = "transform_load_master_dimtrade",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/16_transform_load_master_dimtrade.sql",
	dag = dag_psql
)

# Task24 - Load master.dimessages with alert from master.dimtrade
load_master_dimessages_dimtrade = PostgresOperator(
    task_id = "load_master_dimessages_dimtrade",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/17_load_master_dimessages_dimtrade.sql",
	dag = dag_psql
)

# Task25 - Transform & load master.financial
transform_load_master_financial = PostgresOperator(
    task_id = "transform_load_master_financial",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/18_transform_load_master_financial.sql",
	dag = dag_psql
)

# Task26 - Transform & load master.factcashbalances
transform_load_master_factcashbalances = PostgresOperator(
    task_id = "transform_load_master_factcashbalances",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/19_transform_load_master_factcashbalances.sql",
	dag = dag_psql
)

# Task27 - Transform & load master.factholdings
transform_load_master_factholdings = PostgresOperator(
    task_id = "transform_load_master_factholdings",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/20_transform_load_master_factholdings.sql",
	dag = dag_psql
)

# Task28 - Transform & load master.factwatches
transform_load_master_factwatches = PostgresOperator(
    task_id = "transform_load_master_factwatches",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/21_transform_load_master_factwatches.sql",
	dag = dag_psql
)

# Task29 - Transform & load master.factmarkethistory
transform_load_master_factmarkethistory = PostgresOperator(
    task_id = "transform_load_master_factmarkethistory",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/22_transform_load_master_factmarkethistory.sql",
	dag = dag_psql
)

# Task30 - Load master.dimessages with alert from master.factmarkethistory
load_master_dimessages_factmarkethistory = PostgresOperator(
    task_id = "load_master_dimessages_factmarkethistory",
    postgres_conn_id = "pg_sf_3",
    sql = "/transformations/23_load_master_dimessages_factmarkethistory.sql",
	dag = dag_psql
)


# Task Dependencies

# Staging schema dependency
create_staging_schema >> load_txt_csv_sources_to_staging
create_staging_schema >> load_finwire_to_staging >> parse_finwire
create_staging_schema >> convert_customermgmt_xml_to_csv >> load_customer_mgmt_to_staging

# Master schema dependency
load_txt_csv_sources_to_staging >> create_master_schema
parse_finwire >> create_master_schema
load_customer_mgmt_to_staging >> create_master_schema

# Transformation/Loading to master dependency
create_master_schema >> load_master_tradetype
create_master_schema >> load_master_statustype
create_master_schema >> load_master_taxrate
create_master_schema >> load_master_industry
create_master_schema >> transform_load_master_dimdate
create_master_schema >> transform_load_master_dimtime
create_master_schema >> transform_load_master_dimcompany
transform_load_master_dimcompany >> load_master_dimessages_dimcompany
transform_load_master_dimdate >> transform_load_master_dimbroker
transform_load_master_dimdate >> transform_load_master_prospect
load_master_taxrate >> transform_load_master_dimcustomer
transform_load_master_prospect >> transform_load_master_dimcustomer
transform_load_master_dimcustomer >> load_master_dimessages_dimcustomer
transform_load_master_dimcustomer >> update_master_prospect
transform_load_master_dimbroker >> transform_load_master_dimaccount
transform_load_master_dimcustomer >> transform_load_master_dimaccount
transform_load_master_dimcompany >> transform_load_master_dimsecurity
transform_load_master_dimaccount >> transform_load_master_dimtrade
load_master_statustype >> transform_load_master_dimtrade
load_master_tradetype >> transform_load_master_dimtrade
transform_load_master_dimsecurity >> transform_load_master_dimtrade
transform_load_master_dimtrade >> load_master_dimessages_dimtrade
transform_load_master_dimcompany >> transform_load_master_financial
transform_load_master_dimaccount >> transform_load_master_factcashbalances
transform_load_master_dimdate >> transform_load_master_factcashbalances
transform_load_master_dimtrade >> transform_load_master_factholdings
transform_load_master_dimcustomer >> transform_load_master_factwatches
transform_load_master_dimsecurity >> transform_load_master_factwatches
transform_load_master_dimdate >> transform_load_master_factwatches
transform_load_master_dimdate >> transform_load_master_factmarkethistory
transform_load_master_financial >> transform_load_master_factmarkethistory
transform_load_master_dimcompany >> transform_load_master_factmarkethistory
transform_load_master_dimsecurity >> transform_load_master_factmarkethistory
transform_load_master_factmarkethistory >> load_master_dimessages_factmarkethistory
