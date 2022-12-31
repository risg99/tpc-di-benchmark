## **TPC-DI-Benchmark:** Data Integration with PostgreSQL & Airflow
#### Data Warehouse Benchmark Project 2

---
## Code Reproducibility
---
### Setting up Airflow with Docker
1. Create a local folder:

>`$ mkdir airflow-local
 $ cd airflow-local`

2. Fetch the standard ‘docker-compose.yaml’ file to setup airflow image within docker:

>`$ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.0/docker-compose.yaml'`

This includes services such as airflow-scheduler, airflow-webserver, airflow-worker, airflow-cli, airflow-init, flower, postgres and redis.

3. Create directories (dags, logs, plugins) that will be mounted with docker airflow:

>`$ mkdir ./dags ./logs ./plugins`

4. Setup the airflow user and ensure permissions within container is same as local machine, by setting up .env file:

>`$ echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env`

>`$ cat .env
 AIRFLOW_UID=501
 AIRFLOW_GID=0`

5. Initialize the Airflow DB:

>`$ docker-compose up airflow-init`

6. Begin airflow services:

>`$ docker-compose up`

7. Start airflow-service manually (only once), given that it’s default setting is set to not form a container when image is setup:

>`docker-compose run airflow-cli webserver`

8. Ensure all necessary containers are running:

>`docker ps`

9. Access and login to the Airflow webserver at `localhost:8080`


### Configuration for data integration pipeline

1. Setup connection from airflow within docker, to the local postgres server database using the airflow webserver. This can be done from Admin > Connections. Ensure to set host to ‘host.docker.internal’

2. Add scripts into respective mounted folders to be detected by airflow
    - Within the ‘dags’ folder that is in the main local airflow directory, create an additional folder called ‘sf_3’ (or other sf) and add files within there:
        - Add transformations folder:
            - This will hold all 23 SQL scripts for the staging to master transformations
        - Add the main python script (‘full_historical_load_etl_dag.py’) to create the DAG, tasks, and dependencies
            - Add the remaining scripts that are used as well
            - create_staging_schema.sql
            - create_master_schema.sql
            - staging_data_commands.sql
            - staging_finwire_load1.sql
            - load_staging_finwire_db.sql
        - Within the ‘plugins’ folder:
            - Add the python script (‘customermgmt_conversion.py’) for the customer management conversion from xml to csv
            - Ensure that ‘xmltodict’ module is installed within the container, or add the ‘xmltodict.py’ directly there, that is retrievable from official PyPi site https://pypi.org/project/xmltodict/
            - This is to ensure that the additional module is detected by airflow when running our python script


3. Open up a terminal within the airflow directory (preferably using VS code), and connect directly with the airflow CLI:

>`docker exec -it <CLI container id> <Container command> bash`

    The two values can be found by running:
>`docker ps --no-trunc`

    Values from 'CONTAINER ID' and 'COMMAND' column

    Example: 
>`docker exec -it d44b56bc5c993d564e85014abee743c707bc045834cf56f6a5ed42d353dc7d0d /usr/bin/dumb-init -- /entrypoint bash`

    Next, change directory to where the dag python script is located:

>`cd dags/sf_3`
    
4. Interact with the DAG directly from the airflow CLI
    - Run a specific task for testing:
    >`airflow dags test sf_3 create_staging_schema`
    - Trigger the whole DAG to run:
    >`airflow dags trigger sf_3`

5. Monitor the execution of the complete DAG through the airflow webserver itself
    - Logs can be checked to identify issues with task runs
    - Inspect the task instances to observe the time taken for each task and results

---