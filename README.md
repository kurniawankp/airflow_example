# Airflow Example

Airflow example for building etl from sqlite datasource to data warehouse (bigquery). SQLite data will be converted into avro file, and then uploaded into google cloud storange and connected to bigquery.


## Data Source

SQLite with following tables :

* Country
* League
* Match
* Player
* Player_Attributes
* Team
* Team_Attributes

unzip data source data/sqlite_data/database.sqlite.zip

## Airflow Installation

To use this code, you can use this following instruction
or you can follow instruction from 
apache airflow documentation on https://airflow.apache.org/

* install apache airflow

    ```bash
    AIRFLOW_VERSION=2.1.3
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    # For example: 3.6
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    # For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.1.3/constraints-3.6.txt
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    ```
* Define airflow home path
    
    ```bash
    export AIRFLOW_HOME=path_to_airflow_home
    ```
    default value is ~/airflow. This path is used to store dags
    folder, database for airflow if you use sqlite, and other configuration.

* Initialize database

    ```bash
    airflow db init
    ```
* create user

    ```bash
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname Data \
        --role Admin \
        --email data@engineer.com
    ```
    Feel free to change username,firstname,lastname, and email with whatever 
    value you want
* start web server

    ```bash
    airflow webserver -p 8080
    ```
* start the scheduler

    open new terminal
    ```bash
    airflow scheduler
    ```

* visit http://localhost:8080/

    Use admin account you created to login to airflow

## GCP Setup

In this example I use Google Cloud Storage and Bigquery service 
from Google Cloud Platform. You need to activate both of the service and 
create service account so you can interact with those service. Please
follow GCP documentation :

* [google cloud storage](https://cloud.google.com/storage/docs)
* [bigquery](https://cloud.google.com/bigquery/docs)
* [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console)

## Usage

* install all package

    ```bash
    pip install -r requirements.txt
    ```
* create airflow connection for GCP

    * in airflow interface go to **Admin>Connections**
    * follow [this instruction](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html) to setup GCP Connection in Airflow
* create airflow variable

    * in airflow interface go to **Admin>Variables**
    * follow [this instruction](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
    * for this example you need to create some of variables:

        * HOME_PATH
        * GCS_BUCKET_NAME
        * STAGING_DATASET
        * PRODUCTION_DATASET
        * PROJECT_NAME
        * AIRFLOW_CONN_GCP_CONN
* enable the dags
* trigger the dag by clicking the play button in action column
![images](https://raw.githubusercontent.com/kurniawankp/airflow_example/main/images/airflow%20home%20page.png)

