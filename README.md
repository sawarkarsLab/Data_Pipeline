# Data Pipeline Project

This project implements an ETL pipeline using Apache Airflow, Python, and PySpark. The pipeline extracts data from an Oracle Siebel database, transforms it using PySpark, loads it into a Teradata database, and validates the data.

## Setup

1. **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2. **Configure environment variables**:
    Create a `.env` file in the root directory and add the following variables:
    ```
    DB_HOST=localhost
    DB_PORT=5432
    DB_USER=myuser
    DB_PASSWORD=mypassword
    ```

3. **Run the Airflow scheduler and web server**:
    ```bash
    airflow scheduler
    airflow webserver
    ```

4. **Access the Airflow UI**:
    Open your browser and go to `http://localhost:8080`. You can trigger the `etl_dag` from the Airflow UI.

## Running the Pipeline

1. **Trigger the DAG**:
    Manually trigger the `etl_dag` from the Airflow UI or let it run according to the schedule.

2. **Monitor the DAG**:
    Use the Airflow UI to monitor the progress of the DAG and check for any errors.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
