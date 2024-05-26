# Data Pipeline Project 

  

This project implements an ETL pipeline using Apache Airflow, Python, and PySpark. The pipeline extracts data from an Oracle Siebel database, transforms it using PySpark, loads it into a Teradata database, and validates the data. 

  
 
 
Architecture Diagram  
 <img width="614" alt="architecture diagram" src="https://github.com/Tushar-Ankush-More/data_pipeline_project/assets/170803153/ac64e011-ef0d-4543-a0ad-81ad8d08f7b0">

 
 
 
 
Data Flow: 
<img width="148" alt="repository diagram" src="https://github.com/Tushar-Ankush-More/data_pipeline_project/assets/170803153/35b7211f-adc7-44ec-b3c7-402922821682">

  

Data Source (Oracle Siebel): The pipeline starts by extracting data from the Oracle Siebel database. 

Data Extraction (extract.py): A Python script (extract.py) is responsible for extracting the data from the source database. 

Data Transformation (transform.py): Another Python script (transform.py) transforms the extracted data to prepare it for loading into the target system. This might involve cleaning, filtering, formatting, or aggregating the data. 

(Optional) Data Validation (validate.py): An optional stage represented by a script (validate.py) performs quality checks on the transformed data. This could involve checking for null values, duplicates, or other data integrity issues. Depending on your needs, data may bypass validation and proceed directly to loading. 

Data Loading (load.py): A third Python script (load.py) loads the transformed (and optionally validated) data into the target system, which is Teradata in this case. 

Target System (Teradata): This represents the final destination of the processed data, a Teradata database. 

Orchestration and Monitoring: 

  

Airflow Scheduler (etl_dag.py): This component, likely defined in a Python script (etl_dag.py), acts as the Airflow scheduler. It orchestrates the execution of the data pipeline stages in the desired order and schedule. 

Airflow Webserver: This component, not directly shown in the diagram but implied by the connection, is the Airflow web server. It provides a user interface for monitoring the pipeline execution, managing workflows, and scheduling tasks. 

Monitoring UI: This represents the user interface for monitoring the pipeline's execution status, logs, and potential errors. It likely interacts with the Airflow web server to retrieve this information. 

Overall Workflow: 

  

The Airflow scheduler triggers the execution of the data pipeline. 

The extract.py script extracts data from the Oracle Siebel database. 

The transform.py script transforms the extracted data. 

Optionally, the validate.py script performs data validation. 

The load.py script loads the transformed (and potentially validated) data into the Teradata database. 

The Airflow web server provides a monitoring UI for users to track the pipeline's progress and identify any issues. 

This data pipeline architecture ensures a well-structured and maintainable approach to moving data from the Oracle Siebel source to the Teradata target system. 

 
 
The repository is organized as follows: 
 
 

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

 
