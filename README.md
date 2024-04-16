# Meta Movements Distribution Insights 

## Overview
This project is part of the [DEZoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) and focuses on analyzing Meta Movement Distribution data. The objective of this project is to apply the knowledge gained during the course to build the ETL data pipeline.

## Problem Statement
The main objective is to understand travel patterns based on Movement Distribution Maps that provides an overview of how far people in different regions travel
from home on a given day.

For more details please visit:

[**Movement Distribution Maps**](https://dataforgood.facebook.com/dfg/tools/movement-distribution-maps)

**Questions the dataset helps answer:**

1. How far do people travel from home on average?
2. How does this level of mobility vary with changes in disease prevalence, public-health
messaging and travel policy?

## About the Dataset

**Features of Movement Distribution Maps:**

1. Updated daily.
2. Available at administrative level 2 (equivalent to counties in the US) wherever there are
enough users to ensure privacy, or at level 1 (equivalent to states in the US) if level 2 is
unavailable.
3. Built using a standard methodology for the entire globe.
4. Available to download in csv format for analysis and input into epidemiological forecasts
and models


[Data Source](https://data.humdata.org/dataset/movement-distribution)
## Data Pipeline

The data pipeline consists of two Prefect flows:

1. **Ingest dataset:**
    - Web scrap dataset csv list into ./data folder
    - Unzip csv into ./data/unzips folder
    - Transform into parquet format and load to GCS Bucket

2. **Reporting Pipeline:**
    - Runs once every end of month
    - Tasks done:
        - Create tables in BigQuery else append rows
        - Start Google Dataproc Cluster
        - Read Parquet files in GCS and load them into tables
        - Table is partitioned by Date column (Each day has multiple area data)
        - Table is clustered by country column (To order each country)
        - Read the tables and create reporting tables with aggregated data
        - Stop Dataproc Cluster

## Project Architecture

![Project Architecture](https://github.com/NAMAN108282/meta_movement_distribution/blob/master/Project%20Architecture.png?raw=true)

**DataLake :** Google Cloud Platform Storage(GCS)

**DataWarehouse :** BigQuery

**Workflow Orchestration :** Prefect

**Transformations :** Pyspark

**IaC :** Terraform

**Visualisation Dashboards :** Google Looker Studio

## Results

Discover insights from live and interactive dashboard, which is updated monthly.

[View Live Dashboard](https://lookerstudio.google.com/reporting/4748eaa9-1d82-4c70-b00c-f3c4f472d213)




## Project Recreation Procedure

### Prerequisites:
- Python
- Google Cloud Account
- Google Cloud Service Account JSON Credentials for (BigQuery admin, Compute Engine, DataProc admin and Cloud Storage admin)
- Prefect Cloud
- Terraform

1. Clone this project repository
2. Refresh service-account auth-token

    `gcloud auth application-default login`

3. Add Service Account JSON file to /gcp_key folder
4. Change to terraform directory

     `cd terraform`
    
    Initialize terrform

    `terraform init`

    Change your GCP Project ID

    Verify the changes to be applied:

    `terraform plan`
    

    Apply the changes:

    `terraform apply`
5. Install required Python packages:

    `pip install -r requirements.txt`

6. Update variables 

    `/code/config.json`
  
    Update the config to your values for the following 

    - project_id
    - dataproc_cluster_name
    - your gcp region
    - bucket_name


7. Prefect Server :

    `prefect server start`


8. Prefect Block Setup

    Click on Blocks and create the following blocks:
    - GCP Credentials
    - GCS Bucket

9. Setup Agent

    
    In your terminal type:

    `prefect agent start --pool default-agent-pool`

10. Deploy the Prefect Pipelines which runs every end of the month

    `prefect deployment build code/ingest_data_gcs.py:save_gcs --name ingest_data_gcs --cron "59 22 L * *"  `

    `prefect deployment build code/pipeline.py:main_pipeline --name transform_report --cron "59 22 L * *" `
