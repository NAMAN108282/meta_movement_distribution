from datetime import datetime

from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import StartClusterRequest

from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table
from utils import  get_dataproc_client, wait_cluster_status
from config import read_config

@task(log_prints=True)
def submit_batch(
    job_name: str,
    python_file: str,
    bucket: str,
    cluster_name: str,
    project_id: str,
    region: str,
    dataset_id: str,
):

    job_client = dataproc_client = get_dataproc_client(region)
    current_timestamp = round(datetime.now().timestamp())
    job = {
        "placement": { 
            "cluster_name": cluster_name
        },
        "reference": {
            "job_id": f"job-{job_name}-{current_timestamp}",
            "project_id": project_id
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{bucket}/code/{python_file}",
            "properties": {},
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
            "python_file_uris": [f"gs://{bucket}/code/utils.py",
                                f"gs://{bucket}/code/config.py"],
            "file_uris":[f"gs://{bucket}/code/config.json",]
        }
    }
    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    return operation.result()

@task(log_prints=True)
def start_cluster(project_id: str, region: str, cluster_name: str):
    dataproc_client = get_dataproc_client(region)

    cluster_status = dataproc_client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name).status.state
    if cluster_status == dataproc_v1.ClusterStatus.State.RUNNING:
        print(f"Cluster {cluster_name} is already running!")
        return
    request = StartClusterRequest(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name
    )

    operation = dataproc_client.start_cluster(request=request)
    print("Waiting to start cluster!")

    response = operation.result()
    wait_cluster_status(dataproc_client, project_id, region, cluster_name, dataproc_v1.ClusterStatus.State.RUNNING)

    print("Cluster started successfully!")

@task(log_prints=True)
def stop_cluster(  
    project_id: str, region: str, cluster_name: str):
    dataproc_client = dataproc_client = get_dataproc_client(gcp_key, region)

    cluster_status = dataproc_client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name).status.state
    if cluster_status == dataproc_v1.ClusterStatus.State.STOPPED:
        print(f"Cluster {cluster_name} is already stopped!")
        return

    request = dataproc_v1.StopClusterRequest(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
    )

    operation = dataproc_client.stop_cluster(request=request)

    print("Waiting to stop cluster!")
    response = operation.result()

    # Wait for the cluster to be in the STOPPED status
    wait_cluster_status(dataproc_client, project_id, region, cluster_name, dataproc_v1.ClusterStatus.State.STOPPED)

    print("Cluster instance stopped successfully!")

@flow()
def main_pipeline(target_date: str = None) -> None:

    # Params to submit spark job  
    config = read_config()   
    project_id = config['project_id']
    cluster_name = config['cluster_name']
    region = config['region']
    bucket = config['bucket']
    prefect_gcp_block = config['prefect_gcp_block']
    dataset_id = config['dataset_id']

    gcp_key = GcpCredentials.load('prefect-gcp-creds')
    
    
    start_cluster(  
    project_id, region, cluster_name)

    submit_batch(
        job_name="transform",
        python_file="transform.py",
        bucket=bucket,
        cluster_name=cluster_name,
        project_id=project_id,
        region=region,
        dataset_id=dataset_id
    )

    
    stop_cluster(
    project_id, region, cluster_name)


if __name__ == '__main__':
    main_pipeline()
