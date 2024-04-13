from google.cloud import dataproc_v1



def get_dataproc_client(region: str) -> dataproc_v1.ClusterControllerClient:
    client_options = {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    return dataproc_v1.ClusterControllerClient(client_options=client_options)


def get_cluster_status(project_id, region, cluster_name):

    # Create the Dataproc client with the creds
    dataproc_client = get_dataproc_client(region) 

    # Get the cluster status
    cluster_status = dataproc_client.get_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name
    ).status.state

    return cluster_status

def wait_cluster_status(client: dataproc_v1.ClusterControllerClient, project_id: str, region: str, cluster_name: str, target_state: dataproc_v1.ClusterStatus.State, poll_interval_secs: int = 5):
    while True:
        cluster = client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
        if cluster.status.state == target_state:
            break
        time.sleep(poll_interval_secs)