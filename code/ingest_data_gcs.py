from bs4 import BeautifulSoup
import requests, os
from pathlib import Path
import zipfile
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from config import read_config



@task(log_prints=True, retries=3, retry_delay_seconds=30)
def urls_scrapper(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    urls = []
    for link in soup.find_all("a", {"class": "btn btn-empty btn-empty-blue hdx-btn resource-url-analytics ga-download"}):
        cur_url = link.get('href')
        urls.append(cur_url)

    return urls

@task(log_prints=True)
def write_gcs(urls, gcs_block):
    prefix = "https://data.humdata.org"
    gcs_block = GcsBucket.load(gcs_block)

    for current_url in urls[2:]:
        file_url =  prefix + current_url
        file_name = current_url.split("/")[-1]
        if os.path.exists("data/" + file_name):
            print("File " + file_name + " already exists")
            continue
        else:
            os.system('wget ' + file_url + ' -O ' + "data/" + file_name)
        #pandas read
        with zipfile.ZipFile("data/" + file_name) as zi:
            zi.extractall("data/unzips/")
            zips = zi.namelist()
            
            for each_csv in zips:
                df = pd.read_csv("data/unzips/" + each_csv, header=0, sep=',', encoding='utf-8')
                gcs_block.upload_from_dataframe(
                    df=df,
                    to_path="data/"+each_csv,
                    serialization_format='parquet'
                    )


@task(log_prints=True)
def write_files(gcs_block):
    files_arr = os.listdir("code/")
    gcs_block = GcsBucket.load(gcs_block)
    for file in files_arr[:-1]:
        gcs_block.upload_from_path(from_path="code/"+file, to_path="code/" + file)


@flow()
def save_gcs():
    url = "https://data.humdata.org/dataset/movement-distribution"
    config = read_config()   
    gcs_block = config['prefect_gcs_block']

    urls = urls_scrapper(url)
    write_gcs(urls, gcs_block)
    write_files(gcs_block)


if __name__ == '__main__':
    save_gcs()
