import json
import pathlib
from datetime import datetime
from typing import Dict, List
import grequests
from airflow.decorators import task, dag
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
)
URL = "https://ll.thespacedevs.com/2.0.0/launch/upcoming"

VOLUME_NAME = '/opt/airflow/shared_volume'

with DAG(
    dag_id="download_rocket_launches",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
) as dag:

    @task()
    def download_file_json() -> None:
        response = [grequests.get(URL)]
        print(response)
        response = grequests.map(response)
        print(response)
        response = response[0]
        print(response)
        with open(f"{VOLUME_NAME}/launches.json", "wb") as _f:
            _f.write(response.content)

    @task()
    def get_picture_urls() -> Dict:
        # Ensure directory exists
        pathlib.Path(
            f"{VOLUME_NAME}/images").mkdir(parents=True, exist_ok=True)
        with open(f"{VOLUME_NAME}/launches.json") as f:
            launches = json.load(f)
            image_urls = [launch["image"] for launch in launches["results"]]
            return {'image_urls': image_urls}

    @task()
    def download_one(image_urls: List[str]):
        res_file_name = [
            f"{VOLUME_NAME}/images/{name.split('/')[-1]}"
            for name in image_urls
        ]

        resp_req = (grequests.get(url) for url in image_urls)
        res_data = grequests.map(resp_req)

        for target_file, data in zip(res_file_name, res_data):
            with open(target_file, "wb") as _f:
                _f.write(data.content)

    @task()
    def download_pictures(picture_data: dict):
        image_urls = picture_data.get('image_urls')
        download_one.expand(image_urls=[[ele] for ele in image_urls])
        
    run_get_pictures = get_picture_urls()
    run_download_pictures = download_pictures(run_get_pictures)
