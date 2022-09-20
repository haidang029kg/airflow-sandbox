import json
import pathlib
import pendulum
from datetime import datetime
from typing import Dict, List
import grequests
from airflow.decorators import task, dag
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

URL = "https://ll.thespacedevs.com/2.0.0/launch/upcoming"

VOLUME_NAME = '/opt/airflow/shared_volume'


with DAG(
    dag_id="task_flow_api_v1",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@daily",
    tags=["example"],
) as dag:
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
    def download_all_urls(input: dict):
        list_image_urls = input['image_urls']
        print(list_image_urls)
        return list_image_urls

    @task()
    def download_one(image_url: str):
        res_file_name = f"{VOLUME_NAME}/images/{image_url.split('/')[-1]}"
        return res_file_name

        # resp_req = grequests.get(image_url)
        # print(resp_req)
        # res_data = grequests.map([resp_req])
        # print(resp_req)
        # data = res_data[0]

        # print(res_file_name)

        # with open(res_file_name, "wb") as _f:
        #     _f.write(data.content)

    image_url_dict = get_picture_urls()
    urls = download_all_urls(image_url_dict)
    res = download_one.expand(image_url=urls)
