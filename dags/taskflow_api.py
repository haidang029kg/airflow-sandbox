import json
import pathlib
import random
import pendulum
from datetime import datetime
from typing import Dict, List
import requests
from airflow.decorators import task, dag
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
from airflow.operators.python import ShortCircuitOperator

from airflow.exceptions import AirflowSkipException
URL = "https://ll.thespacedevs.com/2.0.0/launch/upcoming/"

VOLUME_NAME = '/opt/airflow/shared_volume'


def pre_action():
    print('gererate data source')


def gen_choice():
    return 'pre_action'


@task(
    task_id='call_api'
)
def call_api() -> List:
    res = requests.get(URL)
    if res.status_code == 200:
        data = res.json()
        total = data['count']

        limit = 10

        num_pages = int(total / limit) + 1

        res = [
            {"limit": limit, "offset": limit * (ele + 1)}
            for ele in range(num_pages)]
        print(res)
        return res

    return []


@task(max_active_tis_per_dag=1)
def call_api_paging(in_data) -> dict:
    res = requests.get(
        f"{URL}?limit={in_data['limit']}&offset={in_data['offset']}")
    if res.status_code == 200:
        data = res.json()
        return data
    print(res.status_code)
    return {
        "results": [],
        "status_code": res.status_code
    }


@task()
def write_json_file(list_data_json: List[dict]) -> str:
    pathlib.Path(
        f"{VOLUME_NAME}/images").mkdir(parents=True, exist_ok=True)

    path_file = f"{VOLUME_NAME}/launches.json"

    results = []
    for ele in list_data_json:
        results.extend(ele['results'])

    with open(path_file, "w") as f:
        json_object = json.dumps({
            "results": results
        })
        f.write(json_object)

    return path_file


@ task()
def get_picture_urls(path_file: str) -> Dict:
    # Ensure directory exists
    with open(path_file) as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        return {'image_urls': image_urls}


@ task()
def download_all_urls(input: dict):
    list_image_urls = input['image_urls']
    print(list_image_urls)
    return list_image_urls


@ task()
def download_one(image_url: str):
    res_file_name = f"{VOLUME_NAME}/images/{image_url.split('/')[-1]}"
    res = requests.get(image_url)
    with open(res_file_name, "wb") as f:
        f.write(res.content)
    return res_file_name


with DAG(
    dag_id="task_flow_api_v1",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@daily",
    tags=["example"],
) as dag:

    run_this_first = DummyOperator(
        task_id='run_this_first',
    )
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=gen_choice,
    )

    task_gen_ds = PythonOperator(
        task_id='pre_action',
        python_callable=pre_action
    )

    run_this_first >> branching >> task_gen_ds

    res_params_paging = call_api()

    [task_gen_ds, branching] >> res_params_paging

    res_list_data = call_api_paging.expand(in_data=res_params_paging)
    path_file = write_json_file(res_list_data)
    image_url_dict = get_picture_urls(path_file)
    urls = download_all_urls(image_url_dict)
    res = download_one.expand(image_url=urls)
