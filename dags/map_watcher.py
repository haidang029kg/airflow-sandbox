import json
import pathlib
from pydoc import doc
import pendulum
from datetime import datetime
from typing import Dict, List
from airflow.decorators import task
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
from airflow.providers.mongo.hooks.mongo import MongoHook

from map_watcher_utils.interface import ClientConfig

with DAG(
    dag_id="map_watcher",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@daily",
    tags=["example"],
) as dag:

    @task(
    )
    def fetch_clients():
        mw_hook = MongoHook(
            conn_id='vps_mongo',
        )
        res = mw_hook.find(
            mongo_collection='mw_clients',
            query={}
        )
        return [
            ClientConfig(
                ele['client_id'], ele.get('database_id', None), True).__dict__
            for ele in res
        ]

    t = fetch_clients()
