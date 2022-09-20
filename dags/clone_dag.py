import pendulum

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="clone_dag",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@daily",
    tags=["example"],
) as dag:
    branch_1 = BranchPythonOperator(
        task_id="branch_1", python_callable=lambda: "true_1")
    join_1 = EmptyOperator(
        task_id="join_1", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    true_1 = EmptyOperator(task_id="true_1")
    false_1 = EmptyOperator(task_id="false_1")
    branch_2 = BranchPythonOperator(
        task_id="branch_2", python_callable=lambda: "true_2")
    join_2 = EmptyOperator(
        task_id="join_2", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    true_2 = EmptyOperator(task_id="true_2")
    false_2 = EmptyOperator(task_id="false_2")
    false_3 = EmptyOperator(task_id="false_3")

    branch_1 >> true_1 >> join_1
    branch_1 >> false_1 >> branch_2 >> [
        true_2, false_2] >> join_2 >> false_3 >> join_1
