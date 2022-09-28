FROM apache/airflow:2.3.4

RUN pip install autopep8 grequests pyopenssl

RUN pip install apache-airflow-providers-mongo