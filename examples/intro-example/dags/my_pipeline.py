# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.incubator.apache.org/tutorial.html)
"""
from datetime import datetime, timedelta

# import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ml_pipeline',
    default_args=default_args,
    description='A simple ml pipeline',
    schedule_interval=timedelta(days=1),    
) as dag:

    get_data = BashOperator(
        task_id='get_data',
        bash_command='echo "{{ ds }}" && cd /usr/local/airflow/volume && python -m get_data --data_dir /usr/local/airflow/volume --date_time "{{ ds }}"',
    )

    preproc = BashOperator(
        task_id='preproc',
        depends_on_past=True,
        bash_command='cd /usr/local/airflow/volume && python -m preproc --data_dir /usr/local/airflow/volume --date_time "{{ ds }}"',
    )

    train_predict = BashOperator(
        task_id='train_predict',
        depends_on_past=True,
        bash_command='cd /usr/local/airflow/volume && python -m train_predict --data_dir  /usr/local/airflow/volume --date_time "{{ ds }}"',  
        # params={'my_param': 'Parameter I passed in'},
    )

get_data >> preproc >> train_predict 