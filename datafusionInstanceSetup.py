
from __future__ import print_function

from airflow import models
from airflow.operators import bash_operator
from airflow.utils import timezone
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.datafusion import (
    CloudDataFusionCreateInstanceOperator,
    CloudDataFusionCreatePipelineOperator,
    CloudDataFusionDeleteInstanceOperator,
    CloudDataFusionDeletePipelineOperator,
    CloudDataFusionGetInstanceOperator,
    CloudDataFusionListPipelinesOperator,
    CloudDataFusionRestartInstanceOperator,
    CloudDataFusionStartPipelineOperator,
    CloudDataFusionStopPipelineOperator,
    CloudDataFusionUpdateInstanceOperator,
)
from scripts import instanceSetup

import json
import os
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#
#                             !! (DO NOT MODIFY) !!
#
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# !! WARNING !! Modifying the DAG name invalidates the DAG composer configuration
dagIdentifier = "datafusionInstanceSetup"

# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#
#                          -- CALCULATED GLOBAL VARIABLES --
#
#                             !! (DO NOT MODIFY) !!
#
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

## -- -- -- -- -- -- -- -- -- -- -- --
## ws2 dag generic configuration
## -- -- -- -- -- -- -- -- -- -- -- --
dagsConfiguration = models.Variable.get("main", deserialize_json=True)
datafusionInstanceName = dagsConfiguration["datafusionInstanceName"]
datafusionInstanceType = dagsConfiguration["datafusionInstanceType"]
datafusionLocation = dagsConfiguration["datafusionLocation"]
dataprocServiceAccount = dagsConfiguration["dataprocServiceAccount"]
## -- -- -- -- -- -- -- -- -- -- -- --
## dag specific configuration
## -- -- -- -- -- -- -- -- -- -- -- --
dagVariableConfiguration = models.Variable.get(dagIdentifier, deserialize_json=True)

scriptsBucket = dagVariableConfiguration["scriptsBucket"]
artifactsBucket = dagVariableConfiguration["artifactsBucket"]
gcp_project_id = dagVariableConfiguration["gcp_project_id"]
vpc_name = dagVariableConfiguration["vpc_name"]
ip_range = dagVariableConfiguration["ip_range"]
datafusion_instance_type = dagVariableConfiguration["datafusion_instance_type"]
compute_account = dagVariableConfiguration["compute_account"]
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#
#                          -- TEMPLATES AND CONSTANTS --
#
#                             !! (DO NOT MODIFY) !!
#
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#
#                          -- CUSTOMIZE --
#
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

INSTANCE = {
    "type": datafusionInstanceType,
    "displayName": datafusionInstanceName,
    "dataprocServiceAccount": dataprocServiceAccount
}

enable_logging_cmd="""gcloud beta data-fusion instances update {0} \
    --project={1} \
    --location={2} \
    --enable_stackdriver_logging \
    --enable_stackdriver_monitoring """.format(datafusionInstanceName,gcp_project_id,datafusionLocation)

plugins= [
        {
            "name" : "CHANGE-ME",
            "jar": "CHANGE-ME",
            "props": "CHANGE-ME",
            "version": "CHANGE-ME",
            "extends": "CHANGE-ME"
        }
    ]



pipelines_json = []


# -- -- -- -- -- -- -- -- --
# default dag arguments
# -- -- -- -- -- -- -- -- --
default_dag_args = {
    'owner': 'ws2',
    'start_date': datetime(2020, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

with models.DAG(
        dagIdentifier,
        schedule_interval='0 8 * * *',
        #schedule_interval=None,
        catchup=False,
        default_args=default_dag_args) as dag:
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    #
    #                          -- OPERATIONS --
    #
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --


    create_instance = bash_operator.BashOperator(
            task_id= 'create-instance',
            bash_command=instanceSetup.CREATE_DF,
            params={
               'project_id' : gcp_project_id,
               'location': datafusionLocation,
               'datafusionInstanceName' : datafusionInstanceName,
               'datafusion_instance_type' : datafusion_instance_type,
               'vpc_name' : vpc_name,
               'ip_range' : ip_range,
               'compute_account': compute_account,
            },
            dag=dag
    )

    sleep = bash_operator.BashOperator(task_id="sleep", bash_command="sleep 60")

    enable_logging = bash_operator.BashOperator(
        task_id='enable-logging',
        bash_command=enable_logging_cmd,
    )

    install_plugins = []
    for i in range(len(plugins)):
        install_one_plugin = bash_operator.BashOperator(
            task_id='install-plugin-' + plugins[i]["name"],
            bash_command=instanceSetup.INSTALL_PLUGIN,
            params={
                'pluginName': plugins[i]["name"],
                'pluginJar': plugins[i]["jar"],
                'pluginProperties': plugins[i]["props"],
                'pluginVersion': plugins[i]["version"],
                'pluginExtends': plugins[i]["extends"],
                'datafusionInstanceName': datafusionInstanceName,
                'bucket': artifactsBucket,
            }
        )
        install_plugins.append(install_one_plugin)

    sleep2  = bash_operator.BashOperator(task_id="sleep2", bash_command="sleep 60")


    create_pipelines = []
    for i in range(len(pipelines_json)):
        create_one_pipeline = CloudDataFusionCreatePipelineOperator(
            location=datafusionLocation,
            pipeline_name=pipelines_json[i]["name"],
            instance_name=datafusionInstanceName,
            pipeline=pipelines_json[i],
            task_id="create-pipeline-" + pipelines_json[i]["name"],
        )
        create_pipelines.append(create_one_pipeline)

    barrier = bash_operator.BashOperator(task_id="barrier", bash_command="sleep 1")


    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    #
    #                          -- OPERATION DEPENDENCIES --
    #
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    for install_task in install_plugins:
        create_instance >> sleep >> enable_logging >> sleep2 >> install_task  >> barrier

    for create_pipeline in create_pipelines:
        barrier >> create_pipeline
