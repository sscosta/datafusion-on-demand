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


# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#
#                             !! (DO NOT MODIFY) !!
#
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# !! WARNING !! Modifying the DAG name invalidates the DAG composer configuration
dagIdentifier = "datafusionInstanceDestroy"

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
datafusionLocation     = dagsConfiguration["datafusionLocation"]
dataprocServiceAccount = dagsConfiguration["dataprocServiceAccount"]
datafusionPipelineName = dagsConfiguration["datafusionPipelineName"]
## -- -- -- -- -- -- -- -- -- -- -- --
## dag specific configuration
## -- -- -- -- -- -- -- -- -- -- -- --
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
        schedule_interval='0 20 * * *',
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

    delete_pipeline = CloudDataFusionDeletePipelineOperator(
        location=datafusionLocation,
        pipeline_name=datafusionPipelineName,
        instance_name=datafusionInstanceName,
        task_id="delete_pipeline_edm",
    )


    delete_instance = CloudDataFusionDeleteInstanceOperator(
        location=datafusionLocation,
        instance_name=datafusionInstanceName,
        task_id="delete_instance"
    )



    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    #
    #                          -- OPERATION DEPENDENCIES --
    #
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    # -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    delete_pipeline >> delete_instance