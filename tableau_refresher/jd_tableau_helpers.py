# Databricks notebook source
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pipelines.src.utilities.config import edl_config, clean_branch_name
import json as json
import time
import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_workbooks_dataframe
from pyspark.sql import SparkSession

# pd.DataFrame.iteritems = pd.DataFrame.items

# spark = SparkSession.builder.getOrCreate()
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
# spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")


# COMMAND ----------

class TableauApiConnection():
  def __init__(self):
    self.get_connection = self.get_connection()

  def get_connection(self):
    #  1 get tableau-api connection
    tableauSecret_dbutil = dbutils.secrets.get("your-secret-name", "your-appId-databricks-token")
    tableauUrl = 'https://tableaucloud.<your-company>.com'
    tableauToken = 'Databricks Secrets'
    tableauVersion = '3.21'
    tableau_server_config = {
      'tableau_prod': {
          'server': tableauUrl,
          'api_version': tableauVersion,
          'personal_access_token_name': tableauToken,
          'personal_access_token_secret': tableauSecret_dbutil,
          'site_name': '',
          'site_url': ''
        }
    }
    return TableauServerConnection(config_json=tableau_server_config, env='tableau_prod')

tableau_api_connection = TableauApiConnection()
tableau_connection = tableau_api_connection.get_connection
tableau_api_connection.get_connection.sign_in()

TAGS_TO_UPDATE = [
  edl_config.JUPITER_CSM_DASHBOARD_TAG
  ]

# COMMAND ----------

def get_workbook_dataframe_fom_tags(tableau_connection, TAGS_TO_UPDATE):
    #  2 get Pandas DataFrames containing your workbook and datasource details
    workbooks = get_workbooks_dataframe(tableau_connection)
    workbooks_df = workbooks[["name", "id", "tags"]]

    #  3 get your workbook details to dataframe and obtain WORKBOOK_ID value
    workbooks_df_sc = spark.createDataFrame(workbooks_df)

    tags_to_update_dataframe = (
        # get rid of the map's first wrapper
        workbooks_df_sc.select("name", "id", psf.explode("tags").alias("tag_col", "tag_array"))
        # posexplode array of multiple maps into rows
        .select("name", "id", psf.posexplode("tag_array").alias('array_group', 'array_map'))
        # explode array of multiple maps into columns
        .select("name", "id", psf.explode("array_map").alias('tag_label', 'tag_value')).drop('tag_label')
        # get the id's we need by the tags we assign
        .filter(psf.col("tag_value").isin(*TAGS_TO_UPDATE))
    )
    return tags_to_update_dataframe


workbook_df = get_workbook_dataframe_fom_tags(tableau_connection, TAGS_TO_UPDATE)
# # check df of workbook names and id's
# display(workbook_df)


workbook_ids = [x["id"] for x in workbook_df.rdd.collect()]
# # check list of id's to be sent
# print(workbook_ids_to_update_df)


def get_responses_from_update_workbooks(tableau_connection, workbook_ids):
    responses_json = []
    responses_json_df = []
    responses_df = []
    responses_job_ids = []

    for workbook_id in workbook_ids:
        responses = tableau_connection.update_workbook_now(f'{workbook_id}')
        print(responses)
        responses_json = responses.json()
        responses_json_df = spark.read.json(sc.parallelize([json.dumps(responses_json)]))
        responses_df = responses_json_df.rdd.map(lambda x: (x.job)).toDF(["job"])
    return responses_df


responses_df = get_responses_from_update_workbooks(tableau_connection, workbook_ids)
# display(respnses_df)

JOB_LIST_TO_MONITOR = [x["id"] for x in responses_df.rdd.collect()]
# check list of id's to be sent
# print(JOB_LIST_TO_MONITOR)

# COMMAND ----------

def get_finish_code_from_query_job(tableau_connection, JOB_LIST_TO_MONITOR):
    job_response_json = []
    job_responses_json_df = []
    job_response_df = []
    job_response_finishCode = []

    for job_id in JOB_LIST_TO_MONITOR:
        # get the job information
        job_responses = tableau_connection.query_job(f'{job_id}')
        print(job_responses)
        job_response_json = job_responses.json()
        job_responses_json_df = spark.read.json(sc.parallelize([json.dumps(job_response_json)]))
        job_response_df = job_responses_json_df.rdd.map(lambda x: (x.job)).toDF(["job"])
        job_response_finishCode = job_response_df.select("finishCode").collect()[0][0]

        # # check the finishCode and take the correct action
        if job_response_finishCode == '0': # successful
            print(f'Job id {job_id} has completed successfully')

        elif job_response_finishCode == '1': # failed
            notes = job_responses_json_df.get('job').get('extractRefreshJob').get('notes')
            raise Exception(f'Job id {job_id} has failed, error : {notes}')

        elif job_response_finishCode == '2': # canceled
            raise Exception(f'job id :{job_id} canceled')

        elif job_response_finishCode == None: # not completed
            print(f'job {job_id} still working')
            pass
        else: # should never happen, all status are covered
            raise Exception(f"ERROR: Unknown finishCode: {job_response_finishCode}")

        # return job_response_finishCode

get_finish_code_from_query_job(tableau_connection, JOB_LIST_TO_MONITOR)
# job_response_finishCode = get_finish_code_from_query_job(tableau_connection, JOB_LIST_TO_MONITOR)
# display(job_response_finishCode)
