# Databricks notebook source
# MAGIC %md 
# MAGIC Requires Packages:
# MAGIC * tableau-api-lib

# COMMAND ----------

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_workbooks_dataframe
import pyspark.sql.functions as psf
import time

# COMMAND ----------

def get_tableau_connection(
  server, api_version, username, 
  password, site_name, site_url):

  tableau_server_config = {
  'tableau_prod': {
      'server': server,
      'api_version': api_version,
      'username': username,
      'password': password,
      'site_name': site_name,
      'site_url': site_url
      }
  }

  return TableauServerConnection(
    config_json=tableau_server_config, env='tableau_prod')
  

# COMMAND ----------

def create_tableau_refresh_jobs(tableau_connection, workbook_ids):
  print("#### creating tableau refresh jobs ####")
  # a list of the responses
  responses_json = []
  jobs = []

  # sign into the server
  tableau_connection.sign_in()
  
  for workbook_id in workbook_ids:
      # call api to refresh workbook
      responses = tableau_connection.update_workbook_now(f'{workbook_id}')
      
      # Status code shoud be 202, throw an error if not
      if responses.status_code != 202:
        raise Exception(f'workbook_id: {workbook_id}, status_code:{responses.status_code}') 
      
      job_id = responses.json().get('job').get('id')
      print(f'created job: workbook: {workbook_id}, job: {job_id}')

      jobs.append(job_id)


  return jobs 

# COMMAND ----------

def monitor_tableau_refresh_jobs(tableau_connection, job_ids, refresh_minutes=1):
  print("#### starting to monitor tableau refresh jobs ####")
  tableau_connection.sign_in()
  keep_going = True
  
  while keep_going:

    remove = []

    for j in job_ids:
      # get the job information
      job_response = tableau_connection.query_job(j).json()
      
      
      # get the finishCode
      finishCode = get_finishCode(job_response=job_response)
      
      # get the job id 
      job_id = get_job_id(job_response=job_response)
      
      # check the finishCode and take the correct action
      if finishCode == '0': # successful
        # print(f'job completed successfully, removing job_id {job_id}')
        print(f'Job id {job_id} has completed successfully.')
        remove.append(job_id)
      elif finishCode == '1': # failed
        notes = job_response.get('job').get('extractRefreshJob').get('notes')
        raise Exception(f'Job id {job_id} has failed, error : {notes}')
      elif finishCode == '2': # canceled
        raise Exception(f'job id :{job_id} CANCELED!!!')
      elif finishCode == None: # not completed
        # print(f'job {job_id} still working.') 
        pass
      else: # should never happen, all status are covered
        raise Exception(f"ERROR: Unknown finishCode: {finishCode}")

    # remove completed jobs
    for r in remove:
      jobs.remove(r)

    # terminate loop or wait
    if len(jobs) == 0:
      keep_going = False
      print('All jobs have finished.')
    else:
      time.sleep(60*refresh_minutes) # checking once a minute
  return None


def get_finishCode(job_response):
  try:
    finishCode = job_response.get('job').get('finishCode')
  except: 
    finishCode = None
  return finishCode

def get_job_id(job_response):
  try:
    job_id = job_response.get('job').get('id')
  except:
    job_id = None
    print(job_response)
  return job_id

# COMMAND ----------

def get_workbook_list(tableau_connection):
  tableau_connection.sign_in()
  workbooks = get_workbooks_dataframe(tableau_connection)
  workbooks = workbooks[workbooks['project'].notnull()] # clean out bad data
  workbooks_df = workbooks[["name", "id", "tags", "project"]]
  return (
    spark
    .createDataFrame(workbooks_df)
    .withColumn("project_name", psf.col('project').getItem('name'))
    .drop("project")
  )

# COMMAND ----------

def find_workbooks_by_name(tableau_connection, workbook_name, project_name = None):
  workbooks = (
    get_workbook_list(tableau_connection=tableau_connection)
    .filter(psf.col('name') == workbook_name)
    .select('id','name','project_name')
  )
  if project_name is not None:
    workbooks = workbooks.filter(psf.col('project_name') == project_name)
  return workbooks

# COMMAND ----------

def get_workbook_ids_as_list(workbooks_sdf):
  return [x["id"] for x in workbooks_sdf.rdd.collect()]

# COMMAND ----------

def explode_workbook_tags(workbooks):
  return (
    workbooks
    .select("id", "name","project_name",
            psf.explode("tags").alias("tag_col", "tag_array"))
    .select("id", "name","project_name",
            psf.posexplode("tag_array").alias('array_group', 'array_map'))
    .select("id", "name","project_name",
            psf.explode("array_map").alias('tag_label', 'tag_value')).drop('tag_label')
  )

# COMMAND ----------

def find_workbooks_by_tag(tableau_connection, tag, project_name = None):
  workbooks = (
    explode_workbook_tags(get_workbook_list(tableau_connection=tableau_connection))
    .filter(psf.col('tag_value') == tag)
    .select('id','name','project_name', 'tag_value')
  )
  if project_name is not None:
    workbooks = workbooks.filter(psf.col('project_name') == project_name)
  return workbooks
