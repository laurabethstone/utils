conn_config = configuration.tableau_cloud_connection
workbook_name = "License Manager Dashboard"
project_name = None

# COMMAND ----------

# setup the connection
the_connection = get_tableau_connection(
  server=conn_config.tableau_cloud_url,
  api_version=conn_config.tableau_cloud_version,
  username=conn_config.username,
  password=conn_config.password,
  site_name=conn_config.site_name,
  site_url= conn_config.site_url
)

# COMMAND ----------

# search for workbooks to update

workbooks_sdf = find_workbooks_by_name(
  the_connection, 
  workbook_name=workbook_name
  ,project_name=project_name
  )

# display identified workbooks for debugging 
print("#### Workbooks identified to update ####")
workbooks_sdf.show(truncate=False)

# COMMAND ----------

# getting workbook ids from the list
workbooks_to_update = get_workbook_ids_as_list(workbooks_sdf)

if conn_config.do_not_execute == False:
  jobs = create_tableau_refresh_jobs(
    tableau_connection=the_connection, workbook_ids=workbooks_to_update)

  monitor_tableau_refresh_jobs(
    tableau_connection=the_connection, job_ids=jobs)
elif conn_config.do_not_execute == True:
  print('Skipping refresh')
