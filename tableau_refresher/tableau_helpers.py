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
