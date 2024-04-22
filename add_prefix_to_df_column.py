
def add_prefix_to_df_column(prefix_str:None,input_df:None):
  if prefix_str == None:
    prefix_str = "CSPT115_"
  else:
    prefix_str

  if input_df == None:
    input_df = daily_field_summary_CSPT115
  else:
    input_df = input_df

  # data_frame = ""
  # Get all the columns of data frame in a list
  total_columns = input_df.columns
 
  # Run loop to dynamically rename multiple columns
  # in PySpark DataFrame with prefix 'class_'
  for i in range(len(total_columns)):
    # print(total_columns)
    input_df = input_df.withColumnRenamed(total_columns[i], prefix_str + total_columns[i])
  return input_df
