import pandas as pd
import numpy as np
import json
import requests
import datetime
import itertools
from pyspark.sql import SparkSession
import time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType,BooleanType,MapType,ArrayType
from pyspark.sql.functions import col,round
from datetime import datetime,timedelta
import concurrent.futures
from pyspark.sql import functions as F
from pyspark.sql.functions import create_map,lit,expr
import logging
import gc



current_date = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
SINCE = f"'{yesterday} 00:00:00 UTC'"
UNTIL = f"'{current_date} 00:00:00 UTC'"
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
target_table_name = "salesiq_ingestion_daily_raw_f"
target_backup_table_name = "salesiq_ingestion_daily_raw_f_backup"

def read_config(location: str) -> dict:
    with open(location) as f_handle:
        try:
            config = yaml.safe_load(f_handle)
            return config
        except yaml.YAMLError as excp:
            raise excp

loc = os.path.dirname(__file__)
conf_loc = os.path.join(loc, "salesiq_config.yml")
config = read_config(conf_loc)
API_ENDPOINT = config['api']



def get_connection_credential_(env='staging'):
  if env == 'prod':
    role=config['role']['prod']
    private_key = dbutils.secrets.get(scope="jlin2-snowflake",key='jlin2_prod_env_pk')
    snowflake_url = config['snowflake_url']['prod']
    snowflake_warehouse = config['snowflake_warehouse']['prod']
    snowflake_database = config['snowflake_database']['prod']
    schema = config['schema']['prod']
    username = config['service_account']['prod']
  elif env == 'staging':
    private_key = dbutils.secrets.get(scope="jlin2-snowflake",key='jlin2_staging_env_pk')
    snowflake_url = config['snowflake_url']['staging']
    snowflake_warehouse = config['snowflake_warehouse']['staging']
    username = config['service_account']['staging']
    snowflake_database = config['snowflake_database']['staging']
    role = config['role']['staging']
    schema = config['schema']['staging']
  elif env == 'dev':
    role=config['role']['dev']
    private_key = dbutils.secrets.get(scope="jlin2-snowflake",key='jlin2_snowflake_rsa_pem')
    snowflake_url = config['snowflake_url']['dev']
    snowflake_warehouse = config['snowflake_warehouse']['dev']
    snowflake_database = config['snowflake_database']['dev']
    schema = config['schema']['dev']
    username = config['service_account']['dev']
  options = dict(sfUrl=snowflake_url,
              sfUser=username,
              pem_private_key=private_key,
              sfDatabase=snowflake_database,
              sfSchema=schema,
              sfRole=role,
              sfWarehouse=snowflake_warehouse)
  return options



# add queries in the list , each of the queries is a column in the output table
def get_nr_query_list(SINCE,UNTIL):
  nr_query_list = [
    """SELECT count(*) as 'total_count_ajax_request_sub_account' FROM AjaxRequest  SINCE {} until {} """.format(SINCE,UNTIL),
  
    """SELECT count(newrelic.timeslice.value) as 'total_transaction_per_day_apm' FROM Metric WHERE metricTimesliceName in ('HttpDispatcher', 'OtherTransaction/all') AND appName LIKE '%' 
  SINCE {} until {} """.format(SINCE,UNTIL),

  """SELECT count(*) as total_count_mobile_requests FROM MobileRequest SINCE {} until {} """.format(SINCE,UNTIL),
  
  """SELECT count(*) as count_pageview_associated_browser_transactions, uniqueCount(session) as total_count_unique_pageview_sessions FROM PageView SINCE {} until {} """.format(SINCE,UNTIL), 

  """SELECT count(*) AS 'total_transactions_per_day_opentelemetry' FROM Span WHERE service.name LIKE '%' AND (span.kind LIKE 'server' OR span.kind LIKE 'consumer' OR kind LIKE 'server' OR kind LIKE 'consumer') SINCE {} until {} """.format(SINCE,UNTIL), 

# Number of entities sending logs and log line count by source
"""
SELECT 

count(*) as total_count_log_lines_sent_by_entities_logs,

filter(uniqueCount(entity.guid), WHERE newrelic.source = 'api.logs.otlp') as unique_count_entities_sending_logs_oltp_source,

filter(count(entity.guid), WHERE newrelic.source = 'api.logs.otlp') as count_entities_sending_logs_oltp_source,

filter(uniqueCount(entity.guid), WHERE newrelic.source = 'logs.APM') as unique_count_entities_sending_logs_apm_log_source,

filter(count(entity.guid), WHERE newrelic.source = 'logs.APM') as count_entities_sending_logs_apm_log_source,

filter(uniqueCount(entity.guid), WHERE newrelic.source NOT IN ('logs.APM', 'api.logs.otlp')) as unique_count_entities_sending_logs_not_oltp_apm_source,

filter(count(entity.guid), WHERE newrelic.source NOT IN ('logs.APM', 'api.logs.otlp')) as count_entities_sending_logs_not_oltp_apm_source

FROM Log SINCE {} until {} """.format(SINCE,UNTIL),

# Distributed Traces  (Number of entities sending traces and span count by ingest point)
"""
SELECT 

count(*) as count_spans_associated_entities_sending_traces,

filter(uniqueCount(entity.guid), WHERE newRelic.ingestPoint = 'api.traces') as unique_count_entities_sending_traces_api_trace_ingestion_point,

filter(count(*), WHERE newRelic.ingestPoint = 'api.traces') as count_entities_sending_traces_api_trace_ingestion_point,

filter(uniqueCount(entity.guid), WHERE newRelic.ingestPoint = 'browser.spans') as unique_count_entities_sending_traces_browser_spans_ingestion_point,

filter(count(*), WHERE newRelic.ingestPoint = 'browser.spans') as count_entities_sending_traces_browser_spans_ingestion_point,

filter(uniqueCount(entity.guid), WHERE newRelic.ingestPoint = 'xray.polling') as unique_count_entities_sending_traces_xray_polling_ingestion_point,

filter(count(*), WHERE newRelic.ingestPoint = 'xray.polling') as count_entities_sending_traces_xray_polling_ingestion_point,

filter(uniqueCount(entity.guid), WHERE newRelic.ingestPoint = 'mobile.spans') as unique_count_entities_sending_traces_mobile_spans_ingestion_point,

filter(count(*), WHERE newRelic.ingestPoint = 'mobile.spans') as count_entities_sending_traces_mobile_spans_ingestion_point,

filter(uniqueCount(entity.guid), WHERE newRelic.ingestPoint = 'serverless.lambda') as unique_count_entities_sending_traces_serverless_lambda_ingestion_point,

filter(count(*), WHERE newRelic.ingestPoint = 'serverless.lambda') as count_entities_sending_traces_serverless_lambda_ingestion_point,

filter(uniqueCount(entity.guid), WHERE newRelic.ingestPoint NOT IN ('api.traces', 'browser.spans', 'serverless.lambda', 'xray.polling', 'mobile.spans')) AS unique_total_count_other_entities_not_in_list,

filter(count(*), WHERE newRelic.ingestPoint NOT IN ('api.traces', 'browser.spans', 'serverless.lambda', 'xray.polling', 'mobile.spans')) AS count_spans_not_in_list

FROM Span SINCE {} until {} """.format(SINCE,UNTIL),

#Mobile (Number of  transactions, entities, sessions and devices by agent type)

"""
SELECT

count(*) as total_count_mobiles_associated_mobile_transactions,

filter(count(*), WHERE newRelicAgent = 'AndroidAgent') as count_mobile_transaction_android_agent_type,

filter(count(*), WHERE newRelicAgent = 'iOSAgent') as count_mobile_transaction_ios_agent_type,

filter(count(*), WHERE newRelicAgent NOT IN ('iOSAgent', 'AndroidAgent')) as count_mobile_transaction_not_android_ios_agent_type,


filter(uniqueCount(entityGuid), WHERE newRelicAgent = 'AndroidAgent') as unique_count_mobile_transaction_android_agent_type,

filter(uniqueCount(entityGuid), WHERE newRelicAgent = 'iOSAgent') as unique_count_mobile_transaction_ios_agent_type,

filter(uniqueCount(entityGuid), WHERE newRelicAgent NOT IN ('iOSAgent', 'AndroidAgent')) as unique_count_entities_associated_mobile_transactions_not_android_ios_agent,

uniqueCount(sessionId) as unique_count_sessions_associated_mobile_transactions,

filter(uniqueCount(sessionId), WHERE newRelicAgent = 'AndroidAgent') as unique_count_sessions_android_agent_type,

filter(uniqueCount(sessionId), WHERE newRelicAgent = 'iOSAgent') as unique_count_sessions_ios_agent_type,

filter(uniqueCount(sessionId), WHERE newRelicAgent NOT IN ('iOSAgent', 'AndroidAgent')) as unique_count_sessions_not_android_ios_agent_type,

uniqueCount(uuid) as unique_count_uuid_associated_mobile_transactions,

filter(uniqueCount(uuid), WHERE newRelicAgent = 'AndroidAgent') as unique_count_uuid_android_agent_type,

filter(uniqueCount(uuid), WHERE newRelicAgent = 'iOSAgent') as unique_count_uuid_ios_agent_type,

filter(uniqueCount(uuid), WHERE newRelicAgent NOT IN ('iOSAgent', 'AndroidAgent')) as unique_count_uuid_not_android_ios_agent_type

FROM Mobile SINCE {} until {} """.format(SINCE,UNTIL),


#Devices sending network syslog data and count of log lines....., count(*) as log_lines_cnt 
"""
SELECT uniqueCount(device_name) as count_devices_sending_network_syslog_data, count(*) as count_log_lines_devices_sending_network_syslog_data from Log where plugin.type = 'ktranslate-syslog' SINCE {} until {} """.format(SINCE,UNTIL),


#Devices sending VPC flow log data and count of flows

"""
SELECT uniqueCount(vpc_id) as unique_count_vpc_flows_log_data_devices_send FROM Log_VPC_Flows, Log_VPC_Flows_AWS, Log_VPC_Flows_GCP WHERE instrumentation.name = 'vpc-flow-logs' SINCE {} until {} """.format(SINCE,UNTIL),


 #Devices sending network flow log data and count of flows

"""
SELECT uniqueCount(device_name) as count_network_flows_log_data_devices_send, count(*) as count_flows_devices_sending_network_flow_log_data from KFlow where provider='kentik-flow-device' SINCE {} until {} """.format(SINCE,UNTIL),

#Devices sending SNMP data and count of SNMP metrics received

"""
SELECT uniqueCount(device_name) as unique_count_devices_sending_snmp_data, count(*) as count_snmp_metrics_received FROM Metric where instrumentation.provider='kentik' AND provider != 'kentik-agent' SINCE {} until {} """.format(SINCE,UNTIL),

#Number of Prometheus metrics

"""SELECT count(*) as number_prometheus_metrics FROM Metric WHERE integrationName ='nri-prometheus' OR instrumentation.provider = 'prometheus' SINCE {} until {} """.format(SINCE,UNTIL),


#What is the average time to resolve issues?

"""FROM NrAiIncident SELECT sum(durationSeconds) / 60 AS 'average_time_resolve_issues', count(*) AS 'total_critical_issues_resolved' WHERE event = 'close' AND priority = 'critical' SINCE {} until {} """.format(SINCE,UNTIL),


#What is average number of open critical incidents per hour

"""FROM NrAiIncident SELECT rate(count(*), 1 hour) AS 'average_number_open_critical_incidents_per_hour' WHERE event = 'open' AND priority = 'critical'  SINCE {} until {} """.format(SINCE,UNTIL),


#What is the peak number of incidents open in any given hour?
"""SELECT max(open) as peak_number_incidents_open_any_given_hour FROM (FROM NrAiIncident SELECT count(*) AS 'open' WHERE event = 'open' AND priority = 'critical'  TIMESERIES 1 hour ) SINCE {} until {} """.format(SINCE,UNTIL),

"""SELECT uniqueCount(deploymentId) as total_count_deployments from Deployment since {} UNTIL {}""".format(SINCE,UNTIL),

"""from NrAuditEvent select count(targetId) as total_number_drop_rules where description like 'create_nrql_drop_rule%' and targetId not in (select targetId from NrAuditEvent where description like 'delete_nrql_drop_rule%' SINCE {start} UNTIL {end}  limit max) limit max SINCE {start} UNTIL {end}""".format(start=SINCE,end=UNTIL),

"""SELECT average(capacityEphemeralStorageBytes) or 0 as avg_kubernetes_capacity_ephemeralstoragebytes_nr_monitors FROM K8sNodeSample SINCE {start} UNTIL {end}""".format(start=SINCE,end=UNTIL),

"""SELECT average(fsCapacityBytes) or 0 as avg_k8s_persistent_volume_fscapacity_bytes_nr_monitors FROM K8sVolumeSample SINCE {start} UNTIL {end}""".format(start=SINCE,end=UNTIL),

"""SELECT average(diskTotalBytes) or 0 as avg_infrastructure_disktotalbytes_nr_monitors FROM SystemSample SINCE {start} UNTIL {end}""".format(start=SINCE,end=UNTIL)
  
  ]
  return nr_query_list



# list to store the new queries that are added after the table is built, this is needed to maintain the column order of the newly added columns in the table
def get_added_query_list(SINCE,UNTIL):
  added_query_list = [
    
  ]
  return added_query_list



def get_account_data(account_id,nr_query,retry_num=3):
    """
    Fetch account data from NR1 API.

    Args:
        account_id (str): The unique identifier of the account.
        nr_query (str): The New Relic query to retrieve data for the account.
        retry_num (int, optional): The number of retries in case of a failed request like http 500 error.

    Returns:
        dict: A dictionary containing the retrieved account data
    """
    try:
      res = requests.post(API_ENDPOINT, json={
                  "query": nr_query,
                  "account": account_id,
                  "restrictions": {
                      "isNewRelicAdmin": True,
                      },

                  "metadata": {"source": "nr1"}})
      if res.status_code == 200:
          print(f"successfully ingesting data for {account_id}")
          return res.json()
    
      elif  res.status_code == 429:
        if retry_num>0:
          retry_after = int(res.headers.get('Retry-After', 5))
          print(f"Rate limit exceeded for {account_id}. Retrying after {retry_after} seconds...")
          
          time.sleep(retry_after)
          # sleep for 5s, then retry 
          return get_account_data(account_id,nr_query,retry_num-1)
        else:
          print("error:maxium retry reaches for rate limiting")
          # stop it if it reaches max retries
          return None
      elif res.status_code == 500:
        if retry_num>0:
          print(f"500 Internal Server Error occurred for account {account_id}. Skipping to next account.")
          
          time.sleep(5)
          
          return get_account_data(account_id,nr_query,retry_num-1)
        else:
          print("error maxium retries reaches, server error persists")
          
          return None
      else:
          res.raise_for_status()
          #break
    except requests.exceptions.RequestException as e:
          print(f"Error occurred: {e}")
          print(res.status_code)
          
    except requests.Timeout:
          print("Error: the request timed out")
          
    except ValueError:
          print("Error: failed to parse the data to JSON")
          
    except requests.ConnectionError:
          print("Error: failed to eastablish a connection to the server")
          
        



# for daily ingestion 
def create_account_df_daily(account_id,nr_query):
  """
    Create a daily DataFrame for account data.

    Args:
        account_id (str): The unique identifier of the account.
        nr_query (str): The NRDB query to retrieve data for the account.

    Returns:
         A DataFrame containing daily account data.

    Example:
        account_data = create_account_df_daily('123456', 'SELECT * FROM ...')

  """
  res = get_account_data(account_id,nr_query)
  date_format = '%Y-%m-%dT%H:%M:%SZ'
  end_date = str(datetime.strptime(res['metadata']['beginTime'], date_format).date())
  metric_values = []

  metric_name_list = [data['alias'] for data in res['metadata']['contents']]

  
  for data in res['results']:
    for _, v in data.items():
      metric_values.append(v)
  #if v is not None else 0
  data_map = {k:v for k, v in zip(metric_name_list,metric_values)}
  
  data_map['date'] = end_date
  
  data_map['account_id'] = account_id
  
  return pd.DataFrame([data_map]) 
  # keep this in case we may need to deal with nested json data too
  # else:
  #   all_dict = {}
  #   #list_data = []
  #   for i in res['facets']:
  #     #print({i['name']:i['results']})
  #     sub_dict = {}
  #     for j in i['results']:
  #       #print(j['latest'])
  #       #print (j['average'])
  #       #print(j['max'])
  #       sub_dict.update({k:v for k,v in j.items()})
  #     all_dict[i['name']]=sub_dict
  #     #list_data.append((i['name'],sub_dict))
  #   df = pd.DataFrame(columns=['date','account_id','entity_agg_json_content'])
  #   #print(all_dict)
  #   df.loc[0,'date'] = start_date
  #   df.loc[0,'account_id'] = account_id
  #   df.loc[0,'entity_agg_json_content']= json.dumps(all_dict) if len(all_dict)>0 else None 
   
  #   return df




def get_account_id():
  """
  Function to fetch subaccount_ids from snowflake db
  
  """
  options = get_connection_credential()
  spark= SparkSession.builder.getOrCreate() 

  query = """
  select distinct account_id from transformed_staging.common.acct_hierarchy_d_tv
  where account_sales_channel = 'SALES_LED' and effective_end_date = '9999-12-31' and account_status != 'cancelled'
  """
  df = spark.read \
      .format("snowflake") \
      .options(**options) \
      .option("query", query) \
      .load()
  account_list = [ str(row['ACCOUNT_ID']) for row in df.collect()] 
  return account_list



def generate_merge_df_all(nr_query_list,account_id_list_slice,account_id_list=None):
  #start_df = pd.DataFrame()
  """
    Generate and merge DataFrames for multiple NRDB queries and account IDs.

    Args:
        nr_query_list (list): A list of NRDB queries to retrieve data for each account.
        account_id_list_slice (list): A subset of account IDs to process. This is needed to accept the output from the break_list function 
        which breaks up all the all account IDs in chunks to avoid OOM issue in Spark's drive node.  
        account_id_list (list, optional): A list of all account IDs (default is None). Used it for backfill data. 

    Returns:
        A merged DataFrame containing data from multiple queries for the specified accounts.

  """
  if account_id_list ==None:
      account_ids = account_id_list_slice
  else:
      account_ids = account_id_list
  start_df = pd.DataFrame({'date':yesterday,'account_id':account_ids})
  for query in nr_query_list:
    result = []
    print('begin to process query {}'.format(query))
    #for account_id in df1['ACCOUNT_ID'].unique()[:20]:
    for account_id in account_ids:
      df = create_account_df_daily(account_id,query)
      result.append(df)
    df_all_accounts = pd.concat(result)
    if len(start_df)==0:
      start_df = df_all_accounts
    else:
      start_df = pd.merge(start_df,df_all_accounts,how='left',on=['date','account_id'])
  return start_df





import math
def break_list(l,n):
  """
    Function to break the list of account IDs into chunks, in order to avoid OOM issue of the Spark driver. 

    Args:
        l (list): The input list(account IDs).
        n (int): How many chunks we want to divide the list into.

    Returns:
        list of lists: A list containing sublists

    Example:
        original_list = [1,20,302,4,30,5,60,89,20,112,20,20,20,40]
        sublists = break_list(original_list, 3)
        The output is [[1, 20, 302], [4, 30, 5], [60, 89, 20], [112, 20, 20], [20, 40]]
  """
  list_len = len(l)
  list_len_avg = math.ceil(list_len/n)
  total_list = []
  for i in range(0,list_len,list_len_avg):
    #for j in range(i,list_len,list_len_avg):
    if i + list_len_avg > list_len:
      total_list.append(l[i:])
    else:
      total_list.append(l[i:i+list_len_avg])
  return total_list





def generate_merge_df_all_multi_threading(nr_query_list, account_id_list_slice, account_id_list_backfill=None):
    """
    Similar to the generate_merge_df_all function, except that this function uses threading to process multiple account IDs 
    concurrently, which drastically improve the performance. 

    Function to generate and merge DataFrames for multiple NRDB queries and account IDs.

    Args:
        nr_query_list (list): A list of NRDB queries to retrieve data for each account.
        account_id_list_slice (list): A subset of account IDs to process. This is needed to accept the output from the break_list function 
        which breaks up all the all account IDs in chunks to avoid OOM issue in Spark's drive node.  
        account_id_list_backfill (list, optional): A list of all account IDs (default is None). Used it for backfilling data. 

    Returns:
        A merged DataFrame containing data from multiple queries for the specified accounts.

    """
    if account_id_list_backfill ==None:
      account_ids = account_id_list_slice
    else:
      account_ids = account_id_list_backfill  
    
    start_df = pd.DataFrame({'date':yesterday,'account_id':account_ids})
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(create_account_df_daily, account_id, query): (account_id, query)
                   for account_id in account_ids for query in nr_query_list}
        df_result = {}
        for future in concurrent.futures.as_completed(futures):
            account_id, query = futures[future]
            #print(query)
            #print(future.result())
            try:
                df = future.result()
                if query not in df_result.keys():
                  df_result[query]= [df]
                else:
                  df_result[query].append(df)
                
            except Exception as exc:
                print(f"Error occurred for account {account_id} and query {query}: {exc}")
                logging.error(f"Error occurred for account {account_id} and query {query}: {exc}")
        for _, v in df_result.items():
          df = pd.concat(v)
          if len(start_df)==0:
            start_df = df
          else:
            start_df = pd.merge(start_df, df, how='left', on=['date', 'account_id'])
          
    return start_df



def write_df_db(nr_query_list,account_id_list_slice, multithreading_on=True, account_id_list_backfill=None):
  """
    Function to write the dataFrames to the snowflake database 

    Args:
        nr_query_list (list): A list of NRDB queries to retrieve data for each account.
        account_id_list_slice (list): A subset of account IDs to process. This is needed to accept the output from the break_list function 
        which breaks up all the all account IDs in chunks to avoid OOM issue in Spark's drive node.  
        multithreading_on (bool, optional): Enable multithreading for data retrieval.
        account_id_list_backfill (list, optional): This is to use for backfilling data.

    """
  options = get_connection_credential()
  spark= SparkSession.builder.getOrCreate()
  #nr_query_list = get_nr_query_list(SINCE,UNTIL)
  if account_id_list_backfill ==None:
    if multithreading_on:
      df = generate_merge_df_all_multi_threading(nr_query_list,account_id_list_slice)
    else: 
      df = generate_merge_df_all(nr_query_list,account_id_list_slice)
      
  else:
    if multithreading_on:
      df = generate_merge_df_all_multi_threading(nr_query_list,[],account_id_list_backfill)
    else: 
      df = generate_merge_df_all(nr_query_list,[],account_id_list_backfill)

  # the start and end timestamp of the NRQL queries from which we pull the data using the API. create the columns for reference
  df['nrql_starting_timestamp'] = SINCE
  df['nrql_ending_timestamp'] = UNTIL
  df['ingestion_script_execution_timestamp'] = current_datetime
  df['dq_checks_pass'] = ""


  # this part is needed, in order to maintain the column order of the newly added columns
  added_columns = []
  for i in get_added_query_list(SINCE,UNTIL):
    added_columns.extend([m[0] for m in get_added_column_names_regex(i)])

  existing_columns = [i for i in df.columns if i not in added_columns] 
  existing_columns_ordered = sorted(existing_columns)
  all_columns = existing_columns_ordered + added_columns 
  df = df[all_columns]

  spark_df = spark.createDataFrame(df)
 
  table_name = target_table_name
  table_name_backup = target_backup_table_name
  for table in [table_name,table_name_backup]:
    spark_df.write \
    .format("snowflake") \
    .options(**options) \
    .option("dbtable", table) \
    .mode("append") \
    .save()



def backfill_data_date_range(start_date,end_date, account_id_list_slice, start_date_hours='00:00:00',end_date_hours='00:00:00',account_id_list=None):
  
  """
    Function to backfill data for a specified date range and account IDs.

    Args:
        start_date (str): The start date for data backfill in the format 'YYYY-MM-DD'.
        end_date (str): The end date for data backfill in the format 'YYYY-MM-DD'.
        account_id_list_slice (list): A sublist of account IDs to process. The reason for having it is the same as in the previous functions.
        start_date_hours (str, optional): The start time of the date range in the format 'HH:MM:SS' (default is '10:00:00').
        end_date_hours (str, optional): The end time of the date range in the format 'HH:MM:SS' (default is '10:00:00').
        account_id_list (list, optional): A list of specific account IDs for backfilling (default is None). When it is None and 
        a date range is provided, it will backfill all the account IDs. 
  """
  options = get_connection_credential()
  spark= SparkSession.builder.getOrCreate() 

  dr_start_date = start_date +' '+start_date_hours
  dr_end_date = end_date + ' '+end_date_hours
  dates = pd.date_range(dr_start_date,dr_end_date).strftime('%Y-%m-%d %H:%M:%S')
  # print(dr_start_date)
  # print(dr_end_date)
  dates = [ f"\'{date + ' UTC'}\'"  for date in dates]
  date_pair = [(dates[i],dates[i+1]) for i in range(len(dates)) if i+1<len(dates)]
  nr_query_list_date_range = [get_nr_query_list(i,j) for i, j in date_pair]
  
  if account_id_list == None: 
  # delete the data for the date range from the underlying table
    for table_name in [target_table_name,target_backup_table_name]:
      query = f"""delete from {target_table_name} where date between '{start_date}' and '{end_date}'""".\
        format(start_date=f"\'{start_date}\'",end_date=f"\'{end_date}\'",target_table_name={table_name})
      spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(options,query)

      
    for query_list in nr_query_list_date_range:
      write_df_db(query_list,account_id_list_slice,multithreading_on=True)

  #only backfill some hand picked ids for some date range
  else:
    for table_name in [target_table_name,target_backup_table_name]:
      account_id_list_str = ",".join([i for i in map(lambda x:str(x),account_id_list)])
      query = f"""delete from {table_name} where date between '{start_date}' and '{end_date}' and account_id in ({account_id_list_str})""".\
        format(start_date={start_date},end_date={end_date},account_id_list_str=f"{account_id_list_str}",table_name={table_name})
      
      #f"delete from sales_iq_fct_daily_2 where date between '2023-07-01' and '2023-07-03' and account_id in (3931030,1511808,990276,3290204)"
      spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(options,query)
    print("delete data for the accounts")
    
    for query_list in nr_query_list_date_range:
      write_df_db(query_list,[],multithreading_on=True,account_id_list_backfill=account_id_list)
    



def get_added_column_names_regex(nrql_query):
  """
  Function to extract column name from a NRDB query using regex
  """
  import re
  pattern = r'(?:\s+as\s+)(?P<renamed>\w+)|(?P<renamed_without_as>\s+\w+\,)|(?:^\s*\w+\.)' \
            r'(?P<original>\w+)(?:\,|from)'

  matches = re.findall(pattern, nrql_query, re.IGNORECASE)
  return matches



def add_new_metric_column(nrql_query):
  
  """
  Function to add new columns to an existing table. The input parameter is a NRDB query string. This can be done in snowflake directly as well. 
  """
  options = get_connection_credential()

  matches = get_added_column_names_regex(nrql_query)
  column_names = [m[0] + " NUMBER(38,0)" for m in matches]

  # query to add new metric columns to the table in snowflake
  for table_name in [target_table_name, target_backup_table_name]:
    alter_table_query = """
    ALTER TABLE {table_name}
    ADD COLUMN {column_names}""".format(column_names=",".join(column_names),table_name=table_name)
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(options,alter_table_query)
    print(alter_table_query)




def dedup_backfill_account_data():

  """
  Function to check:
  if there are any duplicated records in a daily run. If there are, remove the duplicates and backfill the data
  if there are any accounts that are missing from the daily run. If there are, backfill those missing accounts 
  This function runs within the get_dq_checks function on a daily basis to ensure the dq issues will be fixed automatically after they are detected 
  This function can also be used to backfill daily run data from where it is stopped, in the case of a system failure or any issue with the databricks cluster that shuts down the node unexpectedly
  """
  options = get_connection_credential()
  spark= SparkSession.builder.getOrCreate() 

  dedup_query = """
  select ACCOUNT_ID,count(*) cnt from {table_name}
  where date = '{date}'
  group by all 
  having count(*)>1
  """.format(date=yesterday,table_name=target_table_name)
  df_dup = spark.read \
      .format("snowflake") \
      .options(**options) \
      .option("query", dedup_query) \
      .load()
  dup_account_id = [str(row['account_id']) for row in df_dup.selectExpr('account_id').collect()]


  # query to check if there is any missing account ids that are not captured in the run, if so, run the backfill function to add them 
  check_account_id_query = """
  with all_account_ids as (
  select distinct account_id from transformed_staging.common.acct_hierarchy_d_tv
    where account_sales_channel = 'SALES_LED' and effective_end_date = '9999-12-31' and account_status != 'cancelled')
    select account_id from all_account_ids where account_id not in (select ACCOUNT_ID from {table_name}
  where date = '{date}')
  """.format(date=yesterday,table_name=target_table_name)
  missing_accounts = spark.read \
      .format("snowflake") \
      .options(**options) \
      .option("query", check_account_id_query) \
      .load()
  miss_account_id = [str(row['account_id']) for row in missing_accounts.selectExpr('account_id').collect()]
  combined_list = list(set(dup_account_id + miss_account_id))
  if len(combined_list)>0:
    list_subsets = break_list(combined_list,10)
    for l in list_subsets:
      backfill_data_date_range(start_date=yesterday,end_date=current_date,account_id_list_slice=[],account_id_list=l)
      gc.collect()



def check_has_data():
  """
  function to check if the target table has data from the current run 
  in the case of pipeline failure and retry kicks in, only backfill the data from where it gets stopped
  """
  options = get_connection_credential()
  spark= SparkSession.builder.getOrCreate() 
  query = """
  select * from {table_name}
  where date = '{date}'
  """.format(date=yesterday,table_name=target_table_name)
  df = spark.read \
      .format("snowflake") \
      .options(**options) \
      .option("query",query) \
      .load()
  return df.count()



def get_dq_checks(execution_time = 0, sample_rate=0.025):
  import random
  """
    Retrieve and store data quality checks for account data.

    Args:
        execution_time (int, optional): Execution time of the daily ingestion (default is 0). Store it for reference and future process analysis and optimization.
        sample_rate (float, optional): Sampling rate for data quality checks (default is 0.025).

  """
  options = get_connection_credential()
  spark= SparkSession.builder.getOrCreate() 

  query = """
  select * from {table_name}
  where date = '{date}'
  """.format(date=yesterday,table_name=target_table_name)
  df_dq = spark.read \
      .format("snowflake") \
      .options(**options) \
      .option("query", query) \
      .load()

  schema = StructType([
      StructField("date", StringType(), True),
      StructField("duplicate_check", BooleanType(), True),
      StructField("consistency_check", BooleanType(), True),
      StructField("null_checks",BooleanType(),True),
      StructField("null_check_map",MapType(StringType(), IntegerType()),True),
      StructField("row_cnt", IntegerType(), True),
      StructField("total_ingestion_time",StringType(),True),
  ])
  #check if the data contains duplicates in the account_id column
  dup_check_result = True if df_dq.groupBy("account_id").count().filter(col("count") > 1).first() == None else False 
  
  # if after the daily run, the data contains depulicates, execute the dedup_backfill function to resolve the duplicate issue for the day automatically
  if not dup_check_result:
    dedup_backfill_account_data()
    # run the check again after the fix, since dup_check_result will be stored in a table 
    dup_check_result = True if df_dq.groupBy("account_id").count().filter(col("count") > 1).first() == None else False


  # check if the data is consistent after making the same API calls at a later time 
  query_list = get_nr_query_list(SINCE,UNTIL)

  df_pd = df_dq.toPandas()
  df_sample = df_pd.sample(frac=sample_rate, random_state=10)
  random_account_id = df_sample['ACCOUNT_ID'].tolist()
  random_data = spark.createDataFrame(df_sample)
  random_data = random_data.drop(*['NRQL_ENDING_TIMESTAMP','INGESTION_SCRIPT_EXECUTION_TIMESTAMP','NRQL_STARTING_TIMESTAMP','DQ_CHECKS_PASS'])

  # randomly select 3 columns for consistency check
  cols = [i.upper() for i in random_data.columns if i not in ['AVG_KUBERNETES_CAPACITY_EPHEMERALSTORAGEBYTES_NR_MONITORS',
'AVG_K8S_PERSISTENT_VOLUME_FSCAPACITY_BYTES_NR_MONITORS','TOTAL_TRANSACTION_PER_DAY_APM',
'AVG_INFRASTRUCTURE_DISKTOTALBYTES_NR_MONITORS','ACCOUNT_ID']]
  random_cols=random.sample(cols,3) + ['ACCOUNT_ID']
  random_data_sorted = random_data[sorted(random_cols)].orderBy('ACCOUNT_ID')

  #random_account_id = [row['account_id'] for row in random_data.selectExpr('account_id').collect()]
  random_df = generate_merge_df_all_multi_threading(query_list,random_account_id)
  regen_df = spark.createDataFrame(random_df)
  regen_df_sorted = regen_df[sorted([i for i in map(lambda x:x.upper(),random_cols)])].orderBy('ACCOUNT_ID')

  consistency_check_result = True if (random_data_sorted.exceptAll(regen_df_sorted)).count()/random_data_sorted.count()<=0.015 else False 


  # check the numbers of nulls for each metric/column and store them in a map
  null_cnt_df = random_data.select([col(column).isNull().cast("int").alias(column) for column in random_data.columns])
  null_cnt_df_sum = null_cnt_df.selectExpr(*["sum({column}) as {column}".format(column=column) for column in null_cnt_df.columns])

  k_v_pair = []
  for column in null_cnt_df_sum.columns:
    k_v_pair.append(lit(column))
    k_v_pair.append(null_cnt_df_sum[column])

  map_column_content = null_cnt_df_sum.withColumn('null_check_map',create_map(k_v_pair)).select('null_check_map').collect()[0]['null_check_map']
  
  # check the number of row count for the day
  row_count = df_dq.count()
  expr_string = "+".join([column for column in null_cnt_df_sum.columns])
  total_null_cnt = null_cnt_df_sum.withColumn('total_null_cnt',expr(expr_string)).collect()[0]['total_null_cnt']

  # exclude the columns that are known to have a lot of nulls before calculating the total nulls of all the columns
  # the null counts of the columns that have a lot of nulls are tracked in the metric map column  
  expr_string_rm = "+".join([column for column in null_cnt_df_sum.columns if column not in ['AVG_KUBERNETES_CAPACITY_EPHEMERALSTORAGEBYTES_NR_MONITORS',
'AVG_K8S_PERSISTENT_VOLUME_FSCAPACITY_BYTES_NR_MONITORS','TOTAL_TRANSACTION_PER_DAY_APM'
'AVG_INFRASTRUCTURE_DISKTOTALBYTES_NR_MONITORS','ACCOUNT_ID']])
  total_null_cnt_rm = null_cnt_df_sum.withColumn('total_null_cnt_rm',expr(expr_string_rm)).collect()[0]['total_null_cnt_rm']

  null_check_result = True if total_null_cnt/(row_count*len(null_cnt_df_sum.columns))<=0.005 and total_null_cnt_rm <= 50 else False

  
  data = [(current_date,dup_check_result,consistency_check_result,null_check_result,map_column_content,row_count,str(execution_time))]
  spark_df = spark.createDataFrame(data,schema)
  table = "salesiq_daily_dq_checks_f"
  spark_df.write \
    .format("snowflake") \
    .options(**options) \
    .option("dbtable", table) \
    .mode("append") \
    .save()
  print(null_cnt_df_sum.columns)
  



  # update the main table with the dq checks result for the day 

  for table_name in [target_table_name, target_backup_table_name]:
    if null_check_result and dup_check_result and consistency_check_result:
      update_table_query = """
      update {table_name} 
      set dq_checks_pass = 'yes' where date = '{date}'
      """.format(table_name=table_name,date=yesterday)
    else:
      update_table_query = """
      update {table_name} 
      set dq_checks_pass = 'no' where date = '{date}'
      """.format(table_name=table_name,date=yesterday)

    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(options,update_table_query)
  
  



def main(is_backfill=False,num_chunks=50,backfill_ids=None,backfill_start_date=None,backfill_end_date=None):
  """
    Main function to retrieve, backfill, and perform data quality checks for account data.
    If not set to backfill, it first checks if the target table has data from the current run 
    in the case of pipeline failure and retry kicks in in airflow, only backfill the data from where it gets stopped
    Args:
        is_backfill (bool, optional): Set to True for backfilling data (default is False).
        For backfilling, if no backfill_ids is provided, backfill all the account_ids, on the other hand, if backfill_ids is not None, backfill only the provided account IDs
        num_chunks (int, optional): The number of chunks to divide account IDs (avoid OOM issue in Spark, the general cluster is small).
        backfill_ids (list, optional): A list of specific account IDs for backfilling (default is None).
        backfill_start_date (str, optional): The start date for data backfill in the format 'YYYY-MM-DD' (default is None).
        backfill_end_date (str, optional): The end date for data backfill in the format 'YYYY-MM-DD' (default is None).
  """

  query_list = get_nr_query_list(SINCE,UNTIL)

  account_ids = get_account_id()
  account_ids_breakdown_list = break_list(account_ids,num_chunks)
  has_data = check_has_data()

  if is_backfill:
    if backfill_ids ==None:
      for l in account_ids_breakdown_list:
        backfill_data_date_range(backfill_start_date,backfill_end_date,l)
    else:
      backfill_data_date_range(backfill_start_date,backfill_end_date,[],account_id_list=backfill_ids)
  
  elif has_data>0:
    print('run from where it stopped')
    start_time = datetime.now()
    dedup_backfill_account_data()
    end_time = datetime.now()
    execution_time = end_time - start_time
    get_dq_checks(execution_time=execution_time)
  
  elif not is_backfill:
    start_time = datetime.now()
    for l in account_ids_breakdown_list:
      write_df_db(query_list,l,multithreading_on=True)
      gc.collect()
    end_time = datetime.now()
    execution_time = end_time - start_time
    get_dq_checks(execution_time=execution_time)

  






main()


#backfill_data_date_range(start_date="2023-08-05",end_date="2023-08-07",start_date_hours='06:00:00',end_date_hours='06:00:00')


# test backfill some account_ids for some date range
#main(is_backfill=True, backfill_start_date='2023-08-23',backfill_end_date='2023-08-24',backfill_ids=[3812981,3034335])
#backfill_data_date_range(start_date='2023-08-23',end_date='2023-08-24',account_id_list_slice=[],account_id_list=[3812981,3034335])
