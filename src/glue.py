"""
This script reads the exported data using Athena, transforms the data using ANSI SQL functions,and output the data to pandas data frame
Data is read from pandas dataframe and loaded to Dynamo DB table
Multiple threads of this script can be run concurrently to parallelize importing of data.
Each instance of Glue job runs on split of the data determined by total no of workers and worker no passed as imput parameter. 
For example if total no of workers is 5, then five instances of the Glue job will be spawned, each instance processing subset of data independently in parallel.
  
INPUT
------
Exported data from Dynamo DB Table in JSON format stored in S3 bucket in Source account
      
OUTPUT  
------  
Dynamo DB table in Target Account where the data from Source account will be copied

            
JOB_PARAMETERS
--------------
workers_total: Number of workers 
worker_no: Specific worker instance
athena_table:  Athena table that is mapped to the exported data in the Source account
target_table: Dynamo Db table in Target account where the data needs to be copied
batch_size - this is for dividing the data into smaller chunks when writing the data to target dynamod db table
                
"""  

import json
import boto3
import sys
import logging
import awswrangler as wr
from awsglue.utils import getResolvedOptions

# set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
                

# Retrieve Job parameters
args = getResolvedOptions(sys.argv, ['workers_total','worker_no','athena_table','target_table','batch_size'])
workers_total= args['workers_total']
worker_no = args['worker_no']
athena_table = args['athena_table']
target_table =args['target_table']
batch_size =args['batch_size']


logger.info('Retrieved job parameters Successfully')


def write_to_dynamo(df,batch_size,table_name):
   try:
    # Dataframe is split into chunks of size specifed in batch_size
      
      chunk_size = int(df.shape[0] / int(batch_size))
        
    # Each chunk is iterated and written to target dynamo table using awswrangler
      for start in range(0, df.shape[0], chunk_size):
        df_subset = df.iloc[start:start + chunk_size]
        wr.dynamodb.put_df(df=df_subset,table_name=table_name)
            
   except:
      print("Error executing batch_writer:" + sys.exc_info()[0])

def main():
  try:

    # Sql to query the data using Athena, apply transformation like md5 to encrypt the PII data
    # Dataset is partioned using the mod function to return a subset of rows, allowing each thread to run on smaller subset for parallelization
    # SQL shown below is a sample - replace it to match with the schema of the table that you have created in Athena
    # To show how data can be transformed - we have applied Athena native function md5 to encrypt PII data in attribute2

    sql = """
          select rn,
                 Item.attribute_pk.S as attribute_pk,
		 Item.attribute1.S as attribute1,
		 Item.attribute2.S as attribute2,
                 to_hex(md5(to_utf8(attribute2 ))) as encrypted_attribute2
          from (select row_number() over() AS rn, * FROM {} )
          where mod(rn, {})={} - 1
          """.format(athena_table,workers_total,worker_no)
          
    logger.info(f'SQL is {sql}')


   # awswrangler function to execute the SQL and return results as a pandas dataframe
    df = wr.athena.read_sql_query(sql,database='default')
    
    logger.info('Read from Athena table Successfully')

    
   # Call function to pass pandas dataframe,size of the batch and Target Dyanmo Db Table where data will be written
    write_to_dynamo(df,batch_size,target_table)
    logger.info('Write to Dynamo DB  Successful')


  except:
    print("ERROR"+sys.exc_info()[0])


main()
