# Run this script from Target Account to import the data to the Dynamo DB table
import boto3

#Provide Credentials to connect to the Target Account
ACCESS_KEY = <Target Account Access Key>
SECRET_KEY = <Target Account Secret Key>

# Get a glue client
glue = boto3.client('glue', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


#Input parameters
#workers_total: Number of workers. Controls how many parallel threads of the job will be invoked. Adjust the value as per data volumes and how fast you would like the data to be imported.
#athena_table:  Athena table that is mapped to the exported data loaded in S3 in the Source account
#target_table: Dynamo Db table in Target account where the data needs to be copied
#batch_size - this is for dividing the data into smaller chunks when writing the data to target dynamod db table

job_name = <job_name>
workers_total= <workers_total>
athena_table = <athena_table>
target_table = <target_table>
batch_size=  <batch_size>

#Invoke multiple instance of Glue  job to split the dataset and import in parallel
for worker_no in range(int(workers_total)):
  glue.start_job_run(
               JobName = job_name,
               Arguments = {
                 '--workers_total':   str(workers_total),
                 '--worker_no':  str(worker_no),
                 '--athena_table':  athena_table,
                 '--target_table': target_table,
                 '--batch_size': str(batch_size)
               } )
