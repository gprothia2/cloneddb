# Use this script to export the data in Dynamod DB Table to S3 bucket

import boto3

# Set the variables for bucket name, credentials, source account,dynamodb table that needs to be exported and prefix name where 
exported data will be loaded

bucket_name = <bucket_name>
prefix_name = << The Amazon S3 bucket prefix to use as path of the exported snapshot >>
ACCESS_KEY = <Source Account Access Key>
SECRET_KEY = <Source Account Secret Key>
source_account = <Source Account Number>
source_ddb_table = <<  Name of the Dynamo DB table that  needs to be exported >>

# Export Dynamo Db Table
ddb = boto3.client('dynamodb', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
table_arn= f'arn:aws:dynamodb:us-east-1:{source_account}:table/{source_ddb_table}'
ddb.export_table_to_point_in_time(TableArn=table_arn,
    S3Bucket=bucket_name,
    S3Prefix=prefix_name,
    ExportFormat='DYNAMODB_JSON')
