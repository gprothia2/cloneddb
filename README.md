There are 4 python scripts in /src folder. These scripts automate the steps to copy dynamo db table from source account to target account.

source_account_bucket_creation.py - Run this script to create S3 bucket in source account. This bucket will be used for downloading the data from dynamo DB table. The script also creates a bucket policy to provide read/write access to the target account.

source_account_export_table.py - Run this script to start the export of dynamo db table to S3 bucket

Glue.py - Copy this script to create a glue job that reads the data from S3 bucket and imports it to Dynamo db table in target account

run_glue_parallel_jobs.py - Run this script to kick off single or multiple glue jobs to import the data from S3 bucket to Dynamo db table in target account. This script can be called in parallel mode by specifying parameter workers_total >1. Larger the value, more threads will be spawned in parallel.
