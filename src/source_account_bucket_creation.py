# Run this script to create the bucket in Source account where the exported data will be uploaded. This script will alos give permessions to target account to access the bucket

import boto3
import json

# Set the variables for bucket name, credentials to connect to source account, and the target account number

bucket_name = <bucket_name>
target_account = <target_account>
ACCESS_KEY = <Source Account Access Key>
SECRET_KEY = '<Source Account Secret Key>

# create a new S3 object
s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

# create a bucket
s3.delete_bucket(Bucket=bucket_name)
s3.create_bucket(Bucket=bucket_name)

# define the bucket policy to grant bucket access to target account
bucket_policy = {
    'Version': '2012-10-17',
    'Statement': [{
        'Sid': 'AddPerm',
        'Effect': 'Allow',
        'Principal':{"AWS": f"arn:aws:iam::{target_account}:root"},
        'Action': 's3:*',
        'Resource': [f'arn:aws:s3:::{bucket_name}/*',f'arn:aws:s3:::{bucket_name}']
    }]
}
bucket_policy = json.dumps(bucket_policy)

# Set the new policy
s3.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)
