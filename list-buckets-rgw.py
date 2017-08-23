#!/usr/bin/python
import boto3
import botocore
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
hostname = 'localhost'
endpoint_url = 'http://{}:{}'.format(hostname, 8000)
config = botocore.client.Config(s3={'addressing_style':'path','signature_version':'s3'})

session = boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
client = session.client('s3', endpoint_url=endpoint_url, config=config)

response = client.list_buckets()

buckets = [bucket['Name'] for bucket in response['Buckets']]

print("Bucket List: %s" % buckets)
