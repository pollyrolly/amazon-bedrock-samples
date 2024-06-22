import boto3
import traceback
import json
import os
import cfnresponse
def handler(event, context):
    source_bucket = os.environ['SOURCE_BUCKET']
    source_prefix = os.environ['SOURCE_PREFIX']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    destination_prefix = os.environ['DESTINATION_PREFIX']

    print(event)
    s3 = boto3.client('s3')
    response ={}
    if event['RequestType'] != 'Delete':
        try:
            #source_bucket = event['ResourceProperties'].get('S3Global')
            #source_prefix = event['ResourceProperties'].get('ScriptPath')+'/'
            #destination_bucket = event['ResourceProperties'].get('S3Local')
            #destination_prefix = event['ResourceProperties'].get('ScriptPath')+'/'

            response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

            for obj in response.get('Contents', []):
                copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
                new_key = obj['Key'].replace(source_prefix, destination_prefix, 1)
                response = s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=new_key)
        except:
            print(traceback.format_exc())
            cfnresponse.send(event, context, cfnresponse.FAILED, {})
            raise
    cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
