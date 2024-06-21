import json
import io
import csv
import boto3
import os

s3 = boto3.client('s3')

def create_csv(json_result):
    column_metadata = json_result['ColumnMetadata']
    column_names = [column['Name'] for column in column_metadata]
    records = json_result['Records']
    # Write the CSV data to an in-memory buffer
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(column_names)
    if  records != []:
        for record in records:
            row = []
            for value in record:
                if 'StringValue' in value:
                    row.append(value['StringValue'])
                elif 'LongValue' in value:
                    row.append(str(value['LongValue']))
            writer.writerow(row)
    return csv_buffer.getvalue()


def lambda_handler(event, context):
    # TODO implement
    print(event)
    result_bucket = os.environ["BUCKET_NAME"]
    result_prefix = os.environ["RESULT_PREFIX"]
    try:
        list = s3.list_objects_v2(Bucket=result_bucket, Prefix=result_prefix)
        
        for obj in list.get('Contents', []):
            if "json" in obj["Key"]:
                #print(obj['Key'])#read all the results
                response = s3.get_object(Bucket=result_bucket, Key=obj['Key'])
                intm = json.loads(response['Body'].read().decode('utf-8'))
                csv_buf = create_csv(intm)
                result = s3.put_object(Bucket=result_bucket, Key=obj['Key'].replace("json","csv"), Body=csv_buf)
        return {
                'statusCode': 200,
                'message': 'CSV files created'
                }
    except Exception as e:
        print(repr(e))
        return {
                'statusCode': 400,
                'message': 'problem in processing CSV files '
                }