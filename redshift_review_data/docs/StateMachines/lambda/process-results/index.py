import json
import io
import os
import csv
import boto3
 
bucket = os.environ["BUCKET_NAME"]
manifest_prefix = os.environ["MANIFEST_PREFIX"]

s3 = boto3.client("s3")

def manifest_list():
  try:
      list = s3.list_objects_v2(Bucket=bucket, Prefix=manifest_prefix)
      manifest_list = []
      for obj in list.get('Contents', []):
          if "manifest" in obj["Key"]:
              manifest_list.append(obj)
      return manifest_list
  except Exception as e:
    print (repr(e))
    return []
    

def lambda_handler(event, context):
  print(event)
  bucket = event.get("S3BucketName")
  workflow = json.loads(event.get("input").get("Output")).get("QueryList")
  #status = event.get("input").get("Status")
  
  # check how many workflow.manifests are 
  lenmf = len(manifest_list())
  print(lenmf)
  if lenmf >= 3 or lenmf == 0:
    endstate = True
  else:
    endstate = False
    
  next_wf = {}
  for step in workflow:
    if workflow[step]["Status"] == "SUCCEEDED":
      pass
    else:
      next_wf[step] = workflow[step]
      query = workflow[step]
  
  if next_wf != {}:   
    if endstate == False:
      return { 
          'statusCode': 200,
          'body': {
              "QueryList": next_wf,
              "Query": query,
              "S3BucketName": bucket
          },
          'rerun': True,
          'succedded': False
      }
    else:
      report={}
      for step in next_wf:
        report[step] = { "Status" : next_wf[step]["Status"]  }
      return { 
            'statusCode': 400,
            'error': {
              'error_queries': report,
              'error_message' : "There is a persistant error in executing the listed queries. Please contact the TAM team",
              },
            'rerun': False,
            'succedded': False
        }  
  else:
    return { 
          'statusCode': 200,
          'rerun': False,
          'succedded': True
      }  