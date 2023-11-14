
import json
import base64
import logging
import boto3
import random
import string
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

region = 'us-east-1'
rekognition = boto3.client('rekognition', region_name=region)

# path creation in opensearch
INDEX = 'photosearch'

ES_HOST = 'search-photosearch-233nqmnt67mplwafimz3w6uyha.us-east-1.es.amazonaws.com'
REGION = 'us-east-1'

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
    
def push_to_opensearch(document, key):
    awsauth = get_awsauth(REGION, 'es')
    client = boto3.client('opensearch')
    es_client = OpenSearch(hosts=[{
        'host': ES_HOST,
        'port': 443
    }],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class = RequestsHttpConnection)
    
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 1
            }
        }    
    }
    
    res = es_client.index(index=INDEX, id=key, body=document)
    print('RESSSS', res)
    print('MORE RES', es_client.get(index=INDEX, id=key))
    

def lambda_handler(event, context):
    print(json.dumps(event))
    
    picture = event['body']
    picture = base64.b64decode(picture)
    object_key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
    object_key = object_key+".jpg"
    bucket = "photobuckets2"
    print('FILENAMEEEEEE')
    print(object_key)
    
    
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=object_key, Body=picture)
    
    # s3_event = event['Records'][0]['s3']
    # bucket = s3_event['bucket']['name'] # Change the 'name' to photosbucketass2
    # object_key = s3_event['object']['key']
    
    params = {
        'Image': {
            'S3Object': {
                'Bucket': bucket,
                'Name': object_key
            }
        },
        'MaxLabels': 10,
        'MinConfidence': 75
    }
    
    response = rekognition.detect_labels(**params)
    # for label in response['Labels']:
    #     print (label['Name'] + ' : ' + str(label['Confidence']))
    #     data['labels'].append(label['Name'])
    print('RESPONSE FROM REKOGNITION!')
    print(json.dumps(response))
    
    labels = ', '.join([label['Name'].lower() for label in response['Labels']])
    
    opensearch_document = {
        "objectKey": object_key,
        "bucket": bucket,
        "created_timestamp" :datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "labels": [labels]
    }
    
    print('OPENSEARCH DOCUMENT: ', opensearch_document)
    push_to_opensearch(opensearch_document, object_key)
    
    
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            "Control-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "*",
        },
        'body': json.dumps('IMAGES LABELS HAVE BEEN DETECTED YAYYY!')
    }
