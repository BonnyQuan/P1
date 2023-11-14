import json
import boto3
import logging
from urllib.parse import parse_qs
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

REGION = boto3.session.Session().region_name  # Get the AWS region dynamically

HOST = 'search-photosearch-233nqmnt67mplwafimz3w6uyha.us-east-1.es.amazonaws.com'

def opensearch_query(tags):
    q = {'query': {'multi_match': {'query': tags}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
        http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)

    res = client.search(q)
    # Handle the response as needed
    _response = {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",       
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Methods": "GET,OPTIONS",},
        "body": json.dumps(res),
    }
    logger.debug(f'line 38 res: {res}')
    return res

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key, cred.secret_key, region, service, session_token=cred.token)

def lambda_handler(event, context):
    print('EVENT:-----> {}'.format(json.dumps(event)))
    
    headers = {"Content-Type": "application/json"}
    lex = boto3.client('lexv2-runtime')

    query = event['queryStringParameters']['q']

    lex_response = lex.recognize_text(
        botId="NL2QL88PE6",
        botAliasId="JYCUQXZCGO",
        localeId='en_US',
        sessionId="9560755949256",
        text=query)
        
    logger.debug("line 59 LEX RESPONSE - {}".format(json.dumps(lex_response)))
    
    slots = lex_response["interpretations"][0]["intent"]['slots']
    logger.debug(f'line 62 slots: {slots}')
    finalImages = []
    
    def process_interpreted_value(value):
        # Replace this with the action you want to perform on the interpretedValue
        logger.debug(f"Processing interpretedValue: {value}")
        print(f"Processing interpretedValue: {value}")
    logger.debug(f'line 69 slots: {slots}')
    for key, sub_dict in slots.items():
        if sub_dict and 'value' in sub_dict and sub_dict['value'] and 'interpretedValue' in sub_dict['value']:
            interpreted_value = sub_dict['value']['interpretedValue']
            process_interpreted_value(interpreted_value)
            _res = opensearch_query(interpreted_value)
            logger.debug(f'line 75, _res: {_res}')
            finalImages.append(_res)
    
    logger.debug('line 77, SLOTS - {}'.format(slots))
    
    finalBody = json.dumps({
        'event': event,
        'finalImages': finalImages
    })
    
    logger.debug(f'line 84, finalBody: {finalBody}');
    
    return {
      "statusCode": 200,
      "headers": {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
      },
      "body": finalBody
    }

    ### CONFIRMATION OF PIPELINE CONNECTION 2.0 ### 
