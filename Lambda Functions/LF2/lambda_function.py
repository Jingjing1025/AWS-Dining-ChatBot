import boto3
import json
import requests
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
import random

region = 'us-east-1' # For example, us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = '' # For example, search-mydomain-id.us-west-1.es.amazonaws.com
index = 'restaurants'
url = 'https://' + host + '/' + index + '/_search'

def sendSNS(phone_number, message):
    # Create an SNS client
    sns = boto3.client('sns')
    
    # Publish a simple message to the specified SNS topic
    response = sns.publish(
        PhoneNumber='+1'+phone_number,
        Message=message,
        Subject='AWS SNS test',
        MessageStructure='string'
    )
    
    # Print out the response
    print(response)

def queryFromDB(bizID):
    client = boto3.resource('dynamodb',
                      region_name=region,
                      aws_access_key_id='',
                      aws_secret_access_key='')
    table = client.Table('yelp-restaurants')
    
    response = table.query(
    KeyConditionExpression=Key('Business ID').eq(bizID)
    )
    
    return (response['Items'][0]['Name'], response['Items'][0]['Address'])

# Lambda execution starts here
def lambda_handler(event, context):

    # Put the user query into the query DSL for more accurate search results.
    # Note that certain fields are boosted (^).
    for record in event['Records']:
       msg=record["body"]
       customerInfo = json.loads(msg)
       cuisine = customerInfo['Cuisine']
       location = customerInfo['Location']
       number_of_people = customerInfo['NumberOfPeople']
       dining_date = customerInfo['DiningDate']
       dining_time = customerInfo['DiningTime']
       phone_number = customerInfo['PhoneNumber']
       

    query = {
        "size": 25,
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        }
    }

    # ES 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    
    # print(json.dumps(query))

    # Make the signed HTTP request
    try:
        r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    except:
        print("failed connection")

    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }

    # Add the search results to the response
    response['body'] = r.text
    found_results = json.loads(r.text)

    bizIDs = []
    for hit in found_results['hits']['hits']:
        bizIDs.append(hit['_source']['Business ID'])
    
    rand_number1 = random.randint(0,8)
    rand_number2 = random.randint(9,16)
    rand_number3 = random.randint(17,24)
    rand_biz_id1 = bizIDs[rand_number1]
    rand_biz_id2 = bizIDs[rand_number2]
    rand_biz_id3 = bizIDs[rand_number3]
    name1, address1 = queryFromDB(rand_biz_id1)
    name2, address2 = queryFromDB(rand_biz_id2)
    name3, address3 = queryFromDB(rand_biz_id3)
    
    output = "Hello! Here are my " + cuisine + " restaurant suggestions for " + number_of_people + " people, for " + dining_date + " at " + dining_time + ": 1. " + name1 + ", located at " + address1 + ", 2. " + name2 + ", located at " + address2 + ", 3. " + name3 + ", located at " + address3 + ". Enjoy your meal!"
    
    sendSNS(phone_number, output)
    
    print(output)
        
    return response