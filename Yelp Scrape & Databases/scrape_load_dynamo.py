from yelpapi import YelpAPI
import argparse
from pprint import pprint
import requests
import json
import boto3
from decimal import *

yelp_api = YelpAPI('yelp_api_key')

dict_list = []
terms = ['chinese', 'japanese', 'korean', 'thai', 'american', 'mexican', 'italian']
for term in terms:
    location='NY'
    response = yelp_api.search_query(term=term, location=location)
    total = response['total']
    num_query =0
    if total>1000:
        total=1000
    if total%50==0:
        num_query=int(total/50)
    else:
        num_query=int(total/50)+1

    for j in range(num_query):
        response = yelp_api.search_query(term=term, location=location, limit=50, offset=j*50)
        for i in range(len(response['businesses'])):
            dict_list.append(response['businesses'][i])


client = boto3.resource('dynamodb',
                      region_name='us-east-1',
                      aws_access_key_id='AKIAILM7VF356JT7DQRQ',
                      aws_secret_access_key='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')
table = client.Table('yelp-restaurants')


for elem in dict_list:
    Item = {
        'Business ID': elem['id'],
        'Name': elem['name'],
        'Address': ','.join(elem['location']['display_address']),
        'Coordinates': 'latitude: ' + str(elem['coordinates']['latitude']) + ', longtitude: ' + str(
            elem['coordinates']['longitude']),
        'Number of Reviews': elem['review_count'],
        'Rating': Decimal(elem['rating']),
        'Zip Code': str(elem['location']['zip_code'])
    }
    
    try:
        response = table.put_item(
            Item=Item
        )
    except Exception as err:
        print(Item, "Error", err)

print("PutItem succeeded")
