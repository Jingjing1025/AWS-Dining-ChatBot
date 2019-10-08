from yelpapi import YelpAPI
import argparse
from pprint import pprint
import requests
import json
import boto3
from decimal import *

yelp_api = YelpAPI('yelp_api_key')

dict_list = []
visited = []
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
			bizID = response['businesses'][i]['id']
			if bizID not in visited:
				visited.append(bizID)
				dict_list.append(response['businesses'][i])

dict_es = []

for elem in dict_list:
	cuisine = elem['categories'][0]['alias']
	Index = { 'index' : {
		'_index': 'restaurants', 
		'_type' : 'Restaurant'
		}
	}
	Item = {
		'Business ID': elem['id'],
		'Cuisine': cuisine
	}
	dict_es.append(Index)
	dict_es.append(Item)
	
	with open('yelp_data_es.json', 'a') as fout:
		json.dump(Index, fout)
		fout.write('\n')
		json.dump(Item, fout)
		fout.write('\n')

print("PutItem succeeded")
