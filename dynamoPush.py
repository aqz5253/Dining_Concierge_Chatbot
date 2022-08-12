from requests_aws4auth import AWS4Auth
import boto3
import requests
import datetime
import json
from decimal import *
from urllib.parse import quote
from requests.auth import HTTPDigestAuth


API_KEY = ""

url = 'https://api.yelp.com/v3/businesses/search'

dynamodb = boto3.client('dynamodb',region_name="us-east-1",aws_access_key_id="",aws_secret_access_key="")

headers = {'Authorization' : 'Bearer {}'.format(API_KEY)}

c = ('chinese', 'japanese', 'tradamerican', 'mexican', 'italian')

bid = set()


def get(cat):
	bus = []
	for i in range(0, 1000, 50):
		if cat == 'japanese' and i >= 950:
			break
		params = {
			'location' : 'New York City',
			'categories' : cat,
			'limit': 50,
			'offset': i}

		res = requests.get(url=url, params=params, headers=headers)
		d = res.json()
		print(d.keys())
		if d.get('error') is not None:
			print(d)
		business = d['businesses']

		for b in business:
			if b['id'] in bid:
				continue
			else:
				bus.append(b)
				bid.add(b['id'])
	return bus


for cat in c:
	data = dict()
	data[cat] = get(cat)
	with open(cat, mode = 'w') as f:
		json.dump(data, f)

def buildItem(data, cuisine):
    item = {}

    if 'id' in data:
        item['business_id'] = {'S': data['id']}
    if 'name' in data:
        item['name'] = {'S': data['name']}
    if 'rating' in data:
        item['rating'] = {'S': str(data['rating'])}
    if 'coordinates' in data:
        item['location'] = {'SS': [str(data['coordinates']['latitude']), str(data['coordinates']['longitude'])]}
    if 'price' in data:
        item['price'] = {'S': data['price']}
    if 'location' in data and 'display_address' in data['location']:
        item['address'] = {'S': ", ".join(row for row in data['location']['display_address'])}
    if 'phone' in data:
        item['phone'] = {'S': data['phone']}
    item['cuisine'] = {'S': cuisine}
    item['insertedAtTimestamp'] = {'S': str(datetime.datetime.now())}

    return item

failCount, passCount = 0, 0
for cname in ('chinese', 'italian', 'japanese','mexican', 'tradamerican'):
    with open(cname, 'r') as f:
        data = json.load(f)[cname]

    for i, business in enumerate(data):
        if not business['is_closed']:
            item = buildItem(business, cname)
            if i == 0 or i == 1:
                print(item)
            try:
                dynamodb.put_item(TableName='yelp-restaurants', Item=item)
                passCount += 1
            except Exception as e:
                print("Failed" + str(e))
                failCount += 1
        print("Processed: {} of {}".format(i, cname))
