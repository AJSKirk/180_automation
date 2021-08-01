import requests
import urllib.request
import json
import os
import time

MONDAY_KEY = open('{}/monday_key.txt'.format(os.path.dirname(__file__)), 'r').read().strip()
MONDAY_URL = "https://api.monday.com/v2"


def query(query_string):
    body = {'query': query_string}
    headers = {'Authorization': MONDAY_KEY}

    r = requests.post(MONDAY_URL, json=body, headers=headers)
    response = r.json()
    if query_string.startswith('query') and 'data' not in response.keys():
        print("Query failed - Sleeping 60s")
        time.sleep(60)
        return query(query_string)
    else:
        return response


def check_wait(query):
    complexity_query = "query {{complexity {{after reset}}"


def query_legacy(query_string):
    """Monday API queries using only the Standard Library - Useful for quick Lambda jobs"""
    body = {'query': query_string}

    req = urllib.request.Request(MONDAY_URL)
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    req.add_header('Authorization', MONDAY_KEY)

    body_bytes = json.dumps(body).encode('utf-8')
    req.add_header('Content-Length', len(body_bytes))

    response = urllib.request.urlopen(req, body_bytes)

    return json.loads(response.read().decode('utf-8'))


def get_column_value(item_data, title, field='text'):
    return next(col[field] for col in item_data['column_values'] if col['title'] == title)
