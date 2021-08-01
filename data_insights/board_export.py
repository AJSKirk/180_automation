import requests
from typing import Dict
import csv


MONDAY_KEY = open('monday_key.txt', 'r').read().strip()
MONDAY_URL = "https://api.monday.com/v2"

BOARD_TO_SCRAPE = '1.2 Branch Contact List - BRANCH TEMPLATE'


def query_monday_api(query):
    body = {'query': query}
    headers = {'Authorization': MONDAY_KEY}

    r = requests.post(MONDAY_URL, json=body, headers=headers)
    return r.json()


def get_board_id(board_name: str) -> int:
    """Returns the ID associated with a given board name"""
    query = "query {boards (limit: 999) {id name}}"
    boards = query_monday_api(query)['data']['boards']
    return next(board['id'] for board in boards if board['name'] == board_name)


def get_data_from_board(board_id: int):
    query = "query {{boards (ids: {}) {{items (limit: 9999) {{name group {{title}} column_values {{title text}}}}}}}}".format(board_id)
    return query_monday_api(query)['data']['boards'][0]['items']


def flatten_item_data(item_row) -> Dict[str, str]:
    flat = {'item_name': item_row['name'], 'group_name': item_row['group']['title']}
    flat.update({field['title']: field['text'] for field in item_row['column_values']})
    return flat


def write_board_to_csv(board_name: str, csv_name=None):
    if csv_name is None: csv_name = '{}.csv'.format(board_name)
    csv_data = [flatten_item_data(row) for row in get_data_from_board(get_board_id(board_name))]
    with open(csv_name, 'w') as f:
        dict_writer = csv.DictWriter(f, csv_data[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(csv_data)


if __name__ == '__main__':
    write_board_to_csv(BOARD_TO_SCRAPE)
