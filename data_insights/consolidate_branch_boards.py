import sys
sys.path.append('..')
from utils import monday
import board_export
import re
from collections import namedtuple
from typing import Iterable
import click
import csv
import time


BoardLocation = namedtuple('BoardLocation', ['id', 'name', 'workspace'])


def get_boards(branch_pattern, board_pattern):
    """Takes a regex pattern that the branch name must match, and a regex pattern that the board name must match"""
    query = "query {boards (limit: 9999) {name id workspace {name}}}"
    x = monday.query(query)
    boards = monday.query(query)['data']['boards']

    branch_boards = [board for board in boards
                     if board['workspace'] is not None and re.match(branch_pattern, board['workspace']['name'])]
    matching_boards = [board for board in branch_boards if re.match(board_pattern, board['name'])]

    return [BoardLocation(board['id'], board['name'], board['workspace']['name']) for board in matching_boards]


def get_board_with_context(board_loc: BoardLocation):
    for row in board_export.get_data_from_board(board_loc.id):
        context_row = {'board_name': board_loc.name, 'workspace': board_loc.workspace}
        context_row.update(board_export.flatten_item_data(row))
        yield context_row


def concat_boards(board_locs: Iterable[BoardLocation]):
    for i, board_loc in enumerate(board_locs, 1):
        print("Extracting Board {} of {}".format(i, len(board_locs)))
        yield from get_board_with_context(board_loc)


@click.command()
@click.argument('branch_pattern')
@click.argument('board_pattern')
@click.option('--csv_name', default=None)
def concat_boards_to_csv(branch_pattern, board_pattern, csv_name=None):
    if csv_name is None: csv_name = '{}_{}.csv'.format(branch_pattern, board_pattern)
    locs = get_boards(branch_pattern, board_pattern)
    data = list(concat_boards(locs))
    with open(csv_name, 'w') as f:
        dw = csv.DictWriter(f, data[0].keys())
        dw.writeheader()
        dw.writerows(data)
    print("Data written to {}".format(csv_name))


if __name__ == "__main__":
    concat_boards_to_csv()
