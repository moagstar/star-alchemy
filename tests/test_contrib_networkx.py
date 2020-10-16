import difflib
import json
from unittest import TestCase

import networkx as nx

from examples.sales import schema
from star_alchemy.contrib.networkx import to_nx


class ToNxTestCase(TestCase):

    def test_to_nx(self):
        G = to_nx(schema)
        actual = json.dumps(nx.tree_data(G, schema.name), indent=4)
        expected = json.dumps(indent=4, obj={
            'name': 'sale',
            'id': 'sale',
            'children': [
                {
                    'name': 'product',
                    'id': 'product',
                    'children': [
                        {'name': 'category', 'id': 'category'}
                    ]
                },
                {
                    'name': 'employee',
                    'id': 'employee',
                    'children': [
                        {'name': 'department', 'id': 'department'},
                        {'name': 'employee_location', 'id': 'employee_location'}
                    ]
                },
                {
                    'name': 'customer',
                    'id': 'customer',
                    'children': [
                        {'name': 'customer_location', 'id': 'customer_location'}
                    ]
                }
            ]
        })
        if actual != expected:
            actual_lines = actual.split('\n')
            expected_lines = expected.split('\n')
            diff = difflib.unified_diff(actual_lines, expected_lines)
            self.fail('\n'.join(diff))
