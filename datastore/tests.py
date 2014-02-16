'''
Created on 2014/1/23

@author: sushih-wen
'''

import unittest
from uuid import uuid4
from api import Datastore
from test_config import DB_SETTINGS


class TestDataStoreAPITestCase(unittest.TestCase):

    def setUp(self):
        self.settings = DB_SETTINGS
        self.dynamodb = Datastore(DB_SETTINGS['dynamodb'])
        self.azure_table = Datastore(DB_SETTINGS['azure_table'])

    def tearDown(self):
        return self

    def test_dynamodb_get_and_set(self):
        key = str(uuid4())
        self.dynamodb.create_table('test')
        table = self.dynamodb.get_table('test')
        self.assertEqual(table.table_name, 'test')
        self.dynamodb.set_data('test', key, 'value')
        self.assertEqual(self.dynamodb.get_data('test', key), 'value')
        self.dynamodb.delete_data('test', key)
        self.assertEqual(self.dynamodb.get_data('test', key), None)

    def test_dynamodb_counter(self):
        key = str(uuid4())
        self.dynamodb.create_table('test_counter')
        self.dynamodb.create_table('test_counter_shard_index')
        self.dynamodb.incr('test_counter', key)
        self.assertEqual(self.dynamodb.get_count('test_counter', key), 1)
        self.dynamodb.incr('test_counter', key, amount=2)
        self.assertEqual(self.dynamodb.get_count('test_counter', key), 3)
        self.dynamodb.incr('test_counter', key, shard_count=10)
        self.assertEqual(self.dynamodb.get_count('test_counter', key, sharded=True), 4)
        self.dynamodb.delete_counter('test_counter', key)

    def test_azure_table(self):
        key = str(uuid4())
        self.azure_table.create_table('test')
        table = self.azure_table.get_table('test')
        self.assertEqual(table.name, 'test')
        self.azure_table.set_data('test', key, 'value')
        self.assertEqual(self.azure_table.get_data('test', key), 'value')
        self.azure_table.delete_data('test', key)
        self.assertEqual(self.azure_table.get_data('test', key), None)

    def test_azure_table_counter(self):
        key = str(uuid4())
        self.azure_table.create_table('testcounter')
        self.azure_table.incr('testcounter', key)
        self.assertEqual(self.azure_table.get_count('testcounter', key), 1)
        self.azure_table.incr('testcounter', key, amount=2)
        self.assertEqual(self.azure_table.get_count('testcounter', key), 3)
        self.azure_table.incr('testcounter', key, shard_count=10)
        self.assertEqual(self.azure_table.get_count('testcounter', key, sharded=True), 4)
        self.azure_table.delete_counter('testcounter', key)

if __name__ == '__main__':
    unittest.main()
