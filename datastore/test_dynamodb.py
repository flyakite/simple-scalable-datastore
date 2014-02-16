'''
Created on 2014/1/23

@author: sushih-wen
'''

import time
import datetime
import unittest
from dynamodb import DynamoDB, DynamoDBError
from test_config import DB_SETTINGS, SomeRecord
from threading import Thread
from uuid import uuid4


class TestDynamoDBTestCase(unittest.TestCase):

    """
    Create a tables before you run the tests, and it takes a minute
    """

    def setUp(self):

        self.settings = DB_SETTINGS['dynamodb']
        self.db = DynamoDB(self.settings)
        try:
            self.db.create_table('test', 5, 5)
            self.db.create_table('test_%Y%m%d', 5, 5)
            self.db.create_table('test_%Y%m%d', 5, 5,
                                 transform_time=datetime.datetime.utcnow() - datetime.timedelta(1))
        except DynamoDBError as e:
            print e

    def tearDown(self):
        s = SomeRecord()
        try:
            self.db.delete_data('test', 'k')
        except:
            pass
        try:
            self.db.delete_data('test', 'c')
        except:
            pass
        try:
            self.db.delete_data('test', s.key)
        except:
            pass
        return self

    def test_create_table(self):
        table = self.db.get_table('test')
        self.assertTrue(table.table_name == 'test')

    def test_create_delete_data(self):
        # pickled
        result = self.db.set_data('test', 'k', 'v')
        self.assertEqual(result, True, 'fail to set data')
        data = self.db.get_data('test', 'k')
        self.assertEqual(data, 'v', 'fail to get data')

        # pickled object
        c = SomeRecord()
        self.db.set_data('test', c.key, c)
        new_c = self.db.get_data('test', c.key)
        self.assertTrue(new_c.key == c.key)
        self.assertTrue(new_c.answer == c.answer)
        self.assertTrue(new_c.number == c.number)

        # unpickled
        self.db.set_data('test', 'k2', set(['a', 'b']), pickled=False)
        data = self.db.get_data('test', 'k2', pickled=False)
        self.assertEqual(data, set(['a', 'b']), 'fail getting correct unpickled data')

        self.assertTrue(self.db.delete_data('test', 'k'))
        self.assertTrue(self.db.delete_data('test', 'k2'))
        self.assertTrue(self.db.delete_data('test', c.key))
        self.assertFalse(self.db.get_data('test', 'k'))

    def test_timesliced_table(self):
        yesterday = datetime.datetime.utcnow() - datetime.timedelta(1)
        self.db.set_data("test_%Y%m%d", "ky", "dy", transform_time=yesterday)
        self.assertEqual(self.db.get_data("test_%Y%m%d", "ky"), "dy")
        self.db.delete_data('test_%Y%m%d', 'ky')
        self.assertEqual(self.db.get_data("test_%Y%m%d", "ky"), None)


class TestDynamoDBCounterTestCase(unittest.TestCase):

    """
    Create table 'test_counter' and 'test_counter_shard_index' before you run this test
    """

    def setUp(self):
        self.settings = DB_SETTINGS['dynamodb']
        self.db = DynamoDB(self.settings)
        self.db.create_table('test_counter', 5, 5)
        self.db.create_table('test_counter_shard_index', 5, 5)
        self.db.create_table('test_counter_%Y%m%d', 5, 5)
        self.db.create_table('test_counter_%Y%m%d', 5, 5,
                             transform_time=datetime.datetime.utcnow() - datetime.timedelta(1))

    def test_incr(self):
        table_name = 'test_counter'
        key = str(uuid4())
        self.db.incr(table_name, key)
        self.assertEqual(self.db.get_count(table_name, key), 1)
        self.db.incr(table_name, key, 2)
        self.assertEqual(self.db.get_count(table_name, key), 3)
        self.db.incr(table_name, key, -1)
        self.assertEqual(self.db.get_count(table_name, key), 2)
        self.db.delete_counter(table_name, key)

    def test_multithread_counter(self):
        table_name = 'test_counter'
        count = 20
        key = str(uuid4())

        def incr_counter():
            self.db.incr(table_name, key)

        start = self.db.get_count(table_name, key) or 0
        threadlist = []
        for _ in xrange(0, count):
            thread = Thread(target=incr_counter)
            thread.start()
            threadlist.append(thread)
            time.sleep(0.01)

        for t in threadlist:
            t.join(60)

        final = self.db.get_count(table_name, key)
        self.assertEqual(start + count, final)
        self.db.delete_counter(table_name, key)

    def test_shard_counter(self):
        table_name = 'test_counter'
        shard_count = 100
        key = str(uuid4())
        self.db.incr(table_name, key, 1, shard_count=shard_count)
        self.assertEqual(self.db.get_count(table_name, key, sharded=True), 1)
        self.db.incr(table_name, key, 2, shard_count=shard_count)
        self.assertEqual(self.db.get_count(table_name, key, sharded=True), 3)
        self.db.delete_counter(table_name, key)

    def test_multithread_shard_counter(self):
        table_name = 'test_counter'
        count = 50
        shard_count = 60
        key = str(uuid4())

        def incr_counter():
            self.db.incr(table_name, key, shard_count=shard_count)

        start = self.db.get_count(table_name, key) or 0
        threadlist = []
        for _ in xrange(0, count):
            thread = Thread(target=incr_counter)
            thread.start()
            threadlist.append(thread)
            time.sleep(0.01)

        for t in threadlist:
            t.join(60)

        final = self.db.get_count(table_name, key, sharded=True)
        self.assertEqual(start + count, final)
        self.db.delete_counter(table_name, key)


if __name__ == '__main__':
    unittest.main()
