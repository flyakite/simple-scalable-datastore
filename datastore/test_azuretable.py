'''
Created on 2014/2/6

@author: sushih-wen
'''

import unittest
import time
from azuretable import AzureTable
from test_config import DB_SETTINGS
from threading import Thread
from uuid import uuid4


class TestAzureTableTestCase(unittest.TestCase):

    def setUp(self):

        self.settings = DB_SETTINGS['azure_table']
        self.db = AzureTable(self.settings)
        self.db.create_table('test')
        self.db.create_table('testcounter')

    def tearDown(self):
        # self.db.delete_table('test')
        # delete table takes some time, cause the next test to fail
        pass

    def test_basic(self):
        table = self.db.get_table('test')
        self.assertEqual(table.name, 'test')

        self.db.set_data('test', 'test_partition', 'data')
        self.assertEqual(self.db.get_data('test', 'test_partition'), 'data')

    def test_incr(self):
        key = str(uuid4())
        count_ori = self.db.get_count('testcounter', key) or 0
        self.db.incr('testcounter', key)
        self.assertEqual(self.db.get_count('testcounter', key), count_ori + 1)
        self.db.incr('testcounter', key)
        self.assertEqual(self.db.get_count('testcounter', key), count_ori + 2)
        self.db.incr('testcounter', key, 3)
        self.assertEqual(self.db.get_count('testcounter', key), count_ori + 5)

    def test_incr_from_zero(self):
        key = str(uuid4())
        self.db.incr('testcounter', key)
        self.assertEqual(self.db.get_count('testcounter', key), 1)

    def test_multithread_counter(self):
        count = 20
        key = str(uuid4())

        def incr_counter():
            self.db.incr('testcounter', key)

        start = self.db.get_count('testcounter', key) or 0
        print 'start counter', start
        threadlist = []
        for _ in xrange(0, count):
            thread = Thread(target=incr_counter)
            thread.start()
            threadlist.append(thread)
            time.sleep(0.01)

        for t in threadlist:
            t.join(60)

        final = self.db.get_count('testcounter', key)
        print 'final counter', final
        self.assertEqual(start + count, final)

    def test_shard_counter(self):
        key = str(uuid4())
        self.db.incr('testcounter', key, 1, shard_count=100)
        self.db.incr('testcounter', key, 2, shard_count=100)
        self.db.incr('testcounter', key, 3, shard_count=100)
        final = self.db.get_count('testcounter', key, sharded=True)
        self.assertEqual(6, final)

    def test_multithread_sharded_counter(self):
        count = 2
        key = str(uuid4())

        def incr_counter():
            self.db.incr('testcounter', key, shard_count=10000)

        start = self.db.get_count('testcounter', key) or 0
        print 'start counter', self.db.get_count('testcounter', key, sharded=True)
        threadlist = []
        for _ in xrange(0, count):
            thread = Thread(target=incr_counter)
            thread.start()
            threadlist.append(thread)
            time.sleep(0.01)

        for t in threadlist:
            t.join(60)

        final = self.db.get_count('testcounter', key, sharded=True)
        print 'final counter', final
        self.assertEqual(start + count, final)
        self.db.delete_counter('testcounter', key)


if __name__ == '__main__':
    unittest.main()
