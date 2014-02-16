'''
Created on 2014/2/5

@author: sushih-wen
'''
import time
import random
from azure import storage
from azure import WindowsAzureError
from azure import WindowsAzureConflictError
from azure import WindowsAzureMissingResourceError

_COUNTER_EXCEEDED_MAX_RETRY = 'Counter exceeded max retry'
_COUNTER_DEFAULT_SHARD_FORMAT = 'shard_%s'
_COUNTER_DEFAULT_ROW_KEY = 'shard_1'

# Errors
_TABLE_NAME_ERROR = 'Table name error'


class AzureTableError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class AzureTable(object):

    """
    Azure Table simple wrapper, atomic counter update and sharding
    """

    def __init__(self, settings):
        self.settings = settings
        self.tableservice = storage.TableService(
            account_name=settings['account_name'],
            account_key=settings['account_key'])
        self.counter_property = settings.get('counter_property', 'c')
        self.max_counter_retry = settings.get('max_counter_retry', 100)

    def create_table(self, table_name, fail_on_exist=False):
        """
            Name of the table to create. Table name may contain only
            alphanumeric characters and cannot begin with a numeric character.
            It is case-insensitive and must be from 3 to 63 characters long.
        """
        try:
            return self.tableservice.create_table(table_name, fail_on_exist)
        except WindowsAzureError as e:
            if 'One of the request inputs is out of range' in str(e):
                raise AzureTableError(_TABLE_NAME_ERROR + " '%s'. %s" % (table_name, e))

    def get_table(self, table_name):
        # return self.tableservice.query_tables(table_name, top=1)  # this doesn't work
        return self.tableservice.query_tables(table_name)[0]

    def get_or_create_table(self, table_name):
        return self.tableservice.create_table(table_name, fail_on_exist=False)

    def delete_table(self, table_name, fail_not_exist=False):
        return self.tableservice.delete_table(table_name, fail_not_exist)

    def set_data(self, table_name, key, data, row_key=''):
        """
        use upsert
        """
        partition_key = key
        return self.tableservice.insert_or_replace_entity(table_name, partition_key, row_key, {'data': data})

    def get_data(self, table_name, key, row_key='', select='data'):
        partition_key = key
        try:
            entity = self.tableservice.get_entity(table_name, partition_key, row_key, select=select)
            return entity.data
        except WindowsAzureMissingResourceError:
            return None

    def delete_data(self, table_name, key, row_key='', if_match='*'):
        partition_key = key
        return self.tableservice.delete_entity(table_name, partition_key, row_key, if_match=if_match)

    def incr(self, table_name, key, amount=1, shard_count=1):
        """
        shard_count: how many slot for this counter
        """
        shard = random.randint(1, shard_count)
        return self._incr(table_name, key, amount=amount, row_key=_COUNTER_DEFAULT_SHARD_FORMAT % shard)

    def _incr(self, table_name, key, amount=1, row_key=_COUNTER_DEFAULT_ROW_KEY, retry=0):
        """
        retry: current tim
        """
        retry += 1

        partition_key = key
        last_modified = '*'
        try:
            entity = self.tableservice.get_entity(table_name, partition_key, row_key, select=self.counter_property)
            if entity:
                last_modified = entity.etag
            else:
                return self._insert_new_counter(table_name, partition_key, row_key, amount)
        except WindowsAzureMissingResourceError:
            # missing table
            try:
                self.tableservice.query_tables(table_name)
            except WindowsAzureMissingResourceError:
                # no specified table
                self.create_table(table_name)
                return self._retry_incr(table_name, key, amount, row_key, retry)
            except WindowsAzureConflictError:
                # TODO: I really don't understand why a query would cause this, but ,jack, it did happen
                return self._retry_incr(table_name, key, amount, row_key, retry)
            else:
                # or I guess the entity is missing
                return self._insert_new_counter(table_name, partition_key, row_key, amount)

        except WindowsAzureError as e:
            print 'something is wrong while querying, weird', e
            return self._retry_incr(table_name, key, amount, row_key, retry)

        try:
            new_count = getattr(entity, self.counter_property) + amount
            setattr(entity, self.counter_property, new_count)
            self.tableservice.update_entity(table_name, partition_key, row_key, entity, if_match=last_modified)
        except WindowsAzureError as e:
            return self._retry_incr(table_name, key, amount, row_key, retry)
        except Exception as e:
            raise AzureTableError(e)

    def _insert_new_counter(self, table_name, key, row_key, amount):
        entity = storage.Entity()
        entity.PartitionKey = key
        entity.RowKey = row_key
        setattr(entity, self.counter_property, amount)
        try:
            return self.tableservice.insert_entity(table_name, entity)
        except WindowsAzureConflictError as e:
            return self._retry_incr(table_name, key, amount, row_key, retry=1)
        except WindowsAzureMissingResourceError as e:
            # I have no idea what is missing
            return self._retry_incr(table_name, key, amount, row_key, retry=1)
        except Exception as e:
            raise AzureTableError(e)

    def _retry_incr(self, table_name, key, amount, row_key, retry):
        if retry >= self.max_counter_retry:
            raise AzureTableError("%s %s times, key: %s" % (_COUNTER_EXCEEDED_MAX_RETRY, retry - 1, key))
        time.sleep(exponential_backoff_waiting_time(retry))
        return self._incr(table_name, key, amount, row_key, retry=retry + 1)

    def get_count(self, table_name, key, row_key=_COUNTER_DEFAULT_ROW_KEY, sharded=False):
        partition_key = key
        if sharded:
            query = "PartitionKey eq '%s'" % partition_key
            counters = self.tableservice.query_entities(table_name, query, select=self.counter_property)
            all_count = 0
            for counter in counters:
                all_count += int(getattr(counter, self.counter_property))
            return all_count
        else:
            try:
                entity = self.tableservice.get_entity(table_name, partition_key, row_key, select=self.counter_property)
                return getattr(entity, self.counter_property)
            except WindowsAzureMissingResourceError:
                return None
            except Exception as e:
                raise AzureTableError(e)

    def delete_counter(self, table_name, key, row_key=_COUNTER_DEFAULT_ROW_KEY):
        partition_key = key
        query = "PartitionKey eq '%s'" % partition_key
        counters = self.tableservice.query_entities(table_name, query)
        if counters:
            for counter in counters:
                self.tableservice.delete_entity(table_name, counter.PartitionKey, counter.RowKey)


def exponential_backoff_waiting_time(retries):
    """
    retires: times of retry
    return: time in second we should wait till next retry
    """
    retries = max(int(retries), 1)
    default_backoff = 0.05
    min_backoff = 0.01
    max_backoff = 60
    backoff = min_backoff + random.randrange(1000 * 0.8 * default_backoff, 1000 * 1.5 * default_backoff) / 1000.0
    backoff *= 2 ^ retries - 1
    backoff = min(backoff, max_backoff)
    return backoff  # print 'retry after %s seconds' % backoff
