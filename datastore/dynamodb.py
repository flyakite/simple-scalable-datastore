'''
Created on 2014/1/23

@author: sushih-wen
'''
import random
import datetime
import cPickle as pickle
from functools import wraps
from boto import dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.exceptions import JSONResponseError, ValidationException
from boto.dynamodb2.table import Table


class DynamoDBError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


"""
boto.exception.JSONResponseError: JSONResponseError: 400 Bad Request
{u'message': u'Requested resource not found: Table: test_counter not found',
 u'__type': u'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException'}
This means the table is specified withoud schema
>>> users = Table('users', schema=[
            ...     HashKey('username'),
            ...     RangeKey('date_joined', data_type=NUMBER)
            ... ], throughput={
            ...     'read':20,
            ...     'write': 10,
            ... }, indexes=[
            ...     KeysOnlyIndex('MostRecentlyJoined', parts=[
            ...         RangeKey('date_joined')
            ...     ]),
            ... ],
"""

_COUNTER_DEFAULT_SHARD_FORMAT = 'shard_%s'
_COUNTER_SHARD_SUFFIX = '_' + _COUNTER_DEFAULT_SHARD_FORMAT
_COUNTER_SHARD_INDEX_TABLE_SUFFIX = '_shard_index'
_COUNTER_SHARD_COUNT_TABLE_SUFFIX = '_shard_count'
_DEFAULT_DATA_PROPERTY = 'data'
_DEFAULT_HASH_KEY_NAME = 'key'

# ERROR
_TABLE_DOES_NOT_EXIST = 'Looks like the table does not exist or the connection is wrong.'


def transform_table_name(f):
    """
    table name may contain time format, e.g"%Y%m%d".
    Translate it into string, according to current time.
    table_name should be the first argument
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        largs = list(args)
        if "%" in args[1]:
            if 'transform_time' in kwargs \
                    and isinstance(kwargs['transform_time'], datetime.datetime):
                t = kwargs['transform_time']
            else:
                t = datetime.datetime.utcnow()
            largs[1] = t.strftime(args[1])
            largs = tuple(largs)
        return f(*largs, **kwargs)
    return wrapper


class DynamoDB(object):

    '''
    DynamoDB simple wrapper for capy special use case
    '''

    def __init__(self, settings):
        '''
        read/write throughtput: read/write capacity times per second
        refer: http://aws.amazon.com/dynamodb/pricing/

        create layer2 connection
        '''
        self.settings = settings
        self.conn = dynamodb2.connect_to_region(settings.get('region', 'ap-northeast-1'),
                                                aws_access_key_id=settings.get('aws_access_key_id'),
                                                aws_secret_access_key=settings.get('aws_secret_access_key')
                                                )
        self.default_throughput = {'read': settings.get('default_throughput').get('read', 100),
                                   'write': settings.get('default_throughput').get('write', 100)
                                   }
        self._tables = {}
        #
        # hash_key_name is 'key', type is 'S' for string
        # no range key
        #
        self.hash_key_name = settings.get('hash_key_name', _DEFAULT_HASH_KEY_NAME)
        self.range_key_name = settings.get('range_key_name', None)
        self.data_property = settings.get('data_property', _DEFAULT_DATA_PROPERTY)
        self.default_schema = [HashKey(self.hash_key_name)]
        if self.range_key_name:
            self.default_schema.append(RangeKey(self.range_key_name))

    @transform_table_name
    def create_table(self, table_name, read=None, write=None, with_api_calls=True, transform_time=None):
        """
        returns a boto.dynamodb2.table.Table instance
        once the table is created, the schema and indexes can't be changed
        transform_time: is used with time formated table_name
        """

        kwargs = {'connection': self.conn,
                  'schema': self.default_schema,
                  'throughput': {
                      'read': read or self.default_throughput.get('read'),
                      'write': write or self.default_throughput.get('write')
                  },
                  # if we need secondary indexes, we need to specify range key
                  # if we specify a range key, the full key to get an element will become primary key+range key,
                  # in this case we need to use query, it's not what we want
                  # Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex
                  #                 'indexes':[IncludeIndex('KeyValueIndex',
                  #                                      parts=self.default_schema,
                  #                                      includes=[self.data_property]
                  #                                      )
                  #                                      ]
                  }
        if with_api_calls:
            try:
                table = Table.create(table_name, **kwargs)
                self._tables[table_name] = table
            except JSONResponseError as e:
                if 'Duplicate' in str(e):  # if the table already exist
                    return self.get_table(table_name)
                elif 'Table is being created' in str(e):  # if the table being created
                    raise DynamoDBError(e)
                else:
                    raise DynamoDBError(e)
        else:
            table = Table(table_name, **kwargs)

        return table

    @transform_table_name
    def get_table(self, table_name, with_api_calls=True):
        """
        get table
        """
        if table_name in self._tables.keys():
            return self._tables[table_name]
        if with_api_calls:
            table = Table(table_name, connection=self.conn)
            self._tables[table_name] = table
        else:
            table = Table(table_name, connection=self.conn, schema=self.default_schema)
        return table

    def get_or_create_table(self, table_name):
        table = self.get_table(table_name)
        if not table:
            table = self.create_table(table_name)
        return table

    @transform_table_name
    def update_throughput(self, table_name, read, write):
        table = self.get_table(table_name)
        table.update(throughput={'read': int(read),
                                 'write': int(write)})
        return table

    @transform_table_name
    def delete_table(self, table_name):
        table = self.get_table(table_name)
        return table.delete()

    def get_item(self, table_name, key, timedelta_slice=1):
        now = datetime.datetime.utcnow()
        item = self._get_item_from_time_sliced_table(table_name, key, now)
        if not item.keys() and timedelta_slice and '%' in table_name:  # if it's a time sliced table
            last_time = now - datetime.timedelta(timedelta_slice)
            item = self._get_item_from_time_sliced_table(table_name, key, last_time)
        return item

    def get_data(self, table_name, key, timedelta_slice=1, pickled=True):
        """
        get data from this or last time sliced table
        timedelta_slice: time slice size, in timedelta
        timedelta_slice=1 means one day

        """
        item = self.get_item(table_name, key, timedelta_slice=timedelta_slice)
        if item:
            if pickled and item.get(self.data_property, None):
                return pickle.loads(str(item[self.data_property]))
            else:
                return item[self.data_property]
        else:
            return None

    @transform_table_name
    def set_data(self, table_name, key, data,
                 range_key=None, pickled=True, overwrite=True, transform_time=None):
        """
        in dynamodb, we should always create table in advance
        because creating table takes time

        The range_key works only if the table schema has range key

        We can't use a dynamic range_key like created time,
        because if so, we can't get the data only by primary key

        Returns ``True`` on success.
        """
        if pickled:
            data = pickle.dumps(data)
        table = self.get_table(table_name)
        if not table:
            # this shouldn't happened,
            table = self.create_table(table_name)
        try:
            data = {self.hash_key_name: key, self.data_property: data}
            if range_key:
                data.update({self.range_key_name: range_key})

            item = table.put_item(data=data, overwrite=overwrite)
        except ValidationException as e:
            raise DynamoDBError(e)
        except JSONResponseError as e:
            raise DynamoDBError(_TABLE_DOES_NOT_EXIST + ": '%s'. %s" % (table_name, e))
        return item

    def _get_item_from_time_sliced_table(self, table_name, key, dtime):
        """
        dtime: is a datetime instance, indicates the
        """
        table_name = dtime.strftime(table_name)
        table = self.get_table(table_name)
        if not table:
            # this shouldn't happened,
            table = self.create_table(table_name)
        try:
            kw = {self.hash_key_name: key}
            return table.get_item(**kw)
        except ValidationException as e:
            raise DynamoDBError(e)

    def delete_data(self, table_name, key, timedelta_slice=1):
        item = self.get_item(table_name, key, timedelta_slice=timedelta_slice)
        if item.keys():

            return item.delete()
        else:
            return False

    def sharded_key(self, key, shard):
        return key + _COUNTER_SHARD_SUFFIX % shard

    def incr(self, table_name, key, amount=1, shard_count=1):
        """
        shard in primary key to avoid hot key and improve performance
        The strategy is try to increase first, if counter doesn't exist,
        create the shard index in the index table
        incr workds event the key doesn't previously exist
        """
        if shard_count < 1:
            shard_count = 1
        shard = str(random.randint(1, shard_count))
        sharded_key = self.sharded_key(key, shard)

        before_update = self.conn.update_item(table_name,
                                              {self.hash_key_name: {"S": sharded_key}},
                                              {self.data_property:
                                               {"Action": "ADD", "Value": {"N": str(amount)}}
                                               },
                                              return_values='ALL_OLD'
                                              )

        if not before_update:  # if this is a new shard key, before_update would be {}
            self._update_counter_indice(table_name, key, shard)

    def _update_counter_indice(self, table_name, key, shard, retry=3):
        """
        save/update counter shard indice to the seperated index table
        data types:
        http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes
        """
        index_table_name = table_name + _COUNTER_SHARD_INDEX_TABLE_SUFFIX
        try:
            self.conn.update_item(index_table_name,
                                  {self.hash_key_name: {"S": key}},
                                  {self.data_property:
                                   {"Action": "ADD", "Value": {"SS": [shard]}}  # string set
                                   }
                                  )
        except JSONResponseError as e:
            print index_table_name, key, shard
            if retry <= 0:
                raise DynamoDBError(e)
            else:
                self._update_counter_indice(table_name, key, shard, retry - 1)

    @transform_table_name
    def get_count(self, table_name, key, sharded=False):
        """
        because we shard on the primary key, we need to know how many shards
        how many shard exists
        """

        if sharded:
            counter_sum = 0
            counter = None
            counters = self._get_counters_from_indice(table_name, key)

            if counters:
                for counter in counters:

                    counter_sum += int(counter.get(self.data_property, 0))
            return counter_sum
        else:
            key = self.sharded_key(key, 1)  # only one shard
            counter = self.get_item(table_name, key)
            if counter:
                return counter.get(self.data_property, None)
            return None

    @transform_table_name
    def delete_counter(self, table_name, key):
        """
        delete sharded counter
        use hight level table to do batch write

        BatchWriteItem:

        Maximum operations in a single request
        - You can specify a total of up to 25 put or delete operations;
        - however, the total request size cannot exceed 1 MB (the HTTP payload).

        Not an atomic operation
        - Individual operations specified in a BatchWriteItem are atomic;
        - however BatchWriteItem as a whole is a "best-effort" operation and not an atomic operation.
        - That is, in a BatchWriteItem request, some operations might succeed and others might fail.
        """
        sharded_keys = self._get_counter_keys(table_name, key)
        if sharded_keys:
            counter_table = self.get_table(table_name)
            with counter_table.batch_write() as batch:
                for sharded_key in sharded_keys:
                    batch.delete_item(key=sharded_key)

        self._delete_counter_indice(table_name, key)

    def _get_counter_keys(self, table_name, key):
        table_name = table_name + _COUNTER_SHARD_INDEX_TABLE_SUFFIX
        shards = self.get_data(table_name, key, pickled=False) or []
        keys = []
        for shard in shards:
            keys.append(self.sharded_key(key, shard))
        return keys

    def _delete_counter_indice(self, table_name, key):
        table_name = table_name + _COUNTER_SHARD_INDEX_TABLE_SUFFIX
        self.delete_data(table_name, key)

    def _get_counters_from_indice(self, table_name, key):

        counters = []
        keys = self._get_counter_keys(table_name, key)
        if keys:
            dkeys = [{self.hash_key_name: k} for k in keys]
            counter_table = self.get_table(table_name)
            counters = counter_table.batch_get(dkeys)  # , attributes_to_get=[self.data_property]

        return counters


#     def get_counter_shard_count(self, table_name, key):
#         table_name = table_name + _COUNTER_SHARD_COUNT_TABLE_SUFFIX
#         return self.get_data(key, pickled=False)
#
#     def update_counter_shard_count(self, table_name, key, shard_count):
#         count = self.get_counter_shard_count(table_name, key)
#         shard_count_table_name = table_name + _COUNTER_SHARD_COUNT_TABLE_SUFFIX
#         if not count or count > shard_count:
#             self.set_data(shard_count_table_name, key, shard_count, pickled=False)
#     def _create_all_counter_shards(self, table_name, key, shard_count=1):
#         """
#         create all counter shards,
#         batch_write is not a good idea
#         some of the shard may already exists
#         """
#         table = self.get_table(table_name)
#         with table.batch_write() as batch:
#             for shard in xrange(1, int(shard_count)+1):
#                 key = key+_COUNTER_SHARD_SUFFIX % shard
#                 self.set_data(table_name, key, 0, pickled=False, overwrite=False)
#                 batch.put_item(data={
#                                      self.hash_key_name: key,
#                                      self.data_property: 0
#                 }, overwrite=False)
