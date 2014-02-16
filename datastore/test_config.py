'''
Created on 2014/1/26

@author: sushih-wen
'''

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

AZURE_ACCOUNT_NAME = ''
AZURE_ACCOUNT_KEY = ''

try:
    from local_test_config import *
except ImportError:
    pass

DB_SETTINGS = {
    'dynamodb': {
        'engine': 'dynamodb',
        'region': 'ap-northeast-1',
        'aws_access_key_id': AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
        'default_throughput': {'read': 5,
                               'write': 5
                               }
    },
    'azure_table': {
        'engine': 'azure_table',
        'account_name': AZURE_ACCOUNT_NAME,
        'account_key': AZURE_ACCOUNT_KEY
    }
}


class SomeRecord(object):

    def __init__(self):
        self.key = 'some_key'
        self.answer = 'some_answer'
        self.number = 1
