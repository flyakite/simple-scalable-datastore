'''
Created on 2014/1/23

@author: sushih-wen
'''


from dynamodb import DynamoDB
from functools import partial
from azuretable import AzureTable


class Datastore():

    """
    datastore interface for different database
    DynamoDB Example settings

    'dynamodb': {
        'engine': 'dynamodb',
        'region': 'ap-northeast-1',
        'aws_access_key_id': AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
        'default_throughput': {'read': 5,
                               'write': 5
                               }
    }

    Azure Table Example settings
    'azure_table': {
        'engine': 'azure_table',
        'account_name': AZURE_ACCOUNT_NAME,
        'account_key': AZURE_ACCOUNT_KEY
    }

    Potential Errors:
    from boto.dynamodb.exceptions import DynamoDBResponseError
    #connection, attempt to delete while creating, dulplicate table name
    """

    def __init__(self, settings):

        self.settings = settings
        if self.settings['engine'].lower() == 'dynamodb':
            self.db = DynamoDB(settings)
        elif self.settings['engine'].lower() == 'azure_table':
            self.db = AzureTable(settings)
        else:
            raise NotImplementedError("%s datastore is not implement yet." %
                                      self.settings.get('engine'))

    def __wrap(self, method, *args, **kwargs):
        return getattr(self.db, method)(*args, **kwargs)

    def __getattr__(self, method):
        return partial(self.__wrap, method)
