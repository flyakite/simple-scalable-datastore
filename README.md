simple-scalable-datastore
=========================

A Python implementation of an API for key-value datastore and sharded counter build on top of DynamoDB and Azure Table.

Download
--------
    git clone git@github.com:flyakite/simple-scalable-datastore.git

Install
-------
    virtualenv --python=python2.7 --distribute --no-site-packages venv
    . venv/bin/activate
    pip install -r requirements.txt

Usage
-----
### DB Initialization
    from datastore.api import DataStore
    
    # DynamoDB
    db = DataStore({
      'engine': 'dynamodb',
      'region': 'ap-northeast-1',
      'aws_access_key_id': AWS_ACCESS_KEY_ID,
      'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
      'default_throughput': {'read': 5,
                             'write': 5
    })

    # Azure Table
    db = DataStore({
      'engine': 'azure_table',
      'account_name': AZURE_ACCOUNT_NAME,
      'account_key': AZURE_ACCOUNT_KEY
    })

### Table Operations
    # create table
    db.create_table('table') # DynamoDB table is creating...

    # get table
    table = db.get_tale('table')

    # delete table
    db.delete_table('table') # DynamoDB table is deleting...

### Create table with provisioned throughput (DynamoDB)
    db.create_table('table2', read=20, write=10)
    
### Set and Get data
    # set data
    db.get_data('table', 'key', 'value')
    
    # get data
    data = db.get_data('table', 'key')

### Counter
    # increment
    db.incr('table', 'counter')

    # get count
    db.get_count('table', 'counter') # 1

    # increment a certain amount
    db.incr('table', 'counter', amount=3)

    # sharded counter
    # sharded counter prevents 'hot' key in DynamoDB 
    # and improves performance in Azure Table (less retry)
    db.create_table('table_shard_index') # needed for DynamoDB
    db.incr('table', 'counter', shard_count=10)

    # get sharded counter count
    sum = db.get_counter('table', 'counter', sharded=True)

    # delete counter
    db.delete_counter('table', 'counter')
    

### Config
Please apply your Amazon AWS account/secret or Azure Table account/secret and put it in test_config.py before you run unittests

### Also...
1. Before you use the code you still need to read official documents and understand the idea of each database, this is just a basic implementation for simple usage.
2. In DynamoDB, creating/deleting table takes around 30 to 60 seconds. Make sure to create table in advanced.
3. Beware, there are two modules boto.dynamodb and boto.dynamodb2 with two layer APIs each. Try to used the latest while deveoping if possible.
4. Azure Table is not full ACID featured, so I used eTag and if-match to update the counter and I found it not very efficient during hot times.
5. In DynamoDB's implementation of the API, table name including time format like "%Y%m%d" is supported. In this way, we can create time series tables for better management, provision higher table thoughput for current 'hot' tables, and even drop the entire stale table instead of deleting each single entites.

### Pull requests and Issues
Welcome pull requests and issues. Please also run tests and check pep8 before submit.

