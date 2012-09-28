MongoTSDB : Mongo Time Series DB
================================

**MongoTSDB** is a small python library which help you to save datas in a
time-series way and make time-series queries. It's powered by MongoDB
aggregation framework, so it requires MongoDB version 2.1.

Metric format
-------------

 - A timestamp.
 - A value.
 - A name.

Insertion
---------

Insert a sample metric ::

    from mongotsdb import TSDB
    tsdb = TSDB('database')

    data = {'date': 1348813543, 'value': 42, 'name': 'connections'}
    tsdb.insert()

Queries
-------

For all the queries below, we have theses data inserted ::

    for i in range(20):
        tsdb.insert({'date': i, 'value': i*10, 'name': 'sample'})

Queries format:

 - request format: 'operator(metric_name)'
 - start date, a timestamp
 - stop date, a timestamp
 - step, an int

Get the sum of all metric ::

    tsdb.request({'request': 'sum(sample)', 'start': 0, 'stop': 20, 'step': 20})

This request will return ::
    
    [{u'_id': {u'date': 0}, u'value': 1900}]

Not very interesting, try to get the sum of metrics by time range equal to 5 ::

    tsdb.request({'request': 'sum(sample)', 'start': 0, 'stop': 20, 'step': 5})

Output ::

    [{u'_id': {u'date': 15}, u'value': 850},
     {u'_id': {u'date': 5}, u'value': 350},
     {u'_id': {u'date': 10}, u'value': 600},
     {u'_id': {u'date': 0}, u'value': 100}]

Huzzah!

You have access to several operators:

 - sum
 - avg
 - min
 - max

Example with avg ::
  
    tsdb.request({'request': 'sum(sample)', 'start': 0, 'stop': 20, 'step': 5})

Output ::

    [{u'_id': {u'date': 15}, u'value': 170.0},
     {u'_id': {u'date': 5}, u'value': 70.0},
     {u'_id': {u'date': 10}, u'value': 120.0},
     {u'_id': {u'date': 0}, u'value': 20.0}]

Tags
----

When inserting metrics or make request on them, you can use tags. It allows to
make multi-dimensionnal requests.

Inserting
_________

You can add tags to metrics, for example ::

    from mongotsdb import TSDB
    tsdb = TSDB('database')

    data = {'date': 1348813543, 'value': 42, 'name': 'connections'}
    tags = {'server': 'server1', 'application': 'youtube'}
    data['tags'] = tags
    tsdb.insert(data)

Requests
________

In each request, you can set tags and request will process only metrics which
match tags.

For each following queries, these metrics are in DB ::

    datas = [(1, 10, 'host1', '1'), (3, 20, 'host1', '3'),
        (13, 20, 'host2', '1'), (17, 20, 'host2', '4'),
        (19, 30, 'host1', '2'), (5, 40, 'host2', '2'),
        (23, 50, 'host1', '3')]

    for data in datas:
        metric = {'date': data[0], 'value': data[1],
            'name': 'metric_tags'}
        metric_tags = {'host': data[2], 'category': data[3]}
        tsdb.insert(metric, **metric_tags)

You can now make request by setting tags value ::

    tsdb.request({'stop': 30, 'start': 0, 'step': 10, 'request':
        'avg(sample)', 'tags': {'host': 'host1'}})

Output ::

    [{u'_id': {u'date': 20, u'tags': {u'host': u'host1'}}, u'value': 50.0},
     {u'_id': {u'date': 10, u'tags': {u'host': u'host1'}}, u'value': 30.0},
     {u'_id': {u'date': 0, u'tags': {u'host': u'host1'}}, u'value': 15.0}]

You can also set multiple tags values ::

    tsdb.request({'stop': 30, 'start': 0, 'step': 10, 'request':
        'avg(sample)', 'tags': {'host': 'host1', 'category': '3'}})

Output ::

    [{u'_id': {u'date': 20, u'tags': {u'category': u'3', u'host': u'host1'}},
      u'value': 50.0},
     {u'_id': {u'date': 0, u'tags': {u'category': u'3', u'host': u'host1'}},
      u'value': 20.0}]

The real awesome feature is that you can choose to group by tags ::

    tsdb.request({'stop': 30, 'start': 0, 'step': 10, 'request':
        'avg(sample)', 'tags': {'host': '*'}})

Output ::

    [{u'_id': {u'date': 20, u'tags': {u'host': u'host1'}}, u'value': 50.0},
     {u'_id': {u'date': 0, u'tags': {u'host': u'host2'}}, u'value': 40.0},
     {u'_id': {u'date': 10, u'tags': {u'host': u'host1'}}, u'value': 30.0},
     {u'_id': {u'date': 10, u'tags': {u'host': u'host2'}}, u'value': 20.0},
     {u'_id': {u'date': 0, u'tags': {u'host': u'host1'}}, u'value': 15.0}]

As you can see, for the date 10, we have two results, one for host1 and another
one for host2.

You can also group by multiple tags ::

    tsdb.request({'stop': 30, 'start': 0, 'step': 10, 'request':
        'avg(sample)', 'tags': {'host': '*', 'category': '*'}})

Output ::

    [{u'_id': {u'date': 20, u'tags': {u'category': u'3', u'host': u'host1'}},
      u'value': 50.0},
     {u'_id': {u'date': 0, u'tags': {u'category': u'2', u'host': u'host2'}},
      u'value': 40.0},
     {u'_id': {u'date': 10, u'tags': {u'category': u'4', u'host': u'host2'}},
      u'value': 20.0},
     {u'_id': {u'date': 10, u'tags': {u'category': u'1', u'host': u'host2'}},
      u'value': 20.0},
     {u'_id': {u'date': 0, u'tags': {u'category': u'3', u'host': u'host1'}},
      u'value': 20.0},
     {u'_id': {u'date': 10, u'tags': {u'category': u'2', u'host': u'host1'}},
      u'value': 30.0},
     {u'_id': {u'date': 0, u'tags': {u'category': u'1', u'host': u'host1'}},
      u'value': 10.0}]

You can even combine wildcard tag value with custom tag value.

Run tests
---------

In order to run tests, you can:

 - Run tests files by hand.
 - Install nosetests (pip install nose) and run it from project root.
