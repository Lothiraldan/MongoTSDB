import unittest

from pymongo import Connection

from mongotsdb import TSDB

from test_utils import (TemplateTestCase, template, Call, avg)


class FunctionnalTestCase(unittest.TestCase):

    def setUp(self):
        self.database_name = 'events'
        self.metric_name = 'sample'
        self.tsdb = TSDB(self.database_name)

    def tearDown(self):
        # Clear db
        Connection()[self.database_name][self.metric_name].remove()
        Connection()[self.database_name]['%s.cache' % self.metric_name].remove()


class InsertionTestCase(FunctionnalTestCase):

    def test_simple_insertion(self):
        # Define metric
        metric = {'date': 10, 'value': 42, 'name': self.metric_name}

        # Insert metric
        self.tsdb.insert(metric=metric)

        # Check that metric is in DB
        in_db = Connection()[self.database_name][self.metric_name].find_one()
        metric['_id'] = in_db['_id']

        self.assertEqual(metric, in_db)

    def test_tags_insertion(self):
        # Define metric
        metric = {'date': 10, 'value': 42, 'name': self.metric_name}
        metric_tags = {'tag1': 'tag1', 'tag2': 'tag2'}

        # Insert metric
        self.tsdb.insert(metric=metric, **metric_tags)

        #Chech that metric is in DB
        in_db = Connection()[self.database_name][self.metric_name].find_one()

        expected = metric
        expected['_id'] = in_db['_id']
        expected['tags'] = metric_tags

        self.assertEqual(expected, in_db)


class RequestTestCase(FunctionnalTestCase):

    __metaclass__ = TemplateTestCase

    def test_simple_sum_request(self):
        # Define metrics
        metrics = [
            {'date': 1, 'value': 2, 'name': self.metric_name},
            {'date': 7, 'value': 4, 'name': self.metric_name},
            {'date': 14, 'value': 7, 'name': self.metric_name}]

        # Insert metrics
        for metric in metrics:
            self.tsdb.insert(metric=metric)

        # Make request
        request = {'request': 'sum(%s)' % self.metric_name, 'start': 0,
            'stop': 19, 'step': 10}
        result = self.tsdb.request(request=request)

        # Check return
        expected = [{'_id': {'date': 0}, 'value': 6},
            {'_id': {'date': 10}, 'value': 7}]
        self.assertItemsEqual(result, expected)

    def test_tags_sum_request(self):
        # Define metrics
        metric_host_1 = [
            {'date': 1, 'value': 1, 'name': self.metric_name},
            {'date': 4, 'value': 3, 'name': self.metric_name}]
        metric_tags_1 = {'host': 1}
        metric_1 = (metric_tags_1, metric_host_1)

        metric_host_2 = [
            {'date': 3, 'value': 6, 'name': self.metric_name},
            {'date': 7, 'value': 12, 'name': self.metric_name}]
        metric_tags_2 = {'host': 2}
        metric_2 = (metric_tags_2, metric_host_2)

        # Insert metrics
        for (tags, metrics) in (metric_1, metric_2):
            for metric in metrics:
                self.tsdb.insert(metric=metric, **tags)

        # Make request
        request = {'request': 'sum(%s)' % self.metric_name, 'start': 0,
            'stop': 10, 'step': 5, 'tags': {'host': '*'}}
        result = self.tsdb.request(request=request)

        # Check return
        expected = [
            {'_id': {'date': 0, 'tags': {'host': 1}}, 'value': 4},
            {'_id': {'date': 0, 'tags': {'host': 2}}, 'value': 6},
            {'_id': {'date': 5, 'tags': {'host': 2}}, 'value': 12}]

        self.assertItemsEqual(expected, result)

        # Make request with specific tag
        request = {'request': 'sum(%s)' % self.metric_name, 'start': 0,
            'stop': 10, 'step': 5, 'tags': {'host': 2}}
        result = self.tsdb.request(request=request)

        # Check return
        expected = [
            {'_id': {'date': 0, 'tags': {'host': 2}}, 'value': 6},
            {'_id': {'date': 5, 'tags': {'host': 2}}, 'value': 12}]

        self.assertItemsEqual(expected, result)

    @template({
        'sum': Call('sum', sum),
        'avg': Call('avg', avg),
        'min': Call('min', min),
        'max': Call('max', max),
    })
    def _test_request_with_operator(self, operator, operator_function):
        # Define metrics
        events = [{'date': i, 'value': i, 'name': self.metric_name} for i
            in range(5)]

        expected = operator_function([event['value'] for event in events])

        # Insert metrics
        for event in events:
            self.tsdb.insert(metric=event)

        # Make request
        request = {'request': '%s(%s)' % (operator, self.metric_name),
            'start': 0, 'stop': len(events) + 1, 'step': len(events) + 1}
        result = self.tsdb.request(request=request)

        # Check return
        self.assertEqual(expected, result[0]['value'])

class RequestCacheTestCase(FunctionnalTestCase):

    def test_simple_sum_request(self):
        # Define metrics
        for i in range(20):
            self.tsdb.insert({'date': i, 'value': i*10, 'name': 'sample'})

        # Make request
        request = {'request': 'sum(%s)' % self.metric_name, 'start': 0,
            'stop': 9, 'step': 2}
        result = self.tsdb.request(request=request)

        # Check return
        expected = [{'_id': {'date': 0}, 'value': 10},
            {'_id': {'date': 2}, 'value': 50},
            {'_id': {'date': 4}, 'value': 90},
            {'_id': {'date': 6}, 'value': 130},
            {'_id': {'date': 8}, 'value': 170}]
        self.assertItemsEqual(result, expected)

        # Make new request
        request = {'request': 'sum(%s)' % self.metric_name, 'start': 0,
            'stop': 19, 'step': 10}
        result = self.tsdb.request(request=request)

        # Check return
        expected = [{'_id': {'date': 10}, 'value': 1450},
            {'_id': {'date': 0}, 'value': 450}]
        self.assertItemsEqual(result, expected)

    def test_with_tags(self):
        # Define metrics
        for i in range(20):
            self.tsdb.insert({'date': i, 'value': i*10, 'name': 'sample',
                'tags': {'even': i%2}})

        # Make request
        request = {'request': 'sum(%s)' % self.metric_name, 'start': 0,
            'stop': 9, 'step': 2, 'tags': {'even': 0}}
        result = self.tsdb.request(request=request)

        # Check return
        expected = [{'_id': {'date': 0, 'tags': {'even': 0}}, 'value': 0},
            {'_id': {'date': 2, 'tags': {'even': 0}}, 'value': 20},
            {'_id': {'date': 4, 'tags': {'even': 0}}, 'value': 40},
            {'_id': {'date': 6, 'tags': {'even': 0}}, 'value': 60},
            {'_id': {'date': 8, 'tags': {'even': 0}}, 'value': 80}]
        self.assertItemsEqual(result, expected)

        # Make new request
        request = {'request': 'sum(%s)' % self.metric_name, 'start': 0,
            'stop': 19, 'step': 10, 'tags': {'even': 0}}
        result = self.tsdb.request(request=request)

        # Check return
        expected = [{'_id': {'date': 10, 'tags': {'even': 0}}, 'value': 700},
            {'_id': {'date': 0, 'tags': {'even': 0}}, 'value': 200}]
        print "Result", result, expected
        self.assertItemsEqual(result, expected)
