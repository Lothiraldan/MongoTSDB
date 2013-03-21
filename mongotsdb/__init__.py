from pymongo import Connection

from datetime import datetime

from ranges import Range, SubRange, RangeSet, MultiRange

class TSDB(object):
    def __init__(self, database_name):
        self.db = Connection()[database_name]
        self.aggregator = Aggregator()

    def insert(self, metric, **tags):
        metric_name = metric.pop("name")

        if tags:
            metric['tags'] = tags

        self.db[metric_name].insert(metric)

    def request(self, request):
        request = request.copy()

        step = request['step']

        # Make start and stop match step boundaries
        start = request['start']
        start = start - (start % step)
        request['start'] = start

        stop = request['stop']
        if stop % step:
            stop = stop + (step - (stop % step))
            request['stop'] = stop

        request_call = request.pop("request")
        aggregation_function, metric_name = self._parse_request(request_call)
        tags = request.pop("tags", [])

        pipeline = self.aggregator.dispatch_function(aggregation_function, request, tags)

        if pipeline is None:
            raise Exception("Could not generate pipeline")

        result = self.db[metric_name].aggregate(pipeline)

        # Save results into cache
        cache_collection = self.db['%s.cache' % metric_name]
        # Ensure TTL
        cache_collection.ensure_index('cdate',
            expireAfterSeconds=5*60)

        for r in result['result']:
            cache_document = {}
            date = r.get('_id')['date']
            cache_document['date'] = date
            cache_document['value'] = r['value']
            cache_document['step'] = step
            # cache_document['']
            cache_document['cdate'] = datetime.now()
            cache_collection.insert(cache_document)

        return result['result']

    def _parse_request(self, request_call):
        return request_call.replace('(', ' ').replace(')', '').split()


class Aggregator(object):

    def dispatch_function(self, function, args, tags):
        pipeline = [self._request_match(args['start'], args['stop'], tags),
            self._aggregate_date(args['step'], tags)]

        function_call = getattr(self, function, None)

        if function_call is None:
            return None

        pipeline.append(self._regroup(function_call(), tags))

        return pipeline

    # Operator

    def sum(self):
        return '$sum'

    def min(self):
        return '$min'

    def max(self):
        return '$max'

    def avg(self):
        return '$avg'

    # Util function

    def _request_match(self, start, stop, tags):
        base = {'$match': {'date': {'$gte': start, '$lt': stop}}}

        for tag in tags:
            if tags[tag] != '*':
                base['$match']['tags.%s' % tag] = tags[tag]

        return base

    def _aggregate_date(self, step, tags):
        base = {'$project': {'value': 1, 'date': {'$subtract':
            ['$date', {'$mod': ['$date', step]}]}}}

        for tag in tags:
            base['$project']['tags.%s' % tag] = 1

        return base

    def _regroup(self, function_name, tags):
        base = {'$group': {'_id': {'date': '$date'}, 'value':
            {function_name: '$value'}}}

        for tag in tags:
            base['$group']['_id'].setdefault('tags', {})['%s' % tag] = '$tags.%s' % tag

        return base
