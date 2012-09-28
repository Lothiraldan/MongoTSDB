from pymongo import Connection


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
        request_call = request.pop("request")
        aggregation_function, metric_name = self._parse_request(request_call)
        tags = request.pop("tags", [])

        pipeline = self.aggregator.dispatch_function(aggregation_function, request, tags)

        if pipeline is None:
            raise Exception("Could not generate pipeline")

        result = self.db.command({'aggregate': metric_name, 'pipeline': pipeline})

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
