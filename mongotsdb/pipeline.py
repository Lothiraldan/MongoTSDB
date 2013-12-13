
class PipelineGenerator(object):

    def dispatch_function(self, start, stop, step=None, function=None, tags=None):
        pipeline = [self._request_match(start, stop, tags),
            self._aggregate_date(step, tags)]

        function_call = getattr(self, function, None)

        if function_call is None:
            return None

        pipeline.append(self._regroup(function_call(), tags, group_by_date=step is not None))

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
        base = {'$match': {'date': {'$gte': start, '$lte': stop}}}

        for tag in tags:
            if tags[tag] != '*':
                base['$match']['tags.%s' % tag] = tags[tag]

        return base

    def _aggregate_date(self, step, tags):
        base = {'$project': {'value': 1}}

        if step is not None:
            base['$project']['date'] = {'$subtract': ['$date', {'$mod': ['$date', step]}]}

        for tag in tags:
            base['$project']['tags.%s' % tag] = 1

        return base

    def _regroup(self, function_name, tags, group_by_date=True):
        base = {'$group': {'value': {function_name: '$value'}, '_id': None}}

        if group_by_date:
            base['$group']['_id'] = {'date': '$date'}

        for tag in tags:
            base['$group']['_id'].setdefault('tags', {})['%s' % tag] = '$tags.%s' % tag

        return base
