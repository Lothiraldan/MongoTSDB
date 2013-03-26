from pymongo import Connection
from datetime import datetime
from itertools import chain

from ranges import *

class TSDB(object):
    def __init__(self, database_name):
        self.db = Connection()[database_name]

    def insert(self, metric, **tags):
        metric_name = metric.pop("name")

        if tags:
            metric['tags'] = tags

        self.db[metric_name].insert(metric)

    def request(self, request):
        request = request.copy()

        step = request.pop('step')

        # Make start and stop match step boundaries
        start = request.pop('start')
        # start = start - (start % step)

        stop = request.pop('stop')
        # if stop % step:
        #     stop = stop + (step - (stop % step))

        request_call = request.pop("request")
        aggregation_function, metric_name = self._parse_request(request_call)
        tags = request.pop("tags", {})

        collection = self.db[metric_name]
        cache_collection = self.db['%s.cache' % metric_name]

        # If avg is function or tags wildcard value is used, cannot use cache
        if aggregation_function == 'avg' or '*' in tags.values():
            worker = MultiRangeWorker(start, stop, step, aggregation_function,
                tags, collection)
            result = worker.compute()
            # self.save_result_in_cache(result, metric_name, step, aggregation_function)
            return result
        else:
            range_set = RangeSet(start, stop, step, aggregation_function, tags,
                collection)

            self._load_from_cache(start, stop, step, aggregation_function,
                range_set, cache_collection)

            workers = range_set.generate_workers()
            results = list(chain.from_iterable([w.compute() for w in workers]))

            self.save_result_in_cache(results, metric_name, step,
                aggregation_function)

            return results

    def _load_from_cache(self, start, stop, step, aggregation_function,
            range_set, cache_collection):
        # Compute possibles steps size
        steps = [step]
        steps_divisors = [2, 4, 5, 6, 7, 10, 12, 24]
        for divisor in steps_divisors:
            new_step = step/float(divisor)
            if new_step.is_integer():
                steps.append(int(new_step))


        cache_request = {'function': aggregation_function,
            'step': {'$in': steps}, 'date': {'$gte': start, '$lt': stop}}

        caches = cache_collection.find(cache_request).sort('step', -1).sort('date')
        for cache in caches:
            range_set.add_sub_range(SubRange(cache['date'],
                cache['date'] + (cache['step'] - 1), cache['value']))

    def save_result_in_cache(self, result, metric_name, step, function):
        # Save results into cache
        cache_collection = self.db['%s.cache' % metric_name]
        # Ensure TTL
        cache_collection.ensure_index('cdate',
            expireAfterSeconds=5*60)

        for r in result:
            cache_document = {}
            date = r.get('_id')['date']
            cache_document['date'] = date
            cache_document['value'] = r['value']
            cache_document['step'] = step
            cache_document['function'] = function
            cache_document['cdate'] = datetime.now()
            cache_collection.insert(cache_document)

    def _parse_request(self, request_call):
        return request_call.replace('(', ' ').replace(')', '').split()
