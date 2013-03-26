from pipeline import PipelineGenerator

class RangeSet(object):

    def __init__(self, start, stop, step, function=None, tags=None,
            collection=None):
        self.start = start
        self.stop = stop
        self.step = step
        self.function = function
        self.tags = tags
        self.collection = collection

        self.ranges = []

        # Check if start is aligned to step
        if start % step != 0:
            first_end = start + step - (start % step) - 1
            self.ranges.append(Range(start, first_end))
            start = first_end + 1

        # Check if stop is aligned to step
        if (stop + 1) % step != 0:
            stop = stop - (stop % step)

        for n in range(start, stop, step):
            self.ranges.append(Range(n, (n + step) - 1))

        # Check if stop is aligned to step
        if stop != self.stop:
            self.ranges.append(Range(stop, self.stop))

    def get_sub_ranges(self):
        for range in self.ranges:
            for subrange in range.get_missing_ranges():
                yield subrange

    def generate_workers(self):
        smart_start = None
        smart_stop = None

        workers = []

        for range in self.ranges:
            # First range or last one was not a multi range
            if range.is_empty():
                if smart_start is None:
                    smart_start = range.start
                    smart_stop = range.stop
                else:
                    smart_stop = range.stop
            else:
                if smart_start is not None:
                    workers.append(MultiRangeWorker(smart_start, smart_stop,
                        self.step, self.function, self.tags, self.collection))
                    smart_start = None
                    smart_stop = None

                workers.append(RangeWorker(range, self.function, self.tags,
                    self.collection))

        if smart_start is not None:
            workers.append(MultiRangeWorker(smart_start, smart_stop,
                self.step, self.function, self.tags, self.collection))

        return workers

    def add_sub_range(self, subrange):
        corresponding_range = (subrange.start / self.step)
        self.ranges[corresponding_range].add_sub_range(subrange)


class Range(object):

    def __init__(self, start, stop):
        self.start = start
        self.stop = stop
        self.sub_ranges = []
        self.missing_ranges = [SubRange(start, stop)]

    def is_empty(self):
        return len(self.sub_ranges) == 0

    def is_full(self):
        return len(self.missing_ranges) == 0

    def is_partial(self):
        return len(self.sub_ranges) != 0 and len(self.missing_ranges) != 0

    def add_sub_range(self, subrange):
        for m_range in self.missing_ranges:
            if subrange in m_range:
                self.missing_ranges.remove(m_range)
                self.missing_ranges.extend(m_range.add_sub_range(subrange))

                self.sub_ranges.append(subrange)

                break

    def get_missing_ranges(self):
        return self.missing_ranges

    def __eq__(self, subrange):
        return self.__dict__ == subrange.__dict__

    def __str__(self):
        self_dict = self.__dict__.copy()
        self_dict.pop('missing_ranges')
        return '%s(%s)' % (self.__class__.__name__, self_dict)

    def __repr__(self):
        return self.__str__()


class SubRange(object):

    def __init__(self, start, stop, value=None):
        self.start = start
        self.stop = stop
        self.value = value

    def __eq__(self, subrange):
        return self.__dict__ == subrange.__dict__

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__dict__)

    def __repr__(self):
        return self.__str__()

    def __contains__(self, subrange):
        return subrange.start >= self.start and subrange.stop <= self.stop

    def add_sub_range(self, subrange):

        assert subrange.start >= self.start
        assert subrange.stop <= self.stop

        # If subrange match self
        if subrange.start == self.start and subrange.stop == self.stop:
            return []

        elif subrange.start == self.start:
            return [SubRange(subrange.stop + 1, self.stop)]

        elif subrange.stop == self.stop:
            return [SubRange(self.start, subrange.start - 1)]

        else:
            return [SubRange(self.start, subrange.start - 1),
                SubRange(subrange.stop + 1, self.stop)]


# Workers

class MultiRangeWorker(object):
    def __init__(self, start, stop, step, aggregation_function=None, tags=None,
            collection=None):
        self.start = start
        self.stop = stop
        self.step = step
        self.aggregation_function = aggregation_function
        self.tags = tags
        self.collection = collection

    def __eq__(self, subrange):
        return self.__dict__ == subrange.__dict__

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__dict__)

    def __repr__(self):
        return self.__str__()

    def compute(self):
        generator = PipelineGenerator()
        pipeline = generator.dispatch_function(self.start, self.stop, self.step,
            self.aggregation_function, self.tags)
        return self.collection.aggregate(pipeline)['result']

class RangeWorker(object):

    functions = {
        'sum': sum,
        'min': min,
        'max': max
    }

    def __init__(self, range, aggregation_function=None,
            tags=None, collection=None):
        self.start = range.start
        self.missing = range.missing_ranges
        self.partial = range.sub_ranges
        self.aggregation_function = aggregation_function
        self.tags = tags
        self.collection = collection

    def __eq__(self, subrange):
        return self.__dict__ == subrange.__dict__

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__dict__)

    def __repr__(self):
        return self.__str__()

    def compute(self):
        generator = PipelineGenerator()

        results = []

        generator = PipelineGenerator()
        for sub_range in self.missing:
            pipeline = generator.dispatch_function(sub_range.start, sub_range.stop,
                function=self.aggregation_function, tags=self.tags)
            results.append(self.collection.aggregate(pipeline)['result'][0]['value'])

        function = self.functions[self.aggregation_function]

        results.extend([x.value for x in self.partial])

        id_doc = {'date': self.start}

        if self.tags:
            id_doc['tags'] = self.tags

        return [{'_id': id_doc, 'value': function(results)}]

