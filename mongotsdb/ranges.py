class RangeSet(object):

    def __init__(self, start, stop, step):
        self.start = start
        self.stop = stop
        self.step = step

        self.ranges = []
        for n in range(start, stop, step):
            self.ranges.append(Range(n, (n + step) - 1))

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
                        self.step))
                    smart_start = None
                    smart_stop = None

                workers.append(RangeWorker(range.missing_ranges,
                    range.sub_ranges))

        if smart_start is not None:
            workers.append(MultiRangeWorker(smart_start, smart_stop, self.step))

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

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__dict__)

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
    def __init__(self, start, stop, step):
        self.start = start
        self.stop = stop
        self.step = step

    def __eq__(self, subrange):
        return self.__dict__ == subrange.__dict__

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__dict__)

    def __repr__(self):
        return self.__str__()

class RangeWorker(object):
    def __init__(self, missing, partial):
        self.missing = missing
        self.partial = partial

    def __eq__(self, subrange):
        return self.__dict__ == subrange.__dict__

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__dict__)

    def __repr__(self):
        return self.__str__()
