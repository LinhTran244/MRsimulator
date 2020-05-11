from .datastructures import *
from .decorators import timeit
from multiprocessing.pool import Pool

class MRSimulator:

    # Seconds per cost unit
    SLEEPING_FACTOR=0.2

    def __init__(self, num_mapper=1, num_reducer=1):
        self._num_mapper = num_mapper
        self._num_reducer = num_reducer
        self._mapperPool = Pool(processes=num_mapper)
        self._reducerPool = Pool(processes=num_reducer)

    @staticmethod
    def _simulate_delay(cost):
        from time import sleep
        sleep(cost*MRSimulator.SLEEPING_FACTOR)

    class Mapper:
        def __init__(self,map_function):
            self.map_function = map_function

        def simulate_mapper(self, inputSplit):
            output = sum([self.map_function(k, v) for k, v in inputSplit],PairMultiset())
            MRSimulator._simulate_delay(len(output))
            return output

    class Reducer:
        def __init__(self,reduce_function):
            self.reduce_function = reduce_function

        def simulate_reducer(self, groupSplit):
            output = sum([self.reduce_function(k, g) for k,g in groupSplit.items()],PairMultiset())
            MRSimulator._simulate_delay(len(output))
            return output

    @timeit
    def _shuffle(self, all_map_output: PairMultiset):
        # create groups
        groups = {}
        for k,v in all_map_output:
            same_key_group = groups.get(k) or SameKeyGroup(k)
            same_key_group.add((k,v))
            groups[k] = same_key_group

        # split
        group_list = list(groups.items())
        groups_splits = [{} for i in range(0, self._num_reducer)]
        for i in range(0,len(group_list)):
            k,v = group_list[i]
            groups_splits[i % self._num_reducer].update({k:v})

        MRSimulator._simulate_delay(len(all_map_output) * 2)

        return groups_splits

    @timeit
    def _reduce_phase(self, group_splits, reducer):
        reducers = [self._mapperPool.apply_async(reducer.simulate_reducer, (group_split,)) for group_split in
                    group_splits]
        output = sum([r.get() for r in reducers], PairMultiset())
        return output

    @timeit
    def _map_phase(self, input_splits, mapper):
        mappers = [self._mapperPool.apply_async(mapper.simulate_mapper, (split,)) for split in input_splits]
        all_map_results = sum([m.get() for m in mappers], PairMultiset())
        return all_map_results

    def execute(self, input, map, reduce):

        mapper = MRSimulator.Mapper(map)
        reducer = MRSimulator.Reducer(reduce)

        log_time = {}

        if not isinstance(input, PairMultiset):
            raise ValueError("Input must be a PairMultiset instance")
        input_splits = input.split(self._num_mapper)

        # Simulated map step
        all_map_results = self._map_phase(input_splits, mapper, log_time=log_time)

        # Simulated shuffle step
        group_splits = self._shuffle(all_map_results, log_time=log_time)

        # Simulated reduce step
        output = self._reduce_phase(group_splits, reducer, log_time=log_time)

        print("Output: %s\n" % output)
        print("Mappers: {m} Reducers: {r}\n".format(m=self._num_mapper, r=self._num_reducer))
        print("Map time:     {mt:6d}\n"
              "Shuffle time: {st:6d}\n"
              "Reduce time:  {rt:6d}\n"
              "Total time:   {t:6d}".format(mt=log_time["_MAP_PHASE"],
                                         st=log_time["_SHUFFLE"],
                                         rt=log_time["_REDUCE_PHASE"],
                                         t=log_time["_MAP_PHASE"]+log_time["_SHUFFLE"]+log_time["_REDUCE_PHASE"]))

        return output
