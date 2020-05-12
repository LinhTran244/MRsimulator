from .datastructures import *
from .decorators import timeit
from multiprocessing.pool import ThreadPool as Pool

import logging

logging.basicConfig(format="%(threadName)s:%(message)s",level=logging.DEBUG)
log = logging.getLogger("mrsimulator")

class MRSimulator:

    # Seconds per cost unit
    SLEEPING_FACTOR=0.1

    def __init__(self, num_mapper=1, num_reducer=1):
        self._num_mapper = num_mapper
        self._num_reducer = num_reducer
        self._mapperPool = Pool(processes=num_mapper)
        self._reducerPool = Pool(processes=num_reducer)

    @staticmethod
    def _simulate_delay(cost):
        from time import sleep
        sleep_time = cost*MRSimulator.SLEEPING_FACTOR
        log.debug("Sleeping %s", sleep_time)
        sleep(sleep_time)


    @staticmethod
    def simulate_mapper(inputSplit, map_function):
        output = sum([map_function(k, v) for k, v in inputSplit],PairMultiset())
        MRSimulator._simulate_delay(len(inputSplit))
        return output

    @staticmethod
    def simulate_reducer(groupSplit,reduce_function):
         output = sum([reduce_function(k, g) for k,g in groupSplit.items()],PairMultiset())
         MRSimulator._simulate_delay(sum([len(g) for k,g in groupSplit.items()]))
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

        MRSimulator._simulate_delay(3+len(all_map_output)/(self._num_mapper)+self._num_reducer*self._num_mapper*0.1)

        return groups_splits

    @timeit
    def _reduce_phase(self, group_splits, reduce_function):
        reducers = [self._mapperPool.apply_async(MRSimulator.simulate_reducer, (group_split,reduce_function)) for group_split in
                    group_splits]
        output = sum([r.get() for r in reducers], PairMultiset())
        return output

    @timeit
    def _map_phase(self, input_splits, map_function):
        mappers = [self._mapperPool.apply_async(MRSimulator.simulate_mapper, (split,map_function)) for split in input_splits]
        all_map_results = sum([m.get() for m in mappers], PairMultiset())
        return all_map_results

    def execute(self, input, map_function, reduce_function):

        log_time = {}

        if not isinstance(input, PairMultiset):
            raise ValueError("Input must be a PairMultiset instance")
        input_splits = input.split(self._num_mapper)

        # Simulated map step
        all_map_results = self._map_phase(input_splits, map_function, log_time=log_time)

        # Simulated shuffle step
        group_splits = self._shuffle(all_map_results, log_time=log_time)

        # Simulated reduce step
        output = self._reduce_phase(group_splits, reduce_function, log_time=log_time)

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
