from .datastructures import *
from multiprocessing.pool import ThreadPool

class MRSimulator:

    # Seconds per cost unit
    SLEEPING_FACTOR=0.1

    def __init__(self, num_mapper=1, num_reducer=1):
        self.num_mapper = num_mapper
        self.num_reducer = num_reducer
        self.mapperPool = ThreadPool(processes=num_mapper)
        self.reducerPool = ThreadPool(processes=num_reducer)

    @staticmethod
    def simulate_delay(cost):
        from time import sleep
        sleep(cost*MRSimulator.SLEEPING_FACTOR)

    class Mapper:
        def __init__(self,map_function):
            self.map_function = map_function

        def simulate_mapper(self, inputSplit):
            output = sum([self.map_function(k, v) for k, v in inputSplit],PairMultiset())
            MRSimulator.simulate_delay(len(output))
            return output

    class Reducer:
        def __init__(self,reduce_function):
            self.reduce_function = reduce_function

        def simulate_reducer(self, groupSplit):
            output = sum([self.reduce_function(k, g) for k,g in groupSplit.items()],PairMultiset())
            MRSimulator.simulate_delay(len(output))
            return output

    @staticmethod
    def shuffle(all_map_output: PairMultiset, num_reducer):
        # create groups
        groups = {}
        for k,v in all_map_output:
            same_key_group = groups.get(k) or SameKeyGroup(k)
            same_key_group.add((k,v))
            groups[k] = same_key_group

        # split
        group_list = list(groups.items())
        groups_splits = [{} for i in range(0, num_reducer)]
        for i in range(0,len(group_list)):
            k,v = group_list[i]
            groups_splits[i % num_reducer].update({k:v})

        MRSimulator.simulate_delay(len(all_map_output)*2)

        return groups_splits

    def execute(self, input, map, reduce):

        mapper = MRSimulator.Mapper(map)
        reducer = MRSimulator.Reducer(reduce)

        if not isinstance(input, PairMultiset):
            raise ValueError("Input must be a PairMultiset instance")
        input_splits = input.split(self.num_mapper)

        # Simulated map step
        mappers = [ self.mapperPool.apply_async(mapper.simulate_mapper,(split,)) for split in input_splits ]
        all_map_results = sum([m.get() for m in mappers],PairMultiset())

        # Simulated shuffle step
        group_splits = MRSimulator.shuffle(all_map_results,self.num_reducer)

        # Simulated reducer step
        reducers = [ self.mapperPool.apply_async(reducer.simulate_reducer,(group_split,)) for group_split in group_splits]
        output = sum([r.get() for r in reducers],PairMultiset())

        return output
