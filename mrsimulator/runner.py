from .datastructures import *
from multiprocessing.pool import ThreadPool

class MRSimulator:


    def __init__(self, num_mapper=1, num_reducer=1):
        self.num_mapper = num_mapper
        self.num_reducer = num_reducer
        self.mapperPool = ThreadPool(processes=num_mapper)
        self.reducerPool = ThreadPool(processes=num_reducer)

    @staticmethod
    def simulate_delay(collection):
        from time import sleep
        sleep(collection)

    @staticmethod
    def simulate_mapper(self, inputSplit, map):
        output = sum([map(k, v) for k, v in inputSplit])
        MRSimulator.simulate_delay(output)
        return output

    @staticmethod
    def simulate_reducer(self, groupSplit, reduce):
        output = sum([reduce(k, g) for k,g in groupSplit])
        MRSimulator.simulate_delay(output)
        return output

    @staticmethod
    def shuffle(all_map_output: PairMultiset):
        groups = {}
        for k,v in all_map_output:
            same_key_group = groups.get(k) or SameKeyGroup(k.__class__,v.__class__,k)
            same_key_group.add((k,v))
            groups[k] = same_key_group
        return groups

    def execute(self, input, map, reduce):

        if not isinstance(input, PairMultiset):
            raise ValueError("Input must be a PairMultiset instance")
        input_splits = input.split(self.num_mapper)

        # Simulated map step
        mappers = [ self.mapperPool.apply_async(MRSimulator.simulate_mapper,split,map) for split in input_splits ]
        all_map_results = sum([m.get() for m in mappers])

        # Simulated shuffle step
        groups = list(MRSimulator.shuffle(all_map_results).items())
        groups_splits = [{} for i in range(0,self.num_reducer)]
        for i in len(groups):
            groups_splits[i % self.num_reducer].update(groups[i])

        # Simulated reducer step
        reducers = [ self.mapperPool.apply_async(MRSimulator.simulate_reducer,group_split,reduce) for group_split in group_splits]
        output = sum([r.get() for r in reducers])

        return output
