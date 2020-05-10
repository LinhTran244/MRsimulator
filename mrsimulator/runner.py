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
    def simulate_reduce(self, ):


def shuffle(input: PairMultiset, output: SameKeyGroup):
        pass

    def execute(self, input, map, reduce):
        if not isinstance(input, PairMultiset):
            raise ValueError("Input must be a PairMultiset instance")
        inputSplits = input.split(self.num_mapper)

        async_result = pool.apply_async(foo, ('world', 'foo'))  # tuple of args for foo

        # do some other stuff in the main process
        return_val = async_result.get()  # get the return value from your function.

