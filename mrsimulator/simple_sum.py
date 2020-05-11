
from mrsimulator.library.simulator import MRSimulator
from mrsimulator.library.datastructures import *

def map(key, value):
    return key, value

def reduce(key, group: SameKeyGroup):
    sum = 0
    for k,v in group:
        sum += v
    return 1, sum

if __name__ == '__main__':
    raw_data = range(0,10)
    pairs = PairMultiset([(1,v) for v in raw_data])
    print(MRSimulator().execute(pairs,map,reduce))
