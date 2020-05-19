
from mrsimulator import MRSimulator, SameKeyGroup, PairMultiset

def map(key, value):
    return key, value

def reduce(key, group: SameKeyGroup):
    sum = 0
    for k,v in group:
        sum += v
    return 1, sum

if __name__ == '__main__':
    raw_data = range(0,1000)
    pairs = PairMultiset([(1,v) for v in raw_data])
    MRSimulator(num_mapper=100, num_reducer=10).execute(pairs,map,reduce)
