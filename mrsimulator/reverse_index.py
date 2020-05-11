from mrsimulator.library.simulator import MRSimulator
from mrsimulator.library.datastructures import *

def map(key, value):
    words = value.split(" ")
    return [(value, key) for value in words]

def reduce(key, group: SameKeyGroup):
    index = set()
    for k,v in group:
        index.add(v)
    return k, index

if __name__ == '__main__':
    raw_data = "Some multiline\n"\
               "text taken\n"\
               "as an example of a text"
    lines = raw_data.split("\n")
    pairs = PairMultiset([(i, lines[i]) for i in range(0,len(lines))])
    MRSimulator().execute(pairs,map,reduce)
