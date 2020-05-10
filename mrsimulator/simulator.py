

from .datastructures import *

# void map
def map(key, value):
    return PairMultiset(keyType=key.__class__, value=value.__class__).add((key,value))

# void reduce
def reduce(key, group: SameKeyGroup):
    return group
