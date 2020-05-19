# MRsimulator

This project is a python based MapReduce simulator built for educational purposes.
It doesn't have any other use than teach how the MapReduce design helps to achieve parallelism and distribution.

It should be noted that in Python the same results can be achieved on a single machine in a much more elegant and performant way.

## How to install

Requires Python 3.7+
Then simply clone the repository locally or download the source code.

## How to use it

Create a new python file and import the simulator package.
You can use the following skeleton to start:
```
from mrsimulator import MRSimulator, SameKeyGroup, PairMultiset

# your map function
# should return a pair or a list of pairs
def map(key, value):
    pass

# your reduce function
# should return a pair or a list of pairs
# the SameKeyGroup object is iterable as a list of pairs
# example
#   for k,v in group:
#    ... do something...

def reduce(key, group: SameKeyGroup):
    pass

# your main function
if __name__ == '__main__':
    # here you should parse the raw data, create the pairs multiset and then
    # and then launch the simuation
    # create 
    # example: 
    #   pairs = [(1,v) for v in range(0,10)]
    #   MRSimulator(num_mapper=1, num_reducer=1).execute(pairs,map,reduce)

```

Try to implement some algorithms using these constraints, and tweak the num_mappers and num_reducers to have different time results.
You can find some examples in the examples directory.

## How it works under the hood

This simulator doesn't pretend to capture all the complexity of cost functions for MapReduce.
Under the hood it implements the parallelism via a multithreading approach in which each thread is slowed down to exposed a behaviour similar to MapReduce performance in real distributed environment with some meaningfully large data sets.

### Advanced

If you wish to experiment with larger datasets, you can change the simulation cost factor by changing the MRSimulator.SLEEPING_FACTOR. It defaults at 0.1 seconds per operation