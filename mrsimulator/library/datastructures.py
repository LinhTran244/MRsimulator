class PairMultiset:

    def __init__(self, pairlist = None):
        import collections
        self.counter = collections.Counter()
        if pairlist and isinstance(pairlist, list):
            self.add(pairlist)

    def __iter__(self):
        return self.to_list().__iter__()

    def __add__(self, other):

        # if it is a list, try to add it as elements
        if isinstance(other,list):
            return PairMultiset(self.to_list() + other)

        # if it is a tuple, clone and add
        if isinstance(other,tuple):
            return PairMultiset(self.to_list()).add(other)

        # if it is PairMultiset, try to merge it
        if isinstance(other, PairMultiset):
            return PairMultiset(self.to_list() + other.to_list())
        else:
            raise ValueError("Expected a PairMultiset, received a %s", other.__class__.__name__)

    def __eq__(self, other):
        if isinstance(other, PairMultiset):
            return self.counter == other.counter
        else:
            return False

    def __str__(self):
        return "Multiset:{dict}".format(dict=self.to_list())

    def __repr__(self):
        return self.__str__()

    def add(self, element):

        if isinstance(element, list):
            for i in element:
                self.add(i)
            return self

        if isinstance(element ,tuple) and len(element) == 2: # check if is a pair
            self.counter.update({element:1})
            return self
        else:
            raise ValueError("It can only accept a pair (tuple of length 2). Current type: %s, Current length: %s" % (element.__class__.__name__, len(element)))

    def to_list(self):
        return [k for k in self.counter.keys() for i in range(0 ,self.counter.get(k))]

    def split(self,num_split):
        output = [PairMultiset() for i in range(0,num_split)]
        elements = self.to_list()
        for i in range(0,len(elements)):
            output[i % num_split].add(elements[i])
        return output

    def __len__(self):
        return len(self.to_list())

class SameKeyGroup(PairMultiset):

    def __init__(self, key):
        super().__init__()
        self.key = key

    def add(self, element):
        if isinstance(element ,tuple) and len(element) == 2: # check if is a pair
            k ,v = element
            if k == self.key:
                return super().add(element)
            else:
                raise ValueError("Only elements with key %s can be added" ,self.key)
