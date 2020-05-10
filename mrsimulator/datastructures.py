class PairMultiset:

    from collections import Counter

    def __init__(self, keyType, valueType, counter=Counter()):
        self.counter = counter
        self.keyType = keyType
        self.valueType = valueType

    def __get_types__(self):
        return (self.keyType, self.valueType)

    def __iter__(self):
        return self.to_list().__iter__()

    def __add__(self, other):
        if isinstance(other, PairMultiset):
            if other.__get_types__() == self.__get_types__():
                return PairMultiset(self.counter + other.counter)
            else:
                raise ValueError("Impossible to add two PairMultiset of different types")
        else:
            raise ValueError("Expected a PairMultiset, received a %s", other.__name__)

    def __eq__(self, other):
        return isinstance(other ,PairMultiset) and\
               self.valueType == other.valueType and\
               self.keyType == other.keyType and\
               self.counter == other.counter

    def __str__(self):
        return "Multiset<{k},{v}>{dict}".format(k=self.keyType.__name__,
                                                v=self.valueType.__name__,
                                                dict=self.counter)

    def add(self, element):
        if isinstance(element ,tuple) and len(element) == 2: # check if is a pair
            k ,v = element
            if not isinstance(k ,self.keyType):
                raise ValueError("Wrong type for key. It only accepts %s" % self.keyType.__name__)
            if not isinstance(v ,self.valueType):
                raise ValueError("Wrong type for value. It only accepts %s" % self.valueType.__name__)
            self.counter.update({element :1})
        else:
            raise ValueError("It can only accept a pair (tuple of length 2)")

    def to_list(self):
        return [k for k in self.counter.keys() for i in range(0 ,self.counter.get(k))]

    def split(self,num_split):
        output = [PairMultiset(self.keyType, self.valueType) for i in range(0,num_split)]
        elements = self.to_list()
        for i in len(elements):
            output[i % num_split].add(elements[i])
        return output

    def __len__(self):
        return len(self.to_list())

class SameKeyGroup(PairMultiset):

    def __init__(self, keyType, valueType, key):
        super().__init__(keyType, valueType)
        if isinstance(key ,self.keyType):
            self.key = key
        else:
            raise ValueError("Wrong type for key. It only accepts %s" % self.keyType.__name__)


    def add(self, element):
        if isinstance(element ,tuple) and len(element) == 2: # check if is a pair
            k ,v = element
            if k == self.key:
                return super().add(element)
            else:
                raise ValueError("Only elements with key %s can be added" ,self.key)
