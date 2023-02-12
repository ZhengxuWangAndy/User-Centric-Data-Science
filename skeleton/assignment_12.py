from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function

import csv
from itertools import islice
import logging
from enum import Enum
from operator import ne
from typing import List, Tuple
import uuid

import ray

import argparse

from collections import defaultdict

# Note (john): Make sure you use Python's logger to log
#              information about your program
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()

# Partition strategy enum
class PartitionStrategy(Enum):
    RR = "Round_Robin"
    HASH = "Hash_Based"

# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """
    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator

    # Returns the lineage of self
    def lineage(self) -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self, att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Returns the How-provenance of self
    def how(self) -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass



# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    def __init__(self,
                 id=None,
                 name=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        self.pull = pull
        self.partition_strategy = partition_strategy
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self) -> List[ATuple]:
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def apply(self, tuples: List[ATuple]) -> bool:
        logger.error("Apply method is not implemented!")


class Sink(Operator):
    """Sink operator"""
    def __init__(self,
                result=[],
                track_prov=False,
                propagate_prov=False,
                pull=True,
                partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Sink, self).__init__(name="Sink",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        self.result = result
    
    def apply(self, tuples: List[ATuple]):
        if tuples != []  and tuples != [{}]:
            self.result += tuples

    def get_result(self):
        return list(set(self.result))


# Scan operator
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
     # Initializes scan operator
    def __init__(self,
                 filepath,
                 outputs : List[Operator],
                 filter=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Scan, self).__init__(name="Scan",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.batchSize = 10000
        self.head = 0 - self.batchSize
        self.allTuple = []
        self.outputs = outputs
        self.data = []
        self.reader = self.readf(filepath, self.batchSize)
        self.filter = filter

        

    def readf(self, filepath, batchSize):
        batch_temp = []
        with open(filepath, newline="") as file:
            freader = csv.reader(file, delimiter=' ')
            for line in islice(freader, 1, None):
                if self.filter is None:
                    batch_temp.append(ATuple([int(item) for item in line]))
                else:
                    if self.filter[0](line, self.filter[1], self.filter[2]):
                        batch_temp.append(ATuple([int(item) for item in line]))
                if len(batch_temp) == batchSize:
                    # batch = copy.deepcopy(batch_temp)
                    yield batch_temp
                    batch_temp.clear()
            yield batch_temp

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        # YOUR CODE HERE

        try:
            return next(self.reader)
        except StopIteration:
            return None


    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Starts the process of reading tuples (only for push-based evaluation)
    def start(self):
        operator = self.outputs[0]    
        
        try:
            while True:
                self.data = next(self.reader)
                operator.apply(self.data)
        except StopIteration:
            self.data = None
            return None
    
    def get_data(self):
        return self.data


        

# Equi-join operator
class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_inputs (List): A list of handles to the instances of the operator
        that produces the left input.
        right_inputs (List):A list of handles to the instances of the operator
        that produces the right input.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes join operator
    def __init__(self,
                 left_inputs : List[Operator],
                 right_inputs : List[Operator],
                 outputs : List[Operator],
                 left_join_attribute,
                 right_join_attribute,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Join, self).__init__(name="Join",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.left_inputs = left_inputs
        self.right_inputs = right_inputs
        self.left_join_attribute = left_join_attribute
        self.right_join_attribute = right_join_attribute
        self.outputs = outputs
        self.right_Dict = defaultdict(list)        

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        left_Dict = defaultdict(list)
        batch_left_input = self.left_inputs[0].get_next()
        batch_right_input = self.right_inputs[0].get_next()

        if batch_left_input == None and batch_right_input == None:
            return None

        batch_result = []
        
        # add left, check right table 
        if batch_left_input != None:
            for tuple in batch_left_input:
                left_Dict[tuple.tuple[self.left_join_attribute]].append(tuple)

        # if batch_right_input != None:
        #     for tuple in batch_right_input:
        #         self.right_Dict[tuple.tuple[self.right_join_attribute]].append(tuple)
        
        while batch_right_input != None:

            for tuple in batch_right_input:
                self.right_Dict[tuple.tuple[self.right_join_attribute]].append(tuple)
        
            batch_right_input = self.right_inputs[0].get_next()
        

        for tuple in batch_left_input:
            if tuple.tuple[self.left_join_attribute] in self.right_Dict.keys():
                for temp in self.right_Dict[tuple.tuple[self.left_join_attribute]]:
                    batch_result.append(ATuple(tuple.tuple + temp.tuple))

        # if batch_right_input != None:
        #     for tuple in batch_right_input:
        #         if tuple.tuple[self.right_join_attribute] in self.left_Dict.keys():
        #             for temp in self.left_Dict[tuple.tuple[self.right_join_attribute]]:
        #                 batch_result.append(ATuple(tuple.tuple + temp.tuple))

        # if batch_left_input != None:
        #     for tuple in batch_left_input:
        #         if tuple.tuple[self.left_join_attribute] in self.right_Dict.keys():
        #             for temp in self.right_Dict[tuple.tuple[self.left_join_attribute]]:
        #                 batch_result.append(ATuple(tuple.tuple + temp.tuple))

        # if batch_result != []:
        return batch_result

        # return None
        

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # left_batch = self.left_inputs.get_data()
        # right_batch = self.right_inputs.get_data()
            
        # # Hash Join
        # if left_batch and right_batch:
        #     joined = []
        #     h = defaultdict(list)
        #     for s in left_batch:    
        #         h[s.tuple[ self.left_join_attribute]].append(s.tuple)
        #     joined += [ATuple(s + r.tuple) for r in right_batch for s in h[r.tuple[self.right_join_attribute]]]
        #     print("Joined: " + str(len(joined)))
        #     self.outputs[0].apply(joined)
        

        self.right_Dict = defaultdict(list)
        batch_left_input = self.left_inputs.get_data()
        batch_right_input = self.right_inputs.get_data()

        batch_result = []
        
        # add left, check right table 
        # if batch_left_input != None:
        #     for tuple in batch_left_input:
        #         self.left_Dict[tuple.tuple[self.left_join_attribute]].append(tuple)

        if batch_right_input != None:
            for tuple in batch_right_input:
                self.right_Dict[tuple.tuple[self.right_join_attribute]].append(tuple)

        
        for tuple in batch_left_input:
            if tuple.tuple[self.left_join_attribute] in self.right_Dict.keys():
                for temp in self.right_Dict[tuple.tuple[self.left_join_attribute]]:
                    batch_result.append(ATuple(tuple.tuple + temp.tuple))
        
        self.outputs[0].apply(batch_result)


# Project operator
class Project(Operator):
    """Project operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes project operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[None],
                 fields_to_keep=[],
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Project, self).__init__(name="Project",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.fields_to_keep = fields_to_keep
        self.outputs = outputs

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        batch_input = self.inputs[0].get_next()
        if batch_input == None:
            return None
        if self.fields_to_keep == []:
            return batch_input
        result = []
        if len(self.fields_to_keep) == 2:
            for tuple in batch_input:
                result.append(ATuple(tuple.tuple[self.fields_to_keep[0]:self.fields_to_keep[1] + 1]))

        if len(self.fields_to_keep) == 1:
            for tuple in batch_input:
                result.append(ATuple(tuple.tuple[self.fields_to_keep[0]]))
        
        return result


    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        result = []
        if self.fields_to_keep == []:
            result =  tuples
        if len(self.fields_to_keep) == 2:
            for tuple in tuples:
                result.append(ATuple(tuple.tuple[self.fields_to_keep[0]:self.fields_to_keep[1] + 1]))

        if len(self.fields_to_keep) == 1:
            for tuple in tuples:
                result.append(ATuple(tuple.tuple[self.fields_to_keep[0]])) 
        self.outputs[0].apply(result)

# Group-by operator
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes average operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 key,
                 value,
                 agg_fun,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(GroupBy, self).__init__(name="GroupBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.key = key
        self.value = value
        self.agg_fun = agg_fun

        self.allTuples = []
        

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        temp = []
        self.allTuples = []
        while True:
            temp = self.inputs[0].get_next()
            if temp == None:
                break
            self.allTuples += temp
        if self.allTuples == []:
            return None

        aggregateDict = {}
        for tuple in self.allTuples:
            temp = str(tuple.tuple[self.key])
            if temp not in aggregateDict.keys():
                aggregateDict[temp] = []
            aggregateDict[temp].append(tuple.tuple[self.value])
            
        result = []
        for key in aggregateDict.keys():
            aggregateDict[key] = self.agg_fun(aggregateDict[key])
            result.append(ATuple([key,aggregateDict[key]]))
        if result != []:
            return result
        return None


    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):

        aggregateDict = {}
        for tuple in tuples:
            temp = str(tuple.tuple[self.key])
            if temp not in aggregateDict.keys():
                aggregateDict[temp] = []
            aggregateDict[temp].append(tuple.tuple[self.value])
            
        result = []
        for key in aggregateDict.keys():
            aggregateDict[key] = self.agg_fun(aggregateDict[key])
            result.append(ATuple([key,aggregateDict[key]]))
        
        self.outputs[0].apply(result)


# Custom histogram operator
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes histogram operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 key,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Histogram, self).__init__(name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov,
                                        pull=pull,
                                        partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.key = key
        self.outputs = outputs
        self.groups = {}

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        allTuples = []
        temp = self.inputs[0].get_next()
        if temp == None:
            return None
        while temp != None:
            allTuples += temp
            temp = self.inputs[0].get_next()
        
        keySet = set()
        for tuple in allTuples:
            keySet.add(str(tuple.tuple[self.key]))
        
        result = {}
        for key in keySet:
            result[key] = 0
        for tuple in allTuples:
            result[str(tuple.tuple[self.key])] += 1

        result = sorted(result.items(), key=lambda item:item[1], reverse=True)
        return result

        

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # keySet = set()
        # for tuple in tuples:
        #     keySet.add(str(tuple.tuple[self.key]))
        
        # result = {}
        # for key in keySet:
        #     result[key] = 0
        # for tuple in tuples:
        #     result[str(tuple.tuple[self.key])] += 1

        # self.outputs[0].apply([result])

        for row in tuples:
            if row.tuple[self.key] not in self.groups:
                self.groups[row.tuple[self.key]] = 1
            else:
                self.groups[row.tuple[self.key]] += 1
        histogram = sorted(self.groups.items())
        self.outputs[0].apply(histogram)

# Order by operator
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes order-by operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 comparator,
                 sortKeyIndex,
                 ASC=True,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(OrderBy, self).__init__(name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.comparator = comparator
        self.ASC = ASC
        self.sortKeyIndex = sortKeyIndex
        self.allTuples = []

    # Returns the sorted input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        temp = self.inputs[0].get_next()
        if temp == None:
            return None
        
        while True:
            if temp == None:
                break
            self.allTuples += temp
            temp = self.inputs[0].get_next()
        
        return self.comparator(self.allTuples, self.ASC, self.sortKeyIndex)


    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        
        result =  self.comparator(tuples, self.ASC, self.sortKeyIndex)
        self.outputs[0].apply(result)


# Top-k operator
class TopK(Operator):
    """TopK operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes top-k operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 k=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(TopK, self).__init__(name="TopK",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.k = k

        self.allTuples = []

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        temp = self.inputs[0].get_next()
        while temp != None:
            self.allTuples += temp
            temp = self.inputs[0].get_next()
            if len(self.allTuples) > self.k:
                return self.allTuples[:self.k]
        return self.allTuples


    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        result = tuples[:self.k]
        self.outputs[0].apply(result)


# Filter operator
class Select(Operator):
    """Select operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """
    # Initializes select operator
    def __init__(self,
                 inputs : List[Operator],
                 outputs : List[Operator],
                 predicate,
                 tupleIndex,
                 id,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy : PartitionStrategy = PartitionStrategy.RR):
        super(Select, self).__init__(name="Select",
                                     track_prov=track_prov,
                                     propagate_prov=propagate_prov,
                                     pull=pull,
                                     partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.inputs = inputs
        self.outputs = outputs
        self.predicate = predicate
        self.tupleIndex = tupleIndex
        self.id = id
        self.data = []

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        selectInput = self.inputs[0].get_next()
        if selectInput == None:
            return None
        
        return self.predicate(self.tupleIndex, self.id, selectInput)



    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):

        result = self.predicate(self.tupleIndex, self.id, tuples)
        self.data = result
        self.outputs[0].apply(result)
    
    def get_data(self):
        return self.data




# select tuple[tupleIndex] == id
def predicate(tupleIndex, id, batchTuple):
    result = []
    try:
        for tuple in batchTuple:
            if tuple.tuple[tupleIndex] == id:
                result.append(tuple)
        return result
    except:
        return None

# comput the list average of batchTuple[tupleIndex]
def AVG(numList):
    sum = 0
    try:
        for i in numList:
            sum += i
        return sum / len(numList)
    except:
        return None

def comparator(tupleList, ASC, tupleIndex):
    if ASC == True:
        result = sorted(tupleList, key=(lambda tuple:tuple.tuple[tupleIndex]))
    else:
        result = sorted(tupleList, key=(lambda tuple:tuple.tuple[tupleIndex]), reverse=True)

    return result

def AVG_push(tuples, index):
    numList = [int(a.tuple[index]) for a in tuples]
    return [sum(numList)/len(numList)]

def filter(line,id,index):
    return line[index] == str(id)


if __name__ == "__main__":



    logger.info("Assignment #1")

    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    parser = argparse.ArgumentParser()
    parser.add_argument("--query",type=int)
    parser.add_argument("--ff", type=str)
    parser.add_argument("--mf",type=str)
    parser.add_argument("--uid",type=int)
    parser.add_argument("--mid",type=int)
    parser.add_argument("--pull",type=int,default=0)
    parser.add_argument("--output",type=str)
    args = parser.parse_args()

    if args.query == 1 and args.pull == 1:
        F = Scan(filepath=args.ff, outputs=[],filter=[filter,args.uid,0])
        R = Scan(filepath=args.mf, outputs=[],filter=[filter,args.mid,1])
        selectedF = Select([F],[],predicate,0,args.uid)
        selectedR = Select([R],[],predicate,1,args.mid)
        joined = Join([selectedF],[selectedR],[],1,0)
        grouped = GroupBy([joined],[],3,4,AVG)
        # while True:
        temp = grouped.get_next()
            # if temp == None:
                # break
        with open(args.output,'w') as f:
            f.write("Average\n")
            for item in temp:
                f.write(str(item.tuple[1]))
                f.write('\n')
                logger.info(item.tuple)

    # task1 push based
    if args.query == 1 and args.pull == 0:

        sink = Sink(result=[])
        joined = Join([], [], [sink], 1, 0)
        selected_F = Select([], [joined],predicate,0,args.uid)
        joined.left_inputs = selected_F
        selected_R = Select([], [joined],predicate,1,args.mid)
        joined.right_inputs = selected_R
        F = Scan(filepath=args.ff,outputs=[selected_F],filter=[filter,args.uid,0])
        R = Scan(filepath=args.mf, outputs=[selected_R],filter=[filter,args.mid,1])
        F.start()
        R.start()
        temp = AVG_push(sink.get_result(), 4)
        with open(args.output,'w') as f:
            f.write("Average\n")
            for item in temp:
                f.write(str(item))
                f.write('\n')
                logger.info(item)


    # TASK 2: Implement recommendation query for User A
    #
    # SELECT R.MID
    # FROM ( SELECT R.MID, AVG(R.Rating) as score
    #        FROM Friends as F, Ratings as R
    #        WHERE F.UID2 = R.UID
    #              AND F.UID1 = 'A'
    #        GROUP BY R.MID
    #        ORDER BY score DESC
    #        LIMIT 1 )

    # YOUR CODE HERE
    if args.query == 2 and args.pull == 1:
        F = Scan(filepath=args.ff, outputs=[],filter=[filter,args.uid,0])
        R = Scan(filepath=args.mf, outputs=[],filter=[filter,args.mid,1])
        selectedF = Select([F],[],predicate,0,args.uid)
        joined = Join([selectedF],[R],[],1,0)
        projected = Project([joined],[],[3,4])
        grouped = GroupBy([projected],[],0,1,AVG)
        ordered = OrderBy([grouped],[],comparator,1,False)
        limited = TopK([ordered],[],1)
        projectResult = Project([limited],[],[0])
        with open(args.output,'w') as f:
            f.write("MID\n")
            for item in projectResult.get_next():
                f.write(str(item.tuple))
                f.write('\n')
                logger.info(item.tuple)


 # task2 push
    if args.query == 2 and args.pull == 0:

        sink = Sink(result=[])
        projected = Project([],[sink],[0])
        limited = TopK([],[projected],1)
        ordered = OrderBy([],[limited],comparator,1,False)
        grouped = GroupBy([],[ordered],0,1,AVG)
        projected = Project([],[grouped],[3,4])
        joined = Join([], [], [projected], 1, 0)
        selected_F = Select([], [joined],predicate,0,args.uid)
        R = Scan(filepath=args.mf, outputs=[joined],filter=[filter,args.mid,1])
        joined.right_inputs = R
        F = Scan(filepath=args.ff,outputs=[selected_F],filter=[filter,args.uid,0])
        joined.left_inputs = selected_F
        F.start()
        R.start()
        temp = sink.get_result()
        with open(args.output,'w') as f:
            f.write("MID\n")
            for item in temp:
                f.write(str(item.tuple))
                f.write('\n')
                logger.info(item.tuple)

    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    if args.query == 3 and args.pull == 1:
        F = Scan(filepath=args.ff,outputs=[],filter=[filter,args.uid,0])
        R = Scan(filepath=args.mf,outputs=[],filter=[filter,args.mid,1])
        selectedF = Select([F],[],predicate,0,args.uid)
        selectedR = Select([R],[],predicate,1,args.mid)
        joined = Join([selectedF],[selectedR],[],1,0)
        projected= Project([joined],[],[2,4])
        histogtamed = Histogram([projected],[],2)

        temp = histogtamed.get_next()
        with open(args.output,'w') as f:
            f.write("Rating,count\n")
            for item in temp:
                f.write(str(int(item[0])) + ',' + str(item[1]))
                f.write('\n')
                logger.info(item)

    if args.query == 3 and args.pull == 0:

        # task3 push
        sink = Sink(result=[])
        histogtamed = Histogram([],[sink],2)
        projected = Project([],[histogtamed],[2,4])
        joined = Join([],[],[projected],1,0)
        selectedF = Select([],[joined],predicate,0,args.uid)
        joined.left_inputs = selectedF
        selectedR = Select([],[joined],predicate,1,args.mid)
        joined.right_inputs = selectedR
        F = Scan(filepath=args.ff,outputs=[selectedF],filter=[filter,args.uid,0])
        R = Scan(filepath=args.mf, outputs=[selectedR],filter=[filter,args.mid,1])
        F.start()
        R.start()
        temp = sink.get_result()
        with open(args.output,'w') as f:
            f.write("Rating,count\n")
            for item in temp:
                f.write(str(int(item[0])) + ',' + str(item[1]))
                f.write('\n')
                logger.info(item)
        



    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'


    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
