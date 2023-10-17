import sys
from copy import deepcopy
from jobs._utils import Job
from jobs._utils import retrieve_tag_and_tuple
from jobs._utils import add_result_table_prefix

class ReplicatedJoinJob(Job):
    def __init__(self, joinkey, right_tuples):
        self.right_tuples = right_tuples
        self.joinkey = joinkey

    def map(self):
        left_key, right_key = self.joinkey.split(" = ")
        left_table = left_key.split(".")[0]

        left_tuples = []

        for line in sys.stdin:
            if len(line.strip()) == 0:
                continue
            
            tag, tuple = retrieve_tag_and_tuple(line)
            
            if tag != left_table:
                continue
            
            left_tuples.append(eval(tuple))
        
        for ltuple in left_tuples:
            for rtuple in self.right_tuples:
                if ltuple[left_key] == rtuple[right_key]:
                    ltuple_cpy = deepcopy(ltuple)
                    for key in rtuple.keys():
                        ltuple_cpy[key] = rtuple[key]
                    print(f"__result__ {add_result_table_prefix(ltuple_cpy)}")