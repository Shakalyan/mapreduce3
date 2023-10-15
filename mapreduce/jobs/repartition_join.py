import sys
from copy import deepcopy
from jobs._utils import Job
from jobs._utils import add_result_table_prefix
from jobs._utils import retrieve_tag_and_tuple

class RepartitionJoinJob(Job):
    def __init__(self, joinkey):
        self.joinkey = joinkey

    def map(self):
        for line in sys.stdin:
            if len(line.strip()) == 0:
                continue
            tag, tuple = retrieve_tag_and_tuple(line.strip())
            print(f"{self.joinkey}\t{tag} {tuple}")
    
    def reduce(self):
        left_key, right_key = self.joinkey.split(" = ")
        left_table = left_key.split(".")[0]
        right_table = right_key.split(".")[0]

        tuple_map = {}
        tuple_map[left_table] = []
        tuple_map[right_table] = []

        for line in sys.stdin:
            if len(line.strip()) == 0:
                continue
            
            tagged_tuple = line.strip().split('\t')[1]
            tag, tuple = retrieve_tag_and_tuple(tagged_tuple)
            
            if tag != left_table and tag != right_table:
                continue
            
            tuple_map[tag].append(eval(tuple))
        
        for ltuple in tuple_map[left_table]:
            for rtuple in tuple_map[right_table]:
                if ltuple[left_key] == rtuple[right_key]:
                    ltuple_cpy = deepcopy(ltuple)
                    for key in rtuple.keys():
                        ltuple_cpy[key] = rtuple[key]
                    print(f"__result__ {add_result_table_prefix(ltuple_cpy)}")
        