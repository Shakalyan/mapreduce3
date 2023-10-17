import sys
from jobs._utils import Job
from jobs._utils import retrieve_tag_and_tuple
from jobs._utils import add_result_table_prefix

def groupby_is_none(groupby_key):
    return groupby_key.split('.')[1] == 'None'

def get_groupby_table(groupby_key):
    return groupby_key.split('.')[0]
        

class AggregateJob(Job):
    def __init__(self, aggregate_func, result_field_name, groupby_key):
        self.aggregate_func = aggregate_func
        self.result_field_name = result_field_name
        self.groupby_key = groupby_key
    
    def map(self):
        for line in sys.stdin:
            if len(line.strip()) == 0:
                continue
            
            tag, tuple = retrieve_tag_and_tuple(line)
            tuple = eval(tuple)
            if groupby_is_none(self.groupby_key) and tag == get_groupby_table(self.groupby_key):
                print(f"None\t{line}")
            if self.groupby_key in tuple:
                print(f"{tuple[self.groupby_key]}\t{line}")
    
    def reduce(self):
        curr_grpby_v = None
        agg_tuples = []
        for line in sys.stdin:
            if len(line.strip()) == 0:
                continue

            grpby_v, record = line.strip().split('\t')
            if curr_grpby_v is None:
                curr_grpby_v = grpby_v
            
            if curr_grpby_v != grpby_v:
                agg_result = self.aggregate_func(agg_tuples)
                result_tuple = {}
                result_tuple[self.groupby_key] = curr_grpby_v
                result_tuple[self.result_field_name] = agg_result
                result_tuple = add_result_table_prefix(result_tuple)
                print(f"__result__ {result_tuple}")
                curr_grpby_v = grpby_v
                agg_tuples.clear()
            
            tag, tuple = retrieve_tag_and_tuple(record)
            tuple = eval(tuple)
            agg_tuples.append(tuple)

        agg_result = self.aggregate_func(agg_tuples)
        result_tuple = {}
        if not groupby_is_none(self.groupby_key):
            result_tuple[self.groupby_key] = curr_grpby_v
        result_tuple[self.result_field_name] = agg_result
        result_tuple = add_result_table_prefix(result_tuple)
        print(f"__result__ {result_tuple}")



