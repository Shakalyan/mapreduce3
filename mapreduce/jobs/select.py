import sys
from jobs._utils import Job
from jobs._utils import retrieve_tag_and_tuple
from jobs._utils import add_result_table_prefix

class SelectJob(Job):
    def __init__(self, table, columns, predicate):
        self.table = table
        self.columns = columns
        self.predicate = predicate
    
    def map(self):
        for line in sys.stdin:
            if len(line.strip()) == 0:
                continue
                
            tag, tuple = retrieve_tag_and_tuple(line)
            tuple = eval(tuple)
            if tag == self.table and self.predicate(tuple):
                tuple = add_result_table_prefix(tuple)
                if self.columns[0] == '*':
                    print(f"__result__ {tuple}")    
                else:
                    result_tuple = {}
                    for column in self.columns:
                        result_tuple[column] = tuple[column]
                    print(f"__result__ {result_tuple}")