import sys
import os
from jobs._utils import Job

class PrepareTablesJob(Job):
    def __init__(self):
        self.tables = {}
        self.tables['products']     = ['id', 'name', 'price']
        self.tables['procurements'] = ['id', 'company']
        self.tables['prpr_map']     = ['prod_id', 'proc_id', 'amount']

    def map(self):
        file = os.environ["map_input_file"]
        table = file.split('/')[-2]
        for line in sys.stdin:
            if len(line.strip()) == 0:
                continue

            values = line.strip().split(',')
            rowMap = {}
            for i in range(len(values)):
                rowMap[f"{table}.{self.tables[table][i]}"] = values[i]
            
            print(f"{table} {rowMap}")
