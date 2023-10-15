from jobs.callers import run_job
from jobs.prepare_tables import PrepareTablesJob
from jobs.repartition_join import RepartitionJoinJob
from jobs._utils import retrieve_tag_and_tuple
import pyhdfs

def print_separator(columns_max):
    for mx in columns_max:
        print('+', end='')
        print('-'*(mx+2), end='')
    print('+')

def print_data(columns_max, data):
    for i in range(len(data)):
        print('| ', end='')
        print(data[i], end='')
        print(' '*(columns_max[i] - len(data[i])+1), end='')
    print('|')

def delete_result_prefix(tuple):
    new_tuple = {}
    for key in tuple.keys():
        new_key = '.'.join(key.split('.')[1:])
        new_tuple[new_key] = tuple[key]
    return new_tuple

def print_result(hdfs_path):
    client = pyhdfs.HdfsClient()
    with client.open(hdfs_path) as resf:
        res = resf.read().decode('utf-8')
        lines = res.split('\n')[:-1]
        table, tuple = retrieve_tag_and_tuple(lines[0])
        tuple = delete_result_prefix(eval(tuple))
        columns = []
        columns_max = []
        for key in tuple.keys():
            columns.append(key)
            columns_max.append(len(key))
        
        for line in lines:
            table, tuple = retrieve_tag_and_tuple(line)
            tuple = delete_result_prefix(eval(tuple))
            for i in range(len(columns)):
                columns_max[i] = max(columns_max[i], len(tuple[columns[i]]))
        
        print_separator(columns_max)
        print_data(columns_max, columns)
        print_separator(columns_max)

        for line in lines:
            table, tuple = retrieve_tag_and_tuple(line)
            tuple = delete_result_prefix(eval(tuple))
            values = []
            for column in columns:
                values.append(tuple[column])
            print_data(columns_max, values)
        
        print_separator(columns_max)
                


def main():
    databases = ["warehouse/procurements", "warehouse/products", "warehouse/prpr_map"]
    run_job(PrepareTablesJob(), databases, "prepared")
    run_job(RepartitionJoinJob("products.id = prpr_map.prod_id"), ["prepared"], "joined", True)
    run_job(RepartitionJoinJob("__result__.prpr_map.proc_id = procurements.id"), ["prepared", "joined"], "joined2", True)
    print_result('/user/shakalyan/joined2/part-00000')

main()