import sys
import os


class Job:
    def map():
        pass
    def reduce():
        pass


def _hadoop_run(inputs, output, dependencies, srlzd_job, reducer = False):
    cmd = f"hadoop fs -rm -r {output}/; " \
           "yarn jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar "
    for input in inputs:
        cmd += f"-input {input}/ "
    cmd += f"-output {output}/ "
    cmd += f"-file jobs/_runner.py "

    for dp in dependencies:
        cmd += f"-file {dp} "

    cmd += f"-mapper \"./_runner.py {srlzd_job} map\" "
    if reducer:
        cmd += f"-reducer \"./_runner.py {srlzd_job} reduce\" "
    
    os.system(cmd)    


def add_result_table_prefix(tuple):
    prefix = "__result__"
    new_tuple = {}
    for key in tuple.keys():
        if key.split('.')[0] != prefix:
            new_tuple[f"{prefix}.{key}"] = tuple[key]
        else:
            new_tuple[key] = tuple[key]
    return new_tuple


def retrieve_tag_and_tuple(tagged_tuple):
    space_idx = tagged_tuple.find(' ')
    tag = tagged_tuple[:space_idx]
    tuple = tagged_tuple[space_idx+1:]
    return tag, tuple


def agg_func(values):
    return str(sum(map(int, values)))