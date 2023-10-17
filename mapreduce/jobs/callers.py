import pickle, base64, pyhdfs, os
from jobs._utils import _hadoop_run
from jobs.prepare_tables import PrepareTablesJob
from jobs._utils import Job
from jobs._utils import retrieve_tag_and_tuple


def run_job(job: Job, inputs, output, reducer = False):
    print(os.path.realpath(__file__))
    srlzd_job = base64.b64encode(pickle.dumps(job)).decode('ascii')
    _hadoop_run(inputs, output, ["./jobs/_utils.py"], srlzd_job, reducer)


def delete_result_prefix(tuple):
    new_tuple = {}
    for key in tuple.keys():
        tokens = key.split('.')
        if tokens[0] == '__result__':
            new_key = '.'.join(tokens[1:])
            new_tuple[new_key] = tuple[key]
        else:
            new_tuple[key] = tuple[key]
    return new_tuple


def load_table(path, tablename):
    res = []
    client = pyhdfs.HdfsClient()
    with client.open(path) as file:
        lines = file.read().decode('utf-8').split('\n')
        for line in lines:
            tag, tuple = retrieve_tag_and_tuple(line)
            if tag == tablename:
                if tablename == '__result__':
                    res.append(delete_result_prefix(eval(tuple)))
                else:
                    res.append(eval(tuple))
    return res