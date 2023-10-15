import pickle, base64
from jobs._utils import _hadoop_run
from jobs.prepare_tables import PrepareTablesJob
from jobs._utils import Job

def run_job(job: Job, inputs, output, reducer = False):
    srlzd_job = base64.b64encode(pickle.dumps(job)).decode('ascii')
    _hadoop_run(inputs, output, ["./jobs/_utils.py"], srlzd_job, reducer)