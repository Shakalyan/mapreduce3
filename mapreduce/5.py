from jobs.callers import run_job
from jobs.callers import load_table
from jobs.prepare_tables import PrepareTablesJob
from jobs.replicated_join import ReplicatedJoinJob
from jobs.aggregate import AggregateJob
from jobs.select import SelectJob
from jobs._printer import print_table
from functions import where5


def main():
    databases = ["warehouse/procurements", "warehouse/products", "warehouse/prpr_map"]
    run_job(PrepareTablesJob(), databases, "prepared")

    run_job(SelectJob("products", ["*"], where5), ["prepared"], "final", False)
    print_table(load_table("/user/shakalyan/final/part-00000", "__result__"))
    

main()