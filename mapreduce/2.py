from jobs.callers import run_job
from jobs.callers import load_table
from jobs.prepare_tables import PrepareTablesJob
from jobs.repartition_join import RepartitionJoinJob
from jobs.aggregate import AggregateJob
from jobs.select import SelectJob
from jobs._printer import print_table
from functions import agg_func2
from functions import where2

def main():
    databases = ["warehouse/procurements", "warehouse/products", "warehouse/prpr_map"]
    run_job(PrepareTablesJob(), databases, "prepared")

    run_job(RepartitionJoinJob("prpr_map.prod_id = products.id"), ["prepared"], "joined", True)
    run_job(RepartitionJoinJob("__result__.prpr_map.proc_id = procurements.id"), ["prepared", "joined"], "joined2", True)
    run_job(AggregateJob(agg_func2, "summary", "__result__.procurements.company"), ["joined2"], "aggregated", True)
    run_job(SelectJob("__result__", ["__result__.summary"], where2), ["aggregated"], "final", False)
    print_table(load_table("/user/shakalyan/final/part-00000", "__result__"))

main()