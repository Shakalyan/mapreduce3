from jobs.callers import run_job
from jobs.callers import load_table
from jobs.prepare_tables import PrepareTablesJob
from jobs.repartition_join import RepartitionJoinJob
from jobs.aggregate import AggregateJob
from jobs.select import SelectJob
from jobs._printer import print_table
from functions import agg_func1
from functions import where1


def main():
    databases = ["warehouse/procurements", "warehouse/products", "warehouse/prpr_map"]
    run_job(PrepareTablesJob(), databases, "prepared")

    run_job(RepartitionJoinJob("prpr_map.prod_id = products.id"), ["prepared"], "joined", True)
    run_job(AggregateJob(agg_func1, "sugar_amount", "__result__.products.name"), ["joined"], "aggregated", True)
    run_job(SelectJob("__result__", ["__result__.sugar_amount"], where1), ["aggregated"], "final", False)
    print_table(load_table("/user/shakalyan/final/part-00000", "__result__"))

main()