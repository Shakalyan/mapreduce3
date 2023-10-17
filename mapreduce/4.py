from jobs.callers import run_job
from jobs.callers import load_table
from jobs.prepare_tables import PrepareTablesJob
from jobs.replicated_join import ReplicatedJoinJob
from jobs.aggregate import AggregateJob
from jobs.select import SelectJob
from jobs._printer import print_table
from functions import agg_func4
from functions import where4


def main():
    databases = ["warehouse/procurements", "warehouse/products", "warehouse/prpr_map"]
    run_job(PrepareTablesJob(), databases, "prepared")

    # max_table
    run_job(AggregateJob(agg_func4, "price", "products.None"), ["prepared"], "max_table_aggregated", True)
    max_price_tuple = load_table("/user/shakalyan/max_table_aggregated/part-00000", "__result__")

    # main
    run_job(ReplicatedJoinJob("products.price = price", max_price_tuple), ["prepared"], "joined", False)
    run_job(SelectJob("__result__", ["__result__.products.name", "__result__.products.price"], where4), ["joined"], "final", False)
    print_table(load_table("/user/shakalyan/final/part-00000", "__result__"))
    

main()