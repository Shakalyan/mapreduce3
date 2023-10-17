products = LOAD '/user/shakalyan/warehouse/products/000000_0' USING PigStorage(',') as (id:int, name:chararray, price:int);
prpr_map = LOAD '/user/shakalyan/warehouse/prpr_map/000000_0' USING PigStorage(',') as (prod_id:int, proc_id:int, amount:int);

joined   = JOIN products BY id, prpr_map BY prod_id;
filtered = FILTER joined BY name == 'Sugar';
grouped  = GROUP filtered ALL;
result   = FOREACH grouped GENERATE SUM(filtered.amount);
dump result;