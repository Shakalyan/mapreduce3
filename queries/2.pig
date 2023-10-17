products     = LOAD '/user/shakalyan/warehouse/products/000000_0' USING PigStorage(',') as (id:int, name:chararray, price:int);
prpr_map     = LOAD '/user/shakalyan/warehouse/prpr_map/000000_0' USING PigStorage(',') as (prod_id:int, proc_id:int, amount:int);
procurements = LOAD '/user/shakalyan/warehouse/procurements/000000_0' USING PigStorage(',') as (id:int, company:chararray);

joined   = JOIN products BY id, prpr_map BY prod_id;
joined   = JOIN joined BY proc_id, procurements BY id;
filtered = FILTER joined BY company == 'Lenta';
expenses = FOREACH filtered GENERATE price * amount as mul;
result   = FOREACH (GROUP expenses ALL) GENERATE SUM(expenses.mul);
dump result;