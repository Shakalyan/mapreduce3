prpr_map     = LOAD '/user/shakalyan/warehouse/prpr_map/000000_0' USING PigStorage(',') as (prod_id:int, proc_id:int, amount:int);
procurements = LOAD '/user/shakalyan/warehouse/procurements/000000_0' USING PigStorage(',') as (id:int, company:chararray);

joined   = JOIN prpr_map BY proc_id, procurements BY id;
filtered = FILTER joined BY company == 'Perek';
result   = FOREACH (GROUP filtered ALL) GENERATE MAX(filtered.amount);
dump result;