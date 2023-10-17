products = LOAD '/user/shakalyan/warehouse/products/000000_0' USING PigStorage(',') as (id:int, name:chararray, price:int);

dump products;