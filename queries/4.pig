products = LOAD '/user/shakalyan/warehouse/products/000000_0' USING PigStorage(',') as (id:int, name:chararray, price:int);

max_price_rel = FOREACH (GROUP products ALL) GENERATE MAX(products.price) as max_price;
joined        = JOIN products BY price, max_price_rel by max_price;
result        = FOREACH joined GENERATE name, price;
dump result;