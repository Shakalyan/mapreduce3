-- Products
drop table if exists products;
create table products(id int, name string, price int) row format delimited fields terminated by ',';

insert into products(id, name, price) values
(1, "Banana", 100),
(2, "Coffee", 250),
(3, "Sugar", 50),
(4, "Tea", 80),
(5, "Potato", 40);

-- Procurements
drop table if exists procurements;
create table procurements(id int, company string) row format delimited fields terminated by ',';

insert into procurements(id, company) values
(1, "Lenta"),
(2, "Perek"),
(3, "5erochka"),
(4, "Magnit");

-- Mapping
drop table if exists prpr_map;
create table prpr_map(prod_id int, proc_id int, amount int) row format delimited fields terminated by ',';

insert into prpr_map(prod_id, proc_id, amount) values
(1, 1, 400),
(2, 1, 50),
(5, 1, 1000),
(3, 2, 700),
(4, 2, 200),
(3, 3, 300),
(5, 4, 600);