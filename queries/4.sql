-- Most expensive product
select products.name, products.price from products
join (select max(price) as price from products) as max
on products.price = max.price;