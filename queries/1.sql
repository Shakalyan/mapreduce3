-- Sold amount of sugar
select sum(prpr_map.amount) as sugar_amount from prpr_map 
join products on products.id = prpr_map.prod_id
where products.name = "Sugar";