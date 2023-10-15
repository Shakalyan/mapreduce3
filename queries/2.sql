-- Lenta summary
select sum(products.price * prpr_map.amount) as summary from prpr_map
join products on products.id = prpr_map.prod_id
join procurements on procurements.id = prpr_map.proc_id
where procurements.company == "Lenta";