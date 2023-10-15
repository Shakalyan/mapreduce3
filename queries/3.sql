-- Largest Perek procurement
select max(prpr_map.amount) as max_amount from prpr_map
join procurements on procurements.id = prpr_map.proc_id
where procurements.company = "Perek";