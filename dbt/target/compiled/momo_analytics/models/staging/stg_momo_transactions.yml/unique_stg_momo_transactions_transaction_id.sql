
    
    

select
    transaction_id as unique_field,
    count(*) as n_records

from "momo_warehouse"."momo_dw_staging"."stg_momo_transactions"
where transaction_id is not null
group by transaction_id
having count(*) > 1


