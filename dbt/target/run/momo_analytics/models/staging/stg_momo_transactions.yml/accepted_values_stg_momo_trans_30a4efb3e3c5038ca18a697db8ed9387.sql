
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        transaction_status as value_field,
        count(*) as n_records

    from "momo_warehouse"."momo_dw_staging"."stg_momo_transactions"
    group by transaction_status

)

select *
from all_values
where value_field not in (
    'SUCCESS','FAILED','PENDING','REVERSED'
)



  
  
      
    ) dbt_internal_test