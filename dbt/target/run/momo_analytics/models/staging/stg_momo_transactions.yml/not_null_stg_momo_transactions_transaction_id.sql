
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_id
from "momo_warehouse"."momo_dw_staging"."stg_momo_transactions"
where transaction_id is null



  
  
      
    ) dbt_internal_test