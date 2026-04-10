
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        operator as value_field,
        count(*) as n_records

    from "momo_warehouse"."momo_dw_staging"."stg_momo_transactions"
    group by operator

)

select *
from all_values
where value_field not in (
    'MTN','VODAFONE','AIRTELTIGO'
)



  
  
      
    ) dbt_internal_test