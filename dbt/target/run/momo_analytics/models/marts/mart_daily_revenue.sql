
  
    

  create  table "momo_warehouse"."momo_dw_marts"."mart_daily_revenue__dbt_tmp"
  
  
    as
  
  (
    /*
  Mart Model: mart_daily_revenue
  ================================
  Aggregates MoMo transaction revenue by day.
  Powers the daily revenue trend in Power BI.

  Grain: One row per transaction_date
  Author: Lawrence Koomson
*/

with staged as (

    select * from "momo_warehouse"."momo_dw_staging"."stg_momo_transactions"

),

daily_revenue as (

    select
        transaction_date,
        extract(year  from transaction_date)    as year,
        extract(month from transaction_date)    as month,
        extract(quarter from transaction_date)  as quarter,
        to_char(transaction_date, 'Month')      as month_name,
        to_char(transaction_date, 'Day')        as day_name,
        is_weekend,

        -- Volume
        count(transaction_id)                           as total_transactions,
        count(case when transaction_status = 'SUCCESS'
                   then 1 end)                          as successful_transactions,
        count(case when transaction_status = 'FAILED'
                   then 1 end)                          as failed_transactions,
        count(case when transaction_status = 'REVERSED'
                   then 1 end)                          as reversed_transactions,

        -- Revenue
        round(sum(transaction_amount_ghs), 2)           as gross_revenue_ghs,
        round(sum(case when transaction_status = 'SUCCESS'
                       then transaction_amount_ghs
                       else 0 end), 2)                  as net_revenue_ghs,
        round(sum(transaction_fee_ghs), 2)              as total_fees_ghs,
        round(avg(transaction_amount_ghs), 2)           as avg_transaction_ghs,

        -- High value
        count(case when is_high_value then 1 end)       as high_value_count,
        round(sum(case when is_high_value
                       then transaction_amount_ghs
                       else 0 end), 2)                  as high_value_revenue_ghs,

        -- Success rate
        round(
            count(case when transaction_status = 'SUCCESS' then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100
        , 2)                                            as success_rate_pct

    from staged
    group by
        transaction_date,
        extract(year  from transaction_date),
        extract(month from transaction_date),
        extract(quarter from transaction_date),
        to_char(transaction_date, 'Month'),
        to_char(transaction_date, 'Day'),
        is_weekend

)

select * from daily_revenue
order by transaction_date
  );
  