
  
    

  create  table "momo_warehouse"."momo_dw_marts"."mart_region_summary__dbt_tmp"
  
  
    as
  
  (
    /*
  Mart Model: mart_region_summary
  =================================
  Aggregates MoMo transaction data by Ghana region.
  Powers the regional heatmap in Power BI.

  Grain: One row per region
  Author: Lawrence Koomson
*/

with staged as (

    select * from "momo_warehouse"."momo_dw_staging"."stg_momo_transactions"

),

region_stats as (

    select
        region,

        -- Volume
        count(transaction_id)                           as total_transactions,
        count(distinct sender_msisdn)                   as unique_customers,

        -- Revenue
        round(sum(transaction_amount_ghs), 2)           as total_volume_ghs,
        round(avg(transaction_amount_ghs), 2)           as avg_transaction_ghs,
        round(sum(transaction_fee_ghs), 2)              as total_fees_ghs,

        -- Success metrics
        round(
            count(case when transaction_status = 'SUCCESS' then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100
        , 2)                                            as success_rate_pct,

        -- Peak hours
        mode() within group (order by transaction_hour) as peak_hour,

        -- Weekend activity
        count(case when is_weekend then 1 end)          as weekend_transactions,
        round(
            count(case when is_weekend then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100
        , 2)                                            as weekend_pct,

        -- High value
        count(case when is_high_value then 1 end)       as high_value_count,
        round(sum(case when is_high_value
                       then transaction_amount_ghs
                       else 0 end), 2)                  as high_value_volume_ghs,

        -- Market share
        round(
            sum(transaction_amount_ghs)
            / sum(sum(transaction_amount_ghs)) over () * 100
        , 2)                                            as revenue_share_pct

    from staged
    group by region

)

select * from region_stats
order by total_volume_ghs desc
  );
  