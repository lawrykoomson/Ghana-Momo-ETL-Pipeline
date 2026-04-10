/*
  Mart Model: mart_operator_performance
  =======================================
  Aggregates transaction metrics per mobile network operator.
  Powers the operator comparison view in Power BI.

  Grain: One row per operator
  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_momo_transactions') }}

),

operator_stats as (

    select
        operator,

        -- Volume
        count(transaction_id)                           as total_transactions,
        count(distinct sender_msisdn)                   as unique_senders,
        count(distinct receiver_msisdn)                 as unique_receivers,

        -- Revenue
        round(sum(transaction_amount_ghs), 2)           as total_volume_ghs,
        round(avg(transaction_amount_ghs), 2)           as avg_transaction_ghs,
        round(min(transaction_amount_ghs), 2)           as min_transaction_ghs,
        round(max(transaction_amount_ghs), 2)           as max_transaction_ghs,
        round(sum(transaction_fee_ghs), 2)              as total_fees_collected_ghs,

        -- Transaction types
        count(case when transaction_type_code = 'P2P'
                   then 1 end)                          as p2p_count,
        count(case when transaction_type_code = 'MBP'
                   then 1 end)                          as merchant_payment_count,
        count(case when transaction_type_code = 'WDR'
                   then 1 end)                          as withdrawal_count,
        count(case when transaction_type_code = 'DEP'
                   then 1 end)                          as deposit_count,

        -- Performance
        round(
            count(case when transaction_status = 'SUCCESS' then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100
        , 2)                                            as success_rate_pct,

        round(
            count(case when transaction_status = 'FAILED' then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100
        , 2)                                            as failure_rate_pct,

        -- Anomalies
        count(case when is_high_value
                   then 1 end)                          as high_value_transactions,
        count(case when is_rapid_succession
                   then 1 end)                          as rapid_succession_flags,

        -- Market share
        round(
            count(transaction_id)::numeric
            / sum(count(transaction_id)) over () * 100
        , 2)                                            as market_share_by_volume_pct,

        round(
            sum(transaction_amount_ghs)
            / sum(sum(transaction_amount_ghs)) over () * 100
        , 2)                                            as market_share_by_value_pct

    from staged
    group by operator

)

select * from operator_stats
order by total_volume_ghs desc