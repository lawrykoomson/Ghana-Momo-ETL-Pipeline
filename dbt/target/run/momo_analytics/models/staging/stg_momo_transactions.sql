
  create view "momo_warehouse"."momo_dw_staging"."stg_momo_transactions__dbt_tmp"
    
    
  as (
    /*
  Staging Model: stg_momo_transactions
  =====================================
  Cleans and standardises the raw MoMo transaction fact table.
  Serves as the single source of truth for all downstream models.

  Source: momo_dw.fact_transactions (loaded by ETL pipeline)
  Author: Lawrence Koomson
*/

with source as (

    select * from "momo_warehouse"."momo_dw"."fact_transactions"

),

staged as (

    select
        -- Primary key
        transaction_id,

        -- Timestamps
        timestamp                                   as transaction_timestamp,
        txn_date                                    as transaction_date,
        txn_hour                                    as transaction_hour,
        txn_day_of_week                             as day_of_week,
        txn_month                                   as transaction_month,
        txn_quarter                                 as transaction_quarter,
        is_weekend,

        -- Parties
        upper(trim(sender_msisdn))                  as sender_msisdn,
        upper(trim(receiver_msisdn))                as receiver_msisdn,

        -- Financials
        amount                                      as transaction_amount_ghs,
        fee                                         as transaction_fee_ghs,
        total_debit                                 as total_debit_ghs,
        round(fee / nullif(amount, 0) * 100, 4)     as fee_rate_pct,

        -- Categorisation
        upper(transaction_type)                     as transaction_type_code,
        transaction_type_label,
        upper(currency)                             as currency,
        amount_bucket,

        -- Operator & geography
        upper(operator)                             as operator,
        upper(status)                               as transaction_status,
        initcap(region)                             as region,

        -- Flags
        is_high_value,
        is_rapid_succession,
        coalesce(time_since_last_txn_sec, 0)        as secs_since_last_txn,

        -- Audit
        loaded_at

    from source
   where
        transaction_id is not null
        and amount > 0
        and status in ('SUCCESS', 'FAILED', 'PENDING', 'REVERSED')
)

select * from staged
  );