
  
    

  create  table "momo_warehouse"."momo_dw_marts"."mart_high_value_transactions__dbt_tmp"
  
  
    as
  
  (
    /*
  Mart Model: mart_high_value_transactions
  ==========================================
  Surfaces all high-value and anomalous transactions
  for compliance monitoring and fraud review in Power BI.

  Grain: One row per flagged transaction
  Author: Lawrence Koomson
*/

with staged as (

    select * from "momo_warehouse"."momo_dw_staging"."stg_momo_transactions"

),

flagged as (

    select
        transaction_id,
        transaction_date,
        transaction_hour,
        day_of_week,
        is_weekend,

        sender_msisdn,
        receiver_msisdn,
        operator,
        region,

        transaction_amount_ghs,
        transaction_fee_ghs,
        total_debit_ghs,
        transaction_type_label,
        transaction_status,
        amount_bucket,
        currency,

        is_high_value,
        is_rapid_succession,
        secs_since_last_txn,

        -- Risk classification
        case
            when is_high_value and is_rapid_succession then 'CRITICAL'
            when is_high_value                         then 'HIGH'
            when is_rapid_succession                   then 'MEDIUM'
            else 'LOW'
        end                                             as risk_level,

        -- Alert reason
        case
            when is_high_value and is_rapid_succession
                then 'High-value rapid succession — immediate review required'
            when is_high_value
                then 'High-value transaction — above 95th percentile'
            when is_rapid_succession
                then 'Rapid succession — multiple txns within 2 minutes'
            else 'Monitoring'
        end                                             as alert_reason,

        loaded_at

    from staged
    where
        is_high_value = true
        or is_rapid_succession = true

)

select * from flagged
order by
    case risk_level
        when 'CRITICAL' then 1
        when 'HIGH'     then 2
        when 'MEDIUM'   then 3
        else 4
    end,
    transaction_amount_ghs desc
  );
  