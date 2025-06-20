{{ config(
    materialized='incremental',
    unique_key='cd_product',
    incremental_strategy='merge'
) }}

with products_src as (
    select
        cd_product,
        product,
        desc_product,
        price,
        last_update_timestamp,
        case
          when lag(price) over (partition by cd_product 
                                order by last_update_timestamp) is null
            then 'prodotto nuovo' --quando prezzo vecchio per ogni prodotto non ce allora è nuovo
          when price != lag(price) over (partition by cd_product 
                                         order by last_update_timestamp)
            then 'prodotto aggiornato' -- quando prezzo ora è diverso da prezzo prima vuol dire che ha fatto delta load e ho aggiornato prezzo 
          else 'prodotto non cambiato' 
        end as changes_in_rows,
        case
          when lag(price) over (partition by cd_product 
                                order by last_update_timestamp) is null
            then price -- quando prezzo vecchio non ce, allora nuovo product e metto suo prezzo
          when price != lag(price) over (partition by cd_product 
                                         order by last_update_timestamp) 
            then price - lag(price) over (partition by cd_product 
                                          order by last_update_timestamp)  -- quando prezzo vecchio diverso da ora, metto variazione prezzo
          else 0
        end as delta_price
    from {{ ref('stg_products') }}
    {% if is_incremental() %}
      where last_update_timestamp >= (
        select coalesce(max(last_update_timestamp), '1900-01-01') from {{ this }}
      )
    {% endif %}
)

select
  cd_product,
  product,
  desc_product,
  price,
  last_update_timestamp,
  changes_in_rows,
  delta_price
from products_src
