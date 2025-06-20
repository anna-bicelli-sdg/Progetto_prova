{{ config(
  materialized='incremental',
  unique_key='cd_product',
  incremental_strategy='merge'
) }}

with

updated_or_new as (
  select
    cd_product,
    product,
    desc_product,
    price,
    last_update_timestamp
  from {{ ref('stg_products') }}
  {% if is_incremental() %}
    where last_update_timestamp >= (
      select coalesce(max(last_update_timestamp)) from {{ this }}
    )
  {% endif %}
), -- seleziono tutte le colonne per righe nuove/ aggiornate

-- CTE che calcola per ogni prodotto il prezzo precedente
price_day0 as (
  select
    cd_product,
    max(last_update_timestamp) as prev_ts,
    max(price) as prev_price
  from {{ this }}
  group by cd_product
),

-- CTE per changes_in_rows
changes_rows as (
  select
    u.cd_product,
    case
      when p.cd_product is null then 'prodotto nuovo'
      when u.price != p.prev_price then 'prodotto aggiornato'
      else 'prodotto non cambiato'
    end as changes_in_rows
  from updated_or_new u
  left join price_day0  p using(cd_product)
),

delta as (
  select
    u.cd_product,
    case
      when p.cd_product is null then u.price -- new price
      when u.price != p.prev_price then u.price - p.prev_price -- differenza tra nuovo e vecchio prezzo
      else 0 -- 0 variazione
    end as delta_price
  from updated_or_new u
  left join price_day0  p using(cd_product)
),


products as (
  select
    u.*,
    c.changes_in_rows,
    d.delta_price
  from updated_or_new u
  left join changes_rows c using(cd_product)
  left join delta d using(cd_product)
)

select * from products
