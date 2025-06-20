{{
    config(
        materialized='incremental',
        unique_key = 'cd_product',
        incremental_strategy = 'merge' 
    )
}} 

with products as (

    {% if is_incremental() %}

        select 
            cd_product,
            product,
            desc_product,
            price,
            last_update_timestamp
        from {{ ref('stg_products')}}
        where last_update_timestamp >= (select max(last_update_timestamp) from {{ this }})  -- trovo last_update piu recente (quello del caricamento incrementale) rispetto a quello che c'era nel stg_products prima del caricamento
    
     {%else%}
        select 
            cd_product,
            product,
            desc_product,
            price,
            last_update_timestamp
        from {{ ref('stg_products')}}

    {% endif %}
)

select * from products
--{% if is_incremental() %}
   -- where last_update_timestamp >= (select max(last_update_timestamp) from {{ this }})  -- trovo last_update piu recente (quello del caricamento incrementale) rispetto a quello che c'era nel stg_products prima del caricamento
--{% endif %}

