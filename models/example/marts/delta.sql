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
),


delta as (

    select
        p.*,
        case
            when not exists (
                select 1
                from {{ this }} t
                where t.cd_product = p.cd_product
            ) then 'prodotto nuovo'
            when p.last_update_timestamp > (
                select max(last_update_timestamp)
                from {{ this }} t
                where t.cd_product = p.cd_product
            ) then 'prodotto aggiornato'
            else ' prodotto non cambiato '
        end as changes_in_rows,
        case
            when not exists (
                select 1
                from {{ this }} t
                where t.cd_product = p.cd_product
            ) then p.price
            when p.last_update_timestamp > (
                select max(last_update_timestamp)
                from {{ this }} t
                where t.cd_product = p.cd_product
            ) then p.price - ( select t.price
                                from {{this}} t
                                where t.cd_product = p.cd_product
                                order by t3.last_update_timestamp desc --ultimo prezzo
                                limit 1 )
            else 0
        end as delta_col,


    from products p
    
)

select
    cd_product,
    product,
    desc_product,
    price,
    last_update_timestamp,
    changes_in_rows,
    delta_col
from delta
--where changes_in_rows != 'prodotto non cambiato'


--questa mi mostra solo le righe nuove o aggiornate e non quelle cambiate, quindi fa il delta
