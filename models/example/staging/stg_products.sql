{{ config( materialized = 'view') }} --in staging solitamente ci sono le views

select *
--from {{ source('product','Day0')}}  -- all'inizio tempo 0 
from {{ source('product','Day1')}}  -- per caricamento incrementale al tempo 1
