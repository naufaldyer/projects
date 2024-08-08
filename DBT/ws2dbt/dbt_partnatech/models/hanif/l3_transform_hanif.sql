{{ 
    config(
        materialized='incremental',
        unique_key='flight_id',
        partition_by={
            "field": "query_date",
            "data_type": "date",
            "granularity": "day"
        }
    ) 
}}

SELECT * FROM {{ ref('l2_transform_hanif') }}
