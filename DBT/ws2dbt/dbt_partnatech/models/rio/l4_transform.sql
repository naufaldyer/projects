{{ config(
        materialized='table',
        schema='rio'
    ) 
}}

SELECT 
  query_date,
  display_link,
  COUNT(display_link) web_company_url_count
FROM {{ ref('l3_transform') }}
GROUP BY 1,2