{{ config(
        materialized='table',
        schema='naufaldy'
    ) 
}}

-- select columns that will used from l3 layer
WITH data_source as (
    SELECT flight_id,
        display_link,
        rank,
        search_terms,
        query_date
    FROM {{ ref('l3_transform_naufaldy') }}
),

--transform data 
transform_data as (
  SELECT 
    *,
    REGEXP_EXTRACT(search_terms, r'to\s+(.*)') as destination,
    EXTRACT(MONTH from query_date) as search_month,
    DATE(query_date) as search_date
  FROM data_source
),

-- aggregate data
aggregate_data as (
  SELECT
    search_date,
    search_month,
    destination,
    display_link,
    rank,
    COUNT(display_link) web_company_url_count,
  FROM transform_data
  GROUP BY 1, 2, 3, 4, 5
)

-- query data
SELECT * FROM aggregate_data