{{ config(materialized='table') }}

SELECT 
  DATE(query_time) as search_date,
  search_terms,
  COUNT(*) as total_searches,
  AVG(rank) as average_rank,
  COUNT(DISTINCT display_Link) as unique_sources
FROM
  {{ ref('l3_transform_hanif') }}
WHERE
  search_terms IS NOT NULL
GROUP BY
  search_date,
  search_Terms
ORDER BY
  search_date DESC,
  total_searches DESC