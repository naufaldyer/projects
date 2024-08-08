SELECT
  EXTRACT(MONTH FROM query_time) as search_month,
  search_terms,
  COUNT(*) as total_searches
FROM
  {{ ref('l3_transform_hanif') }}
GROUP BY
  search_month,
  search_terms
ORDER BY
  search_month, total_searches DESC
