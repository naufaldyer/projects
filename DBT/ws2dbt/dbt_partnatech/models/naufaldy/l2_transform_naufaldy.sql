{{ config(materialized='table') }}

with source_data as (
    select
        flightId flight_id,
        searchTerms search_terms,
        rank,		
        title,		
        snippet,		
        displayLink display_link,		
        link,		
        queryTime query_time, 	
        totalResults total_results,		
        cacheId cache_id,		
        formattedUrl formatted_url,		
        htmlFormattedUrl html_formatted_url,		
        htmlSnippet html_snippet,		
        htmlTitle html_title,		
        kind,		
        pagemap,		
        cseName cse_name,		
        count,		
        startIndex start_index,		
        inputEncoding input_encoding,		
        outputEncoding output_encoding,		
        safe,		
        cx,		
        gl,		
        searchTime search_time,	
        formattedSearchTime formatted_search_time,		
        formattedTotalResults formatted_total_results
    from `partnatech.etl_workshop_naufaldy.flight_tickets_raw`
),

enrich_data AS (
    select
        *,
        DATE(query_time) as query_date
    from source_data
)

select * from enrich_data