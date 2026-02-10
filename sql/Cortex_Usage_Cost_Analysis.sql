-- ============================================================================
-- CORTEX MODEL USAGE AND COST ANALYSIS (Last 7 Days)
-- ============================================================================
-- Replace 2.50 with your actual Snowflake credit rate

-- 1. Cortex LLM Functions Usage (COMPLETE, EXTRACT_ANSWER, SENTIMENT, SUMMARIZE, TRANSLATE, etc.)
SELECT 
    DATE(start_time) AS usage_date,
    function_name,
    model_name,
    SUM(tokens) AS total_tokens,
    ROUND(SUM(token_credits), 6) AS total_credits,
    ROUND(SUM(token_credits) * 2.50, 2) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY DATE(start_time), function_name, model_name
ORDER BY usage_date DESC, total_credits DESC;

-- 2. Cortex Analyst Usage
SELECT 
    DATE(start_time) AS usage_date,
    SUM(request_count) AS total_requests,
    ROUND(SUM(credits), 6) AS total_credits,
    ROUND(SUM(credits) * 2.50, 2) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY DATE(start_time)
ORDER BY usage_date DESC;

-- 3. Cortex Search Service Usage
SELECT 
    DATE(start_time) AS usage_date,
    database_name,
    schema_name,
    service_name,
    ROUND(SUM(credits), 6) AS total_credits,
    ROUND(SUM(credits) * 2.50, 2) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY DATE(start_time), database_name, schema_name, service_name
ORDER BY usage_date DESC;

-- 4. Combined Cortex Cost Summary (All Services)
WITH all_cortex_usage AS (
    SELECT 
        DATE(start_time) AS usage_date,
        'LLM_FUNCTION' AS service_type,
        function_name::VARCHAR AS service_name,
        model_name::VARCHAR AS model_name,
        SUM(tokens) AS usage_count,
        SUM(token_credits) AS credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
    WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY ALL
    
    UNION ALL
    
    SELECT 
        DATE(start_time),
        'CORTEX_ANALYST',
        'ANALYST',
        'text2sql',
        SUM(request_count),
        SUM(credits)
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
    WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY DATE(start_time)
    
    UNION ALL
    
    SELECT 
        DATE(start_time),
        'CORTEX_SEARCH',
        service_name::VARCHAR,
        'search',
        COUNT(*),
        SUM(credits)
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY
    WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
    GROUP BY DATE(start_time), service_name
)
SELECT 
    usage_date,
    service_type,
    service_name,
    model_name,
    usage_count,
    ROUND(credits, 6) AS total_credits,
    ROUND(credits * 2.50, 2) AS estimated_cost_usd
FROM all_cortex_usage
ORDER BY usage_date DESC, credits DESC;

-- 5. Weekly Summary by Service Type
WITH weekly_totals AS (
    SELECT 'LLM Functions' AS service, SUM(token_credits) AS credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
    WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
    
    UNION ALL
    
    SELECT 'Cortex Analyst', SUM(credits)
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
    WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
    
    UNION ALL
    
    SELECT 'Cortex Search', SUM(credits)
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY
    WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
)
SELECT 
    service,
    COALESCE(ROUND(credits, 4), 0) AS total_credits,
    COALESCE(ROUND(credits * 2.50, 2), 0) AS estimated_cost_usd
FROM weekly_totals
ORDER BY credits DESC NULLS LAST;

-- 6. Top Models by Cost (LLM Functions)
SELECT 
    model_name,
    function_name,
    SUM(tokens) AS total_tokens,
    ROUND(SUM(token_credits), 4) AS total_credits,
    ROUND(SUM(token_credits) * 2.50, 2) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY model_name, function_name
ORDER BY total_credits DESC;
