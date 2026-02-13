┌─────────────────────────────────────────────────────────────────────────────┐
│                    ORIGINAL APPROACH (UNRELIABLE)                           │
│─────────────────────────────────────────────────────────────────────────────│
│                                                                             │
│  SHOW STREAMS ... → RESULT_SCAN(LAST_QUERY_ID()) → SELECT "stale"          │
│                                                                             │
│  PROBLEMS:                                                                  │
│  1. RESULT_SCAN inside SP may reference wrong query                        │
│  2. Column names case-sensitive ("stale" vs "STALE")                       │
│  3. If SELECT fails, exception sets v_stream_stale = TRUE but              │
│     variable may already be FALSE from initialization                       │
│  4. SHOW STREAMS doesn't error on stale - it just shows status             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘






┌─────────────────────────────────────────────────────────────────────────────┐
│                    NEW APPROACH (RELIABLE)                                  │
│─────────────────────────────────────────────────────────────────────────────│
│                                                                             │
│  SELECT FROM STREAM → Success = Healthy | Error = Stale                    │
│                                                                             │
│  WHY IT WORKS:                                                              │
│  • Snowflake documentation states: "Querying a stale stream throws error"  │
│  • Error message: "The stream has become stale..."                         │
│  • Direct test - no RESULT_SCAN dependency                                 │
│  • Exception handling catches the stale error reliably                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘


CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM')
AS
    CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();



 CREATE OR REPLACE TASK D_BRONZE.SADB.TASK_PROCESS_OPTRN_LEG_CDC
    WAREHOUSE = INFA_INGEST_WH
    SCHEDULE = '5 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
WHEN
    SYSTEM$STREAM_HAS_DATA('D_BRONZE.SADB.OPTRN_LEG_BASE_STREAM')
AS
    CALL D_BRONZE.SADB.SP_PROCESS_OPTRN_LEG_CDC();



 ┌─────────────────────────────────────────────────────────────────────────────┐
│                        TASK BEST PRACTICES                                  │
└─────────────────────────────────────────────────────────────────────────────┘

1. USE WHEN CLAUSE (IMPLEMENTED ✅)
   ─────────────────────────────────
   WHEN SYSTEM$STREAM_HAS_DATA('stream_name')
   
   • Prevents unnecessary warehouse spin-up
   • Reduces compute costs
   • Task only runs when data exists

2. SET SUSPEND_TASK_AFTER_NUM_FAILURES (NOW ADDED ✅)
   ────────────────────────────────────────────────────
   SUSPEND_TASK_AFTER_NUM_FAILURES = 3
   
   • Auto-suspends after 3 consecutive failures
   • Prevents runaway errors
   • Alerts team to investigate

3. PREVENT OVERLAPPING EXECUTION (IMPLEMENTED ✅)
   ──────────────────────────────────────────────
   ALLOW_OVERLAPPING_EXECUTION = FALSE
   
   • Prevents duplicate processing
   • Ensures data consistency
   • Avoids race conditions

4. DEDICATED WAREHOUSE (IMPLEMENTED ✅)
   ─────────────────────────────────────
   WAREHOUSE = INFA_INGEST_WH
   
   • Isolated from user queries
   • Predictable performance
   • Easier cost tracking

5. SET TIMEOUT (NOW ADDED ✅)
   ──────────────────────────
   USER_TASK_TIMEOUT_MS = 3600000  (1 hour)
   
   • Prevents hung tasks
   • Frees resources on failure

6. MONITOR TASK HISTORY
   ─────────────────────
   Use TASK_HISTORY() to track:
   • Success/failure rates
   • Execution duration
   • Records processed


-- Check task execution history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SEC,
    RETURN_VALUE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_OPTRN_LEG_CDC',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 50;
