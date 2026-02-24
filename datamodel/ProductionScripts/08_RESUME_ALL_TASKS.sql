/*
================================================================================
CDC DATA PRESERVATION - RESUME ALL TASKS
================================================================================
Purpose      : Activate all 21 CDC tasks to start processing
Version      : 1.0 (Production)
Last Updated : 2026-02-23
================================================================================

PREREQUISITES:
- Execute ALL previous scripts (01-07) first
- Verify all infrastructure is correctly configured
- Ensure EXECUTE TASK privilege is granted

WARNING: Executing this script will start all CDC processing tasks.
Ensure you are ready for data to start flowing to D_BRONZE.

================================================================================
*/

-- =============================================================================
-- USE PARAMETERS FROM SESSION
-- =============================================================================
USE DATABASE IDENTIFIER($SOURCE_DATABASE);
USE SCHEMA IDENTIFIER($SOURCE_SCHEMA);
USE WAREHOUSE IDENTIFIER($CDC_WAREHOUSE);

-- =============================================================================
-- PRE-FLIGHT CHECK
-- =============================================================================
-- Verify task privilege
SELECT CURRENT_ROLE() AS CURRENT_ROLE, 
       'Ensure this role has EXECUTE TASK privilege' AS NOTE;

-- Verify all tasks exist in suspended state
SELECT COUNT(*) AS SUSPENDED_TASK_COUNT
FROM TABLE(INFORMATION_SCHEMA.TASKS())
WHERE NAME LIKE 'TASK_PROCESS_%'
AND STATE = 'suspended';

-- =============================================================================
-- RESUME ALL TASKS
-- =============================================================================

-- Task 1: TRAIN_PLAN
ALTER TASK TASK_PROCESS_TRAIN_PLAN RESUME;

-- Task 2: OPTRN
ALTER TASK TASK_PROCESS_OPTRN RESUME;

-- Task 3: OPTRN_LEG
ALTER TASK TASK_PROCESS_OPTRN_LEG RESUME;

-- Task 4: OPTRN_EVENT
ALTER TASK TASK_PROCESS_OPTRN_EVENT RESUME;

-- Task 5: TRKFC_TRSTN
ALTER TASK TASK_PROCESS_TRKFC_TRSTN RESUME;

-- Task 6: TRAIN_CNST_SMRY
ALTER TASK TASK_PROCESS_TRAIN_CNST_SMRY RESUME;

-- Task 7: STNWYB_MSG_DN
ALTER TASK TASK_PROCESS_STNWYB_MSG_DN RESUME;

-- Task 8: EQPMNT_AAR_BASE
ALTER TASK TASK_PROCESS_EQPMNT_AAR_BASE RESUME;

-- Task 9: EQPMV_RFEQP_MVMNT_EVENT
ALTER TASK TASK_PROCESS_EQPMV_RFEQP_MVMNT_EVENT RESUME;

-- Task 10: CTNAPP_CTNG_LINE_DN
ALTER TASK TASK_PROCESS_CTNAPP_CTNG_LINE_DN RESUME;

-- Task 11: EQPMV_EQPMT_EVENT_TYPE
ALTER TASK TASK_PROCESS_EQPMV_EQPMT_EVENT_TYPE RESUME;

-- Task 12: TRAIN_PLAN_EVENT
ALTER TASK TASK_PROCESS_TRAIN_PLAN_EVENT RESUME;

-- Task 13: LCMTV_MVMNT_EVENT
ALTER TASK TASK_PROCESS_LCMTV_MVMNT_EVENT RESUME;

-- Task 14: LCMTV_EMIS
ALTER TASK TASK_PROCESS_LCMTV_EMIS RESUME;

-- Task 15: TRAIN_PLAN_LEG
ALTER TASK TASK_PROCESS_TRAIN_PLAN_LEG RESUME;

-- Task 16: TRKFCG_SBDVSN
ALTER TASK TASK_PROCESS_TRKFCG_SBDVSN RESUME;

-- Task 17: TRKFCG_FIXED_PLANT_ASSET
ALTER TASK TASK_PROCESS_TRKFCG_FIXED_PLANT_ASSET RESUME;

-- Task 18: TRKFCG_FXPLA_TRACK_LCTN_DN
ALTER TASK TASK_PROCESS_TRKFCG_FXPLA_TRACK_LCTN_DN RESUME;

-- Task 19: TRAIN_CNST_DTL_RAIL_EQPT
ALTER TASK TASK_PROCESS_TRAIN_CNST_DTL_RAIL_EQPT RESUME;

-- Task 20: TRKFCG_TRACK_SGMNT_DN
ALTER TASK TASK_PROCESS_TRKFCG_TRACK_SGMNT_DN RESUME;

-- Task 21: TRKFCG_SRVC_AREA
ALTER TASK TASK_PROCESS_TRKFCG_SRVC_AREA RESUME;

-- =============================================================================
-- POST-ACTIVATION VERIFICATION
-- =============================================================================

-- Verify all tasks are now running
SELECT 
    NAME AS TASK_NAME,
    STATE,
    SCHEDULE,
    LAST_COMMITTED_ON,
    CASE 
        WHEN STATE = 'started' THEN 'ACTIVE'
        ELSE 'CHECK REQUIRED'
    END AS STATUS
FROM TABLE(INFORMATION_SCHEMA.TASKS())
WHERE NAME LIKE 'TASK_PROCESS_%'
ORDER BY NAME;

-- Count active tasks
SELECT 
    COUNT(CASE WHEN STATE = 'started' THEN 1 END) AS ACTIVE_TASKS,
    COUNT(CASE WHEN STATE = 'suspended' THEN 1 END) AS SUSPENDED_TASKS,
    COUNT(*) AS TOTAL_TASKS
FROM TABLE(INFORMATION_SCHEMA.TASKS())
WHERE NAME LIKE 'TASK_PROCESS_%';

/*
================================================================================
ALL 21 CDC TASKS RESUMED
================================================================================
Tasks will begin processing on their scheduled intervals (default: 5 minutes).

MONITORING:
1. Check task execution history:
   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
   WHERE NAME LIKE 'TASK_PROCESS_%'
   ORDER BY SCHEDULED_TIME DESC LIMIT 50;

2. Check audit log:
   SELECT * FROM D_BRONZE.SADB.CDC_PROCESSING_LOG
   ORDER BY PROCESSING_START DESC LIMIT 100;

3. Monitor stream health:
   SELECT * FROM D_BRONZE.SADB.CDC_STREAM_STATUS;

4. Check sync status:
   SELECT * FROM D_BRONZE.SADB.CDC_TABLE_SYNC_STATUS;

TO SUSPEND A SPECIFIC TASK:
   ALTER TASK TASK_PROCESS_<TABLE_NAME> SUSPEND;

TO SUSPEND ALL TASKS:
   -- Run these commands individually
   ALTER TASK TASK_PROCESS_TRAIN_PLAN SUSPEND;
   ALTER TASK TASK_PROCESS_OPTRN SUSPEND;
   -- ... (repeat for all 21 tasks)
================================================================================
*/
