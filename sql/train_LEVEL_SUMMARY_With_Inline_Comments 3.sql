/* ============================================================================
   - Added inline comments to explain each logical step, join, filter, and agg.
   - Kept original column names and calculations unchanged.
   ============================================================================ */

SELECT
  -- Basic timestamps for the train movement
  BMR1.ARRIVAL_TMS,                 -- arrival timestamp of this constituent movement
  BMR1.DEPARTURE_TMS,               -- departure timestamp of this constituent movement
  BMR1.SADB_RECEIVING_DT,           -- date when row was received in SADB (source)

  -- Train identifiers and type/kind
  DIM_TRAIN.TITAN_NBR,              -- unique train identifier (Titan number)
  BMR1.TRAIN_NM,                    -- train name
  DIM_TRAIN.TRAIN_TYPE_CD,          -- train type code (e.g., freight/passenger)
  DIM_TRAIN.TRAIN_KIND_CD,          -- train kind code

  -- Consist number (which consist within the train this row represents)
  BMR1.CONSIST_NBR,

  -- Origin / Destination location FSAC codes
  DIM_DEPART.FSAC_CD  DEPART_FSAC,  -- departing location FSAC code
  DIM_ARRIVE.FSAC_CD  ARRIVE_FSAC,  -- arrival location FSAC code

  -- Run number is not available in this dataset, so it's set to NULL
  NULL AS run_nbr,

  -- Geography / subdivision fields
  DIM_OD.SBDVSN_NM,                 -- sub-division name (from the OD lookup)
  COALESCE(DIM_OD.REGION, DIM_SBDVSN.REGION) AS region,  -- region (prefer DIM_OD then fallback)
  COALESCE(DIM_OD.DIVISION_NM, DIM_SBDVSN.DIVISION_NM) AS dvsn, -- division (prefer DIM_OD)

  -- Mileage: miles between origin and destination as computed in the inline subselect
  DIM_ODM.MILES,

  /* =========================
     Aggregations / KPIs
     - The following are roll-up metrics computed per GROUP BY (see bottom)
     ========================= */

  /* Count live equipment units: if there's a locomotive id on the BMR row AND the equipment group is not 'EOTD' */
  SUM(
    CASE
      WHEN COALESCE(BMR1.DIM_LOCOMOTIVE_ID, 0) > 0
       AND COALESCE(DIM_EQUIP.EQPMNT_GROUP_CD, '') <> 'EOTD'
      THEN 1 ELSE 0
    END
  ) LIVE_UNIT,

  /* Count of loaded cars (content_tons > 0). Here each row is a constituent / car-level row so presence of content indicates loaded */
  SUM(CASE WHEN COALESCE(BMR1.CONTENT_TONS, 0) > 0 THEN 1 ELSE 0 END) CARS_LOADED,

  /* Total cars (or constituent rows) where LCMTV_LEAD_TRAIL_IND = 'NA' (interpreted as normal cars not loco indicators) */
  SUM(CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND = 'NA' THEN 1 ELSE 0 END) CARS_TOTAL,

  /* Equipment miles (only for rows where the LCMTV indicator = 'NA' and the equipment group is not EOTD) */
  SUM(
    CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND = 'NA'
      THEN COALESCE(DIM_OD.MILES, 0)
      ELSE 0
    END
  ) EQPMT_MILES,

  /* Train miles: count miles when LCMTV_LEAD_TRAIL_IND indicates lead ('LD') */
  SUM(
    CASE WHEN COALESCE(BMR1.LCMTV_LEAD_TRAIL_IND, '') = 'LD'
      THEN COALESCE(DIM_OD.MILES, 0)
      ELSE 0
    END
  ) TRAIN_MILES,

  /* Sum of horsepower quantity across rows */
  SUM(COALESCE(DIM_LOCO.HRSPWR_QTY, 0)) SUM_HRSPWR_QTY,

  /* Deadhead (DH) tare ton-miles: if lead/trail indicator = 'DH', use loco full weight (or fallback to BMR1.TARE_TONS) * miles */
  SUM(
    CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND = 'DH'
      THEN COALESCE(DIM_LOCO.FULL_WEIGHT_QTY / 2000, BMR1.TARE_TONS, 0)
           * COALESCE(DIM_OD.MILES, 0)
      ELSE 0
    END
  ) DH_TARE_TON_MILES,

  /* Locomotive tare ton-miles for lead/trail locos */
  SUM(
    CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('LD', 'LT')
      THEN COALESCE(DIM_LOCO.FULL_WEIGHT_QTY / 2000, BMR1.TARE_TONS, 0)
           * COALESCE(DIM_OD.MILES, 0)
      ELSE 0
    END
  ) LOCO_TARE_TON_MILES,

  /* Sum of locomotive full weight (tons) using the loco dimension or fallback to 0 */
  SUM(COALESCE(DIM_LOCO.FULL_WEIGHT_QTY / 2000, 0)) LOCO_FULL_WEIGHT,

  /* Content ton-miles: content_tons * miles */
  SUM(COALESCE(BMR1.CONTENT_TONS, 0) * COALESCE(DIM_OD.MILES, 0)) CONTENT_TON_MILES,

  /* Tare tons: sum of either loco full weight (divided by 2000) or BMR1.TARE_TONS fallback */
  SUM(COALESCE(DIM_LOCO.FULL_WEIGHT_QTY / 2000, BMR1.TARE_TONS, 0)) TARE_TONS,

  /* Tare ton-miles for cars (when LCMTV_LEAD_TRAIL_IND = 'NA') */
  SUM(
    CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND = 'NA'
      THEN COALESCE(DIM_LOCO.FULL_WEIGHT_QTY / 2000, BMR1.TARE_TONS, 0)
           * COALESCE(DIM_OD.MILES, 0)
      ELSE 0
    END
  ) TARE_TON_MILES,

  /* Count of live locomotives (lead/trail) - LD or LT indicate live loco */
  COALESCE(
    COUNT(CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('LD', 'LT') THEN DIM_LOCO.DIM_LOCOMOTIVE_ID END),
    0
  ) LIVE_LOCO_COUNT,

  /* Count of deadhead locomotives (DH) */
  COALESCE(
    COUNT(CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('DH') THEN DIM_LOCO.DIM_LOCOMOTIVE_ID END),
    0
  ) DH_LOCO_COUNT,

  /* Revenue car miles loaded:
     - exclude cars whose AAR_CAR_CD begins with M or D (maintenance/other)
     - require content_tons > 0 and LCMTV indicator = 'NA' to count as a loaded revenue car */
  COALESCE(
    SUM(
      CASE
        WHEN SUBSTR(DIM_EQUIP.AAR_CAR_CD, 1, 1) NOT IN ('M', 'D')
         AND BMR1.CONTENT_TONS > 0
         AND BMR1.LCMTV_LEAD_TRAIL_IND = 'NA'
        THEN DIM_OD.MILES
        ELSE NULL
      END
    ),
    0
  ) REV_CAR_MILES_LOADED,

  /* Revenue car miles empty: same logic but content_tons = 0 */
  COALESCE(
    SUM(
      CASE
        WHEN SUBSTR(DIM_EQUIP.AAR_CAR_CD, 1, 1) NOT IN ('M', 'D')
         AND BMR1.CONTENT_TONS = 0
         AND BMR1.LCMTV_LEAD_TRAIL_IND = 'NA'
        THEN DIM_OD.MILES
        ELSE NULL
      END
    ),
    0
  ) REV_CAR_MILES_EMPTY,

  /* Live locomotive miles (LD / LT) */
  COALESCE(
    SUM(CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('LD', 'LT') THEN DIM_OD.MILES END),
    0
  ) LIVE_LOCO_MILES,

  /* Deadhead locomotive miles (DH) */
  COALESCE(
    SUM(CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('DH') THEN DIM_OD.MILES END),
    0
  ) DH_LOCO_MILES,

  /* Passenger business miles (AAR car codes starting with 'M5') */
  COALESCE(
    SUM(CASE WHEN SUBSTR(DIM_EQUIP.AAR_CAR_CD, 1, 2) IN ('M5') THEN DIM_OD.MILES END),
    0
  ) PASS_BUS_MILES,

  /* Work car miles: where AAR_CAR_CD between 'M100' and 'M4999' (string comparison used in original) */
  COALESCE(
    SUM(
      CASE WHEN DIM_EQUIP.AAR_CAR_CD >= 'M100' AND DIM_EQUIP.AAR_CAR_CD <= 'M4999'
           THEN DIM_OD.MILES END
    ),
    0
  ) WORK_CAR_MILES,

  /* Caboose miles: specific code 'M930' */
  COALESCE(SUM(CASE WHEN DIM_EQUIP.AAR_CAR_CD = 'M930' THEN DIM_OD.MILES END), 0) CABOOSE_MILES,

  /* Live loco horsepower-miles: horsepower * miles for live locos */
  COALESCE(
    SUM(
      CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('LD', 'LT')
        THEN COALESCE(DIM_LOCO.HRSPWR_QTY, 0) * COALESCE(DIM_OD.MILES, 0)
      END
    ),
    0
  ) LIVE_LOCO_HP_MILES,

  /* DH loco horsepower-miles */
  COALESCE(
    SUM(
      CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('DH')
        THEN COALESCE(DIM_LOCO.HRSPWR_QTY, 0) * COALESCE(DIM_OD.MILES, 0)
      END
    ),
    0
  ) DH_LOCO_HP_MILES,

  /* Live loco foot-miles: OTSD length (in inches) / 12 => feet * miles */
  COALESCE(
    SUM(
      CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('LD', 'LT')
        THEN COALESCE(DIM_LOCO.OTSD_LENGTH_QTY / 12, 0) * COALESCE(DIM_OD.MILES, 0)
      END
    ),
    0
  ) LIVE_LOCO_FOOT_MILES,

  /* DH loco foot-miles */
  COALESCE(
    SUM(
      CASE WHEN BMR1.LCMTV_LEAD_TRAIL_IND IN ('DH')
        THEN COALESCE(DIM_LOCO.OTSD_LENGTH_QTY / 12, 0) * COALESCE(DIM_OD.MILES, 0)
      END
    ),
    0
  ) DH_LOCO_FOOT_MILES,

  /* Total car length in feet: sum of equipment OTSD_LENGTH_QTY (inches) divided by 12 */
  SUM(COALESCE(DIM_EQUIP.OTSD_LENGTH_QTY, 0)) / 12 CAR_LENGTH_FEET,

  /* Total loco foot miles: sum of loco OTSD length divided by 12 (this seems intended to be multiplied by miles elsewhere) */
  SUM(COALESCE(DIM_LOCO.OTSD_LENGTH_QTY, 0)) / 12 LOCO_FOOT_MILES,

  /* Zone (prefer DIM_OD zone else fallback to subdivision zone) */
  COALESCE(DIM_OD.ZONE, DIM_SBDVSN.ZONE) AS zone

FROM
  DM_BMR.BMR_FACT_CNST_MVMNT   BMR1,      -- main fact: constituent movement rows

  -- dimension tables joined to enrich the fact row
  DM_DIM.DIM_TRAIN           DIM_TRAIN,  -- train master
  DM_DIM.DIM_LOCATION        DIM_ARRIVE, -- arrival location dimension
  DM_DIM.DIM_LOCATION        DIM_DEPART,  -- departure location dimension
  DM_DIM.DIM_EQUIPMENT       DIM_EQUIP,   -- equipment/car dimension
  DM_DIM.DIM_LOCOMOTIVE      DIM_LOCO,    -- locomotive dimension (outer join in original)
  DM_DIM.GIS_ORGN_DSTNTN_MLG DIM_ODM,    -- mapping to mileage group (origin-destination ID)

  /* Inline subquery DIM_OD
     - This derived table computes MILES for origin-destination pairs using:
       A = aggregated miles by ORGN_DSTNTN_MLG_ID and SBDVSN
       B = break-down table providing min/max mile ranges
       C = sub-division zone mapping
     - The CASE block handles edge cases and assigns a MILES value.
     - Keep the original logic intact; comments added for clarity.
  */
  (
    SELECT
      A.ORGN_DSTNTN_MLG_ID,
      A.SBDVSN_NM      AS SBDVSN_NM,
      A.SBDVSN_NBR     AS SBDVSN_NBR,
      C.REGION,
      C.ZONE,
      C.DIVISION_NM,
      /* Complex CASE to compute MILES with edge-case handling.
         - If there's only one row in partition (COUNT(*) OVER(...) = 1), use MILES directly.
         - Else if B.MIN_MILES falls within C's low/high range, compute distance using HIGH_TRACK_MILE and MIN_MILES
           with additional safeguards to avoid negative/overshoot values.
         - Else use alternative calculation referencing C.LOW_TRACK_MILE.
         (This mirrors original nested CASE logic; preserved exactly.)
      */
      CASE
        WHEN COUNT(*) OVER (PARTITION BY A.ORGN_DSTNTN_MLG_ID, A.SBDVSN_NBR) = 1
          THEN MILES

        WHEN (B.MIN_MILES >= C.LOW_TRACK_MILE AND B.MIN_MILES <= C.HIGH_TRACK_MILE) THEN
          CASE
            WHEN ABS(HIGH_TRACK_MILE - MIN_MILES) > MILES THEN
              CASE
                WHEN ABS((ABS(HIGH_TRACK_MILE - MILES)) - MILES) > MILES
                  THEN MILES
                ELSE ABS((ABS(HIGH_TRACK_MILE - MILES)) - MILES)
              END
            ELSE ABS(HIGH_TRACK_MILE - MIN_MILES)
          END

        ELSE
          CASE
            WHEN (ABS(ABS(C.LOW_TRACK_MILE - MIN_MILES) - MILES)) > MILES THEN 0
            ELSE ABS(ABS(C.LOW_TRACK_MILE - MIN_MILES) - MILES)
          END
      END AS MILES

    FROM
      (
        -- A: aggregated miles by origin-destination mlg id and subdivision
        SELECT
          ORGN_DSTNTN_MLG_ID,
          NVL(SBDVSN_NM, 'X') AS SBDVSN_NM,
          NVL(SBDVSN_NBR, 0)  AS SBDVSN_NBR,
          SUM(MILES) AS MILES
        FROM DM_DIM.GIS_ORGN_DSTNTN_MLG_BY_STPRV
        WHERE ACTV_IND = 'Y'
        GROUP BY ORGN_DSTNTN_MLG_ID, SBDVSN_NBR, SBDVSN_NM
      ) A

    LEFT JOIN
      (
        -- B: breakdown table with min/max miles and related keys
        SELECT
          MIN(ORGN_RFRNC_LOW_MLS_QTY) AS MIN_MILES,
          MAX(DSTN_RFRNC_LOW_MLS_QTY) AS MAX_MILES,
          ORGN_SBDVSN_NBR,
          ORGN_SBDVSN_NM,
          ORGN_DSTNTN_MLG_ID,
          DSTNTN_SBDVSN_NBR
        FROM DM_DIM.GIS_ORGN_DSTNTN_MLG_BRKDN
        GROUP BY ORGN_DSTNTN_MLG_ID, ORGN_SBDVSN_NBR, ORGN_SBDVSN_NM, DSTNTN_SBDVSN_NBR
      ) B
      ON A.ORGN_DSTNTN_MLG_ID = B.ORGN_DSTNTN_MLG_ID
         AND A.SBDVSN_NBR = B.ORGN_SBDVSN_NBR
         AND A.SBDVSN_NBR = B.DSTNTN_SBDVSN_NBR

    LEFT JOIN
      DM_DIM.DIM_SUBDVSN_ZONE C
      ON A.SBDVSN_NBR = C.SBDVSN_NBR
         AND C.SBDVSN_NBR = B.DSTNTN_SBDVSN_NBR

    -- filter for rows where the min/max miles fall into zone ranges
    AND ((B.MIN_MILES >= C.LOW_TRACK_MILE AND B.MIN_MILES < C.HIGH_TRACK_MILE)
         OR (B.MAX_MILES >= C.LOW_TRACK_MILE AND B.MAX_MILES <= C.HIGH_TRACK_MILE))

  ) DIM_OD,  -- end of inline DIM_OD derived table

  DM_DIM.DIM_SUBDVSN DIM_SBDVSN

WHERE
  -- Link fact row to origin-destination mapping and derived miles
  BMR1.DIM_GIS_ORG_DST_PAIR_MLG_ID = DIM_ODM.ORGN_DSTNTN_MLG_ID
  AND DIM_ODM.ORGN_DSTNTN_MLG_ID = DIM_OD.ORGN_DSTNTN_MLG_ID

  -- Link to train master
  AND BMR1.DIM_TRAIN_ID = DIM_TRAIN.DIM_TRAIN_ID

  -- Link to departure / arrival locations
  AND BMR1.DIM_DEPARTURE_LCTN_ID = DIM_DEPART.DIM_LOCATION_ID
  AND BMR1.DIM_ARRIVAL_LCTN_ID   = DIM_ARRIVE.DIM_LOCATION_ID

  -- Link to equipment
  AND BMR1.DIM_EQUIPMENT_ID = DIM_EQUIP.DIM_EQUIPMENT_ID

  -- Link to locomotive (outer join in original SQL: (+) was used)
  -- In the original code DIM_LOCO was optionally joined (some BMR rows might not have a loco)
  AND BMR1.DIM_LOCOMOTIVE_ID = DIM_LOCO.DIM_LOCOMOTIVE_ID (+)

  -- Link OD sub-division number to subdivision dimension
  AND DIM_OD.SBDVSN_NBR = DIM_SBDVSN.SBDVSN_NBR

  -- Exclude equipment group EOTD (equipment types not of interest)
  AND NVL(DIM_EQUIP.EQPMNT_GROUP_CD, 'x') NOT IN ('EOTD')

  /* Incremental time window:
     - Keep rows received in SADB between (IncrLoadStart - 30 days) and IncrLoadEnd
     - This keeps a rolling 30-day lookback to capture any late-arriving rows
  */
  AND BMR1.SADB_RECEIVING_DT BETWEEN TRUNC(TO_DATE('$$IncrLoadStart', 'DD-Mon-YYYY') - 30)
                                 AND TRUNC(TO_DATE('$$IncrLoadEnd', 'DD-Mon-YYYY'))

/* NOTE / commented alternatives from original SQL:
   - the original file had other alternative date filters commented out (e.g. sysdate -30 etc.)
   - keep the active one above as it matches the provided original logic
*/

GROUP BY
  -- Group by all non-aggregated columns selected earlier
  BMR1.ARRIVAL_TMS,
  BMR1.DEPARTURE_TMS,
  BMR1.SADB_RECEIVING_DT,
  DIM_TRAIN.TITAN_NBR,
  BMR1.TRAIN_NM,
  DIM_TRAIN.TRAIN_KIND_CD,
  DIM_TRAIN.TRAIN_TYPE_CD,
  BMR1.CONSIST_NBR,
  DIM_DEPART.FSAC_CD,
  DIM_ARRIVE.FSAC_CD,
  -- DIM_OD.run_nbr (commented out in original)
  DIM_OD.SBDVSN_NM,
  COALESCE(DIM_OD.REGION, DIM_SBDVSN.REGION),
  COALESCE(DIM_OD.DIVISION_NM, DIM_SBDVSN.DIVISION_NM),
  COALESCE(DIM_OD.ZONE, DIM_SBDVSN.ZONE),
  DIM_ODM.MILES;
