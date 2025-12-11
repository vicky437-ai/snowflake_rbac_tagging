SELECT /*+ opt_param('optimizer_features_enable' '11.2.0.4') */  rownum as FACT_CNST_MVMNT_ID,

       NVL(FCM.DIM_TRAIN_ID, -999999) as DIM_TRAIN_ID,

       NVL(FCM.DIM_LOCOMOTIVE_ID, -111111) as DIM_LOCOMOTIVE_ID,

       FCM.DIM_DEPARTURE_LCTN_ID,

       FCM.DIM_ARRIVAL_LCTN_ID,

       NVL(FCM.DIM_EQUIPMENT_ID, -111111) as DIM_EQUIPMENT_ID,

       -999999 as DIM_ORIGIN_LCTN_ID,

       -999999 as DIM_DSTNTN_LCTN_ID,

       -999999 as DIM_SHPR_CSTMR_ID,

       -999999 as DIM_CNSGN_CSTMR_ID,

       -999999 as DIM_COMMODITY_ID,

   FCM.DIM_ORG_DST_PAIR_MLG_ID,

       FCM.CONSIST_NBR,

       FCM.DEPARTURE_TMS,

       FCM.DEPARTURE_TIME_ZONE,

       FCM.ARRIVAL_TMS,

       FCM.ARRIVAL_TIME_ZONE,

       NVL(FCM.sqnc_nbr,1) AS CONSIST_SQNC,

       FCM.TARE_TONS,

       FCM.CONTENT_TONS,

       NVL(FCM.LCMTV_LEAD_TRAIL_IND, 'LD') as LCMTV_LEAD_TRAIL_IND,

       NULL AS WYBL_TMS,

      NULL AS WYBL_NBR,

      NULL AS RECIEVE_FROM_SCAC,

       NULL AS DELIVER_TO_SCAC,

      NULL AS WYBL_WEIGHT,

       NULL AS PREPAID_IND,

       FCM.DIM_ARRIVAL_DT_ID,

       FCM.SADB_RECEIVING_DT,

       FCM.EDW_RECEIVING_DT,

       FCM.DIM_SADB_RECEIVING_DT_ID,

       FCM.DIM_EDW_RECEIVING_DT_ID,

       FCM.LOAD_EMPTY_IND,

       NVL(FCM.MARK_CD, 'CP') as MARK_CD,

       NVL(FCM.EQPUN_NBR, '0000002001') as EQPUN_NBR,

       FCM.TRAIN_NM,

       'BMR Insert Lead LCMTV workflow' AS ROW_SOURCE,

      ROUND(FCM.ARRIVAL_TMS-FCM.DEPARTURE_TMS, 6) as run_time,

       FCM.DIM_GIS_ORG_DST_PAIR_MLG_ID,

                  FCM.CPR_EQPMT_POOL_ID,

                  FCM.DEPARTURE_EST_TMS,

                  FCM.ARRIVAL_EST_TMS

      FROM (SELECT FCT.FACT_CNST_MVMNT_ID,

                fct.dim_train_id,

               lcmtv.dim_locomotive_id,

               fct.dim_departure_lctn_id,

               fct.dim_arrival_lctn_id,

               fct.DIM_ORG_DST_PAIR_MLG_ID,

               eqpt.dim_equipment_id,

               fct.consist_nbr,

               fct.departure_tms,

               fct.departure_time_zone,

               fct.arrival_tms,

               fct.arrival_time_zone,

               cnst.sqnc_nbr,

               fct.sadb_receiving_dt,

               NULL load_empty_ind,

               cnst.tare_weight_tons_qty tare_tons,

               cnst.net_weight_tons_qty content_tons,

               CASE WHEN cnst.lead_lcmtv_ind = 'Y' THEN 'LD'

                    WHEN cnst.lcmtv_cnst_status_cd IN ('DEAD','DEADHEAD') THEN 'DH'

                    WHEN cnst.mark_cd IS NOT NULL THEN 'LT'

               END lcmtv_lead_trail_ind,

               fct.dim_arrival_dt_id,

               fct.dim_sadb_receiving_dt_id,

               fct.edw_receiving_dt,

               fct.dim_edw_receiving_dt_id,

               cnst.mark_cd,

               cnst.eqpun_nbr,

               fct.train_nm,

                fct.dim_gis_org_dst_pair_mlg_id,

                                  fct.cpr_eqpmt_pool_id,

                                  fct.DEPARTURE_EST_TMS,

                                  fct.ARRIVAL_EST_TMS

              -- fct.dim_od_ts_pairs_id,

              -- 'CNST' row_source

        FROM dm_bmr.bmr_fact_cnst_mvmnt fct,

             dm_bmr.mvmnt_cnst_event_prcs stg,

             (SELECT *

              FROM atm_train.train_cnst_dtl_rail_eqpt

              WHERE SUBSTR(aar_car_type_cd,1,1) = 'D') cnst,

             (SELECT *

              FROM dm_dim.dim_locomotive

              WHERE edw_current_flg = 'Y') lcmtv,

              (SELECT *

              FROM dm_dim.dim_equipment

              WHERE edw_current_flg = 'Y') eqpt

        WHERE fct.mark_cd = stg.mark_cd

        AND fct.eqpun_nbr = stg.eqpun_nbr

        AND fct.arrival_tms = stg.report_tms

        AND stg.train_cnst_smry_id = cnst.train_cnst_smry_id(+)

        AND stg.train_cnst_smry_vrsn_nbr = cnst.train_cnst_smry_vrsn_nbr(+)

        AND cnst.mark_cd = lcmtv.mark_cd(+)

        AND cnst.eqpun_nbr = lcmtv.eqpun_nbr(+)

        AND cnst.mark_cd = eqpt.mark_cd(+)

        AND cnst.eqpun_nbr = eqpt.eqpun_nbr(+)

        AND fct.fact_cnst_mvmnt_id in (SELECT MIN(fact_cnst_mvmnt_id) fact_cnst_mvmnt_id

                                        FROM (SELECT bmr.train_nm,

                                                     bmr.dim_train_id,

                                                     bmr.dim_departure_lctn_id,

                                                     bmr.dim_arrival_lctn_id,

                                                     bmr.consist_nbr,

                                                     bmr.lcmtv_lead_trail_ind,

                                                     LISTAGG(NVL(bmr.lcmtv_lead_trail_ind,'x'), ',') WITHIN GROUP (ORDER BY bmr.dim_train_id, bmr.train_nm) OVER (PARTITION BY bmr.train_nm, bmr.dim_train_id, bmr.dim_departure_lctn_id, bmr.dim_arrival_lctn_id, bmr.consist_nbr)  AS lcmtv_lead_train_ind_list,

                                                     MIN(bmr.fact_cnst_mvmnt_id) fact_cnst_mvmnt_id,

                                                     COUNT(*) eqpt_count

                                              FROM dm_bmr.bmr_fact_cnst_mvmnt bmr

                                              WHERE  trunc(bmr.sadb_receiving_dt) between to_date('$$IncrLoadStart','DD-Mon-YYYY') AND to_date('$$IncrLoadEnd','DD-Mon-YYYY')

                                              GROUP BY bmr.train_nm,

                                                       bmr.dim_train_id,

                                                       bmr.dim_departure_lctn_id,

                                                       bmr.dim_arrival_lctn_id,

                                                       bmr.consist_nbr,

                                                       bmr.lcmtv_lead_trail_ind)

                                        WHERE INSTR(lcmtv_lead_train_ind_list, 'LD') = 0

                                        GROUP BY dim_train_id,

                                                          train_nm,

                                                          dim_departure_lctn_id,

                                                         dim_arrival_lctn_id,

                                                         consist_nbr)) FCM

WHERE NOT EXISTS (SELECT 'true'

                FROM dm_bmr.bmr_fact_cnst_mvmnt a

                WHERE a.mark_cd = fcm.mark_cd

                AND a.eqpun_nbr = fcm.eqpun_nbr

                AND a.train_nm = fcm.train_nm

                AND fcm.departure_tms > a.departure_tms AND fcm.arrival_tms < a.arrival_tms)

ORDER BY DIM_TRAIN_ID, TRAIN_NM, CONSIST_NBR, RUN_TIME DESC
