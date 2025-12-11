with  cnst_d as (

SELECT /*+ MATERIALIZE */ *

FROM atm_train.train_cnst_dtl_rail_eqpt

--where  edw_update_tms between to_date('11-mar-2020','DD-Mon-YYYY')-10 and  to_date('13-mar-2020','DD-Mon-YYYY') +30

where edw_update_tms between to_date('$$IncrLoadStart','DD-Mon-YYYY')-10 and  to_date('$$IncrLoadEnd','DD-Mon-YYYY') +30

),

wyb as (

SELECT *

                    FROM (SELECT cycle_serial_nbr, mark_cd, eqpun_nbr, origin_scac_cd, origin_fsac_cd, dstntn_scac_cd, dstntn_fsac_cd,

                                 shpr_cprs_cstmr_id, cnsgn_cprs_cstmr_id, stcc_cd,

                                 wybl_cntnt_weight_qty, wybl_create_local_tms,

                                 wybl_nbr, method_of_pymnt_cd,

                                 stnwyb_msg_vrsn_id,CPR_EQPMT_POOL_ID,

                                 MAX(stnwyb_msg_vrsn_id) OVER (PARTITION BY cycle_serial_nbr) max_stnwyb_msg_vrsn_id

                          FROM atm_shpmt.stnwyb_msg_dn

                          WHERE wybl_create_local_tms between to_date('$$IncrLoadStart','DD-Mon-YYYY')-30 and  to_date('$$IncrLoadEnd','DD-Mon-YYYY')

                          AND crnt_vrsn_cd = 'Y')

                    WHERE stnwyb_msg_vrsn_id = max_stnwyb_msg_vrsn_id

)

SELECT

rownum as fact_cnst_mvmnt_id,

       mark_cd, eqpun_nbr,

       load_empty_ind, sqnc_nbr,

       tare_weight_tons_qty, net_weight_tons_qty,

       lead_lcmtv_ind, lcmtv_cnst_status_cd,

       train_nm, source_system_train_cnst_nbr, start_event_cd,

       start_tms, start_time_zone, start_scac_cd, start_fsac_cd, rcvd_from_scac_cd,

       end_event_cd, end_tms, end_time_zone, end_scac_cd, end_fsac_cd, dlvr_to_scac_cd,

       cnst_origin_scac_cd, cnst_origin_fsac_cd, cnst_dstntn_scac_cd, cnst_dstntn_fsac_cd,

       origin_scac_cd, origin_fsac_cd, dstntn_scac_cd, dstntn_fsac_cd,

       shpr_cprs_cstmr_id, cnsgn_cprs_cstmr_id, stcc_cd,

       wybl_cntnt_weight_qty, wybl_create_local_tms,

       wybl_nbr, method_of_pymnt_cd, tyes_train_id, DECODE(NVL(end_titan_nbr,0), 0, start_titan_nbr, end_titan_nbr) titan_nbr,

       lcmtv_lead_trail_ind, sadb_receiving_dt, edw_receiving_dt, null as row_source,CPR_EQPMT_POOL_ID,start_est_tms,end_est_tms

FROM (SELECT DISTINCT sub_division_nm, mark_cd, eqpun_nbr, load_empty_ind, sqnc_nbr,

             tare_weight_tons_qty, net_weight_tons_qty, lead_lcmtv_ind,

             lcmtv_cnst_status_cd, NVL(train_nm, 'UNDEFINE') train_nm, source_system_train_cnst_nbr, start_event_cd, start_tms, start_time_zone,

             'CPRS' AS start_scac_cd,

             start_fsac_cd, rcvd_from_scac_cd, end_event_cd, end_tms, end_time_zone, start_est_tms,end_est_tms,

             'CPRS' AS end_scac_cd,

             end_fsac_cd, dlvr_to_scac_cd,

             cnst_origin_scac_cd, cnst_origin_fsac_cd, cnst_dstntn_scac_cd, cnst_dstntn_fsac_cd,

             origin_scac_cd, origin_fsac_cd, dstntn_scac_cd, dstntn_fsac_cd,

             shpr_cprs_cstmr_id, cnsgn_cprs_cstmr_id, stcc_cd, wybl_cntnt_weight_qty,

             wybl_create_local_tms, wybl_nbr, method_of_pymnt_cd,CPR_EQPMT_POOL_ID,

             tyes_train_id, start_titan_nbr, end_titan_nbr, lcmtv_lead_trail_ind,

             sadb_receiving_dt, edw_receiving_dt

      FROM (SELECT mvmnt.mvmnt_type, mvmnt.mark_cd, mvmnt.eqpun_nbr, mvmnt.train_nm,

                   mvmnt.source_system_train_cnst_nbr, mvmnt.load_empty_ind,

                   mvmnt.start_tms, mvmnt.end_tms, mvmnt.start_est_tms, mvmnt.end_est_tms, mvmnt.start_fsac_cd, mvmnt.end_fsac_cd,

                   mvmnt.start_event_cd, mvmnt.end_event_cd, mvmnt.start_time_zone,

                   mvmnt.end_time_zone, mvmnt.offline_scac_cd rcvd_from_scac_cd, mvmnt.next_offline_scac_cd dlvr_to_scac_cd,

                   mvmnt.cnst_origin_scac_cd, mvmnt.cnst_origin_fsac_cd, mvmnt.cnst_dstntn_scac_cd, mvmnt.cnst_dstntn_fsac_cd,

                   mvmnt.tyes_train_id, mvmnt.end_titan_nbr, mvmnt.start_titan_nbr, mvmnt.sadb_receiving_dt, mvmnt.edw_receiving_dt,

                   cnst_d.sqnc_nbr, cnst_d.tare_weight_tons_qty, cnst_d.net_weight_tons_qty,

                   cnst_d.lead_lcmtv_ind, cnst_d.lcmtv_cnst_status_cd, wyb.stnwyb_msg_vrsn_id,

                   wyb.origin_scac_cd, wyb.origin_fsac_cd, wyb.dstntn_scac_cd, wyb.dstntn_fsac_cd,

                   wyb.shpr_cprs_cstmr_id, wyb.cnsgn_cprs_cstmr_id, wyb.stcc_cd,

                   wyb.wybl_cntnt_weight_qty, wyb.wybl_create_local_tms,

                   wyb.wybl_nbr, wyb.method_of_pymnt_cd,wyb.CPR_EQPMT_POOL_ID,

                   CASE WHEN mvmnt.mvmnt_type = 'LCMTV' THEN

                        CASE WHEN cnst_d.lead_lcmtv_ind = 'Y' THEN 'LD'

                             WHEN cnst_d.lcmtv_cnst_status_cd IN ('DEAD','DEADHEAD') THEN 'DH'

                             ELSE 'LT'

                        END

                            ELSE 'NA'

                   END lcmtv_lead_trail_ind,

                   lctn.sub_division_nm

            FROM (SELECT  mark_cd, eqpun_nbr, source_system_train_cnst_nbr, cycle_serial_nbr, report_est_tms end_est_tms,

                          lag(report_tms) OVER (PARTITION BY mark_cd, eqpun_nbr, NVL(titan_nbr,0), cycle_serial_nbr  ORDER BY report_est_tms) start_tms,

                          report_tms end_tms,

                                                                                                   lag(report_est_tms) OVER (PARTITION BY mark_cd, eqpun_nbr, NVL(titan_nbr,0), cycle_serial_nbr  ORDER BY report_est_tms) start_est_tms,

                           lag(fsac_cd) OVER (PARTITION BY mark_cd, eqpun_nbr, NVL(titan_nbr,0), cycle_serial_nbr  ORDER BY report_est_tms) start_fsac_cd,

                           fsac_cd end_fsac_cd,

                           mvmnt_type, load_empty_ind, train_cnst_smry_id, train_cnst_smry_vrsn_nbr,

                           lag(aar_stndrd_event_cd) OVER (PARTITION BY mark_cd, eqpun_nbr, NVL(titan_nbr,0), cycle_serial_nbr  ORDER BY report_est_tms) start_event_cd,

                           aar_stndrd_event_cd end_event_cd,

                           lag(report_time_zone) OVER (PARTITION BY mark_cd, eqpun_nbr, NVL(titan_nbr,0), cycle_serial_nbr  ORDER BY report_est_tms) start_time_zone,

                           report_time_zone end_time_zone, offline_scac_cd,

                           lead(offline_scac_cd) OVER (PARTITION BY mark_cd, eqpun_nbr, NVL(titan_nbr,0), cycle_serial_nbr  ORDER BY report_est_tms) next_offline_scac_cd,

                           cnst_origin_scac_cd, cnst_origin_fsac_cd, cnst_dstntn_scac_cd, cnst_dstntn_fsac_cd, titan_nbr end_titan_nbr,

                           lag(titan_nbr) OVER (PARTITION BY mark_cd, eqpun_nbr, NVL(titan_nbr,0), cycle_serial_nbr  ORDER BY report_est_tms) start_titan_nbr,

                           train_nm,

                           tyes_train_id, sadb_receiving_dt, edw_receiving_dt

                  FROM dm_bmr.mvmnt_cnst_event_prcs

                  WHERE report_est_tms between to_date('$$IncrLoadStart','DD-Mon-YYYY') and  to_date('$$IncrLoadEnd','DD-Mon-YYYY')) mvmnt,

                   cnst_d, wyb,

                  dm_dim.dim_location lctn

            WHERE mvmnt.train_cnst_smry_id = cnst_d.train_cnst_smry_id(+)

            AND mvmnt.train_cnst_smry_vrsn_nbr = cnst_d.train_cnst_smry_vrsn_nbr(+)

            AND MVMNT.MARK_CD = CNST_D.MARK_CD(+)

            AND mvmnt.eqpun_nbr = cnst_d.eqpun_nbr(+)

            AND mvmnt.mark_cd = wyb.mark_cd(+)

            AND mvmnt.eqpun_nbr = wyb.eqpun_nbr(+)

            AND mvmnt.cycle_serial_nbr = wyb.cycle_serial_nbr(+)

            AND 'CPRS' = lctn.scac_cd(+)

            AND mvmnt.end_fsac_cd = lctn.fsac_cd(+)

            AND mvmnt.start_event_cd <> 'ICHD'

            AND mvmnt.start_fsac_cd <> mvmnt.end_fsac_cd)

) a

WHERE a.train_nm <> 'UNDEFINE'

AND NOT EXISTS (SELECT 'true'

                 FROM dm_bmr.mvmnt_cnst_event_prcs cep

                  WHERE

                                                                  cep.mark_cd = a.mark_cd

                  AND cep.eqpun_nbr = a.eqpun_nbr

AND cep.report_est_tms between to_date('$$IncrLoadStart','DD-Mon-YYYY')-10 and  to_date('$$IncrLoadEnd','DD-Mon-YYYY')

                  AND cep.report_tms between a.start_tms and a.end_tms

                  AND cep.train_nm <> a.train_nm)