SELECT *

FROM (SELECT 'EQPMV' mvmnt_type, run_nbr, mark_cd,  eqpun_nbr, load_empty_ind, cycle_serial_nbr, train_nm, source_system_train_cnst_nbr, lead_lcmtv_mark_cd, lead_lcmtv_eqpun_nbr, aar_stndrd_event_cd, report_tms, report_time_zone, fsac_cd,  offline_scac_cd, cnst_origin_scac_cd, cnst_origin_fsac_cd, cnst_dstntn_scac_cd, cnst_dstntn_fsac_cd, titan_nbr, tyes_train_id, cnst_chng_pt_event_tms as last_rprtd_event_tms,  cnst_chng_rprtng_tm_zn_cd as last_rprtd_rprtng_tm_zn_cd, train_cnst_smry_id, train_cnst_smry_vrsn_nbr, MAX(train_cnst_smry_id) OVER (PARTITION BY event_id) max_train_cnst_smry_id, MAX(train_cnst_smry_vrsn_nbr) OVER (PARTITION BY train_cnst_smry_id, event_id) max_train_cnst_smry_vrsn_nbr, sadb_receiving_dt, edw_receiving_dt, report_est_tms, event_id

      FROM (SELECT   cnst.run_nbr, cnst.mark_cd, cnst.eqpun_nbr, cnst.load_empty_ind, cnst.cycle_serial_nbr, cnst.train_nm, cnst.source_system_train_cnst_nbr, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, cnst.aar_stndrd_event_cd, cnst.report_tms, cnst.report_time_zone, DECODE(cnst.aar_stndrd_event_cd, 'ICHR', cnst.fsac_cd, cnst.rpt_fsac_cd) fsac_cd, DECODE(cnst.aar_stndrd_event_cd, 'ICHR', cnst.rpt_scac_cd, cnst.scac_cd) offline_scac_cd, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.titan_nbr, cnst.tyes_train_id, cnst.cnst_chng_pt_event_tms, MIN(cnst.cnst_chng_pt_event_tms) OVER (PARTITION BY cnst.titan_nbr, cnst.mark_cd, cnst.eqpun_nbr, cnst.cycle_serial_nbr) min_cnst_chng_pt_event_tms, cnst.cnst_chng_rprtng_tm_zn_cd, cnst.train_cnst_smry_id, cnst.train_cnst_smry_vrsn_nbr, cnst.event_id, cnst.sadb_receiving_dt, cnst.edw_receiving_dt, cnst.report_est_tms

            FROM (SELECT /*+ full(erme) */ erme.event_id, erme.mark_cd, eeet.aar_stndrd_event_cd, erme.cycle_serial_nbr, erme.rpt_fsac_cd, erme.fsac_cd, erme.rpt_scac_cd, erme.scac_cd, erme.eqpun_nbr, erme.report_tms, erme.report_time_zone, erme.load_empty_ind,

                         CASE WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'PT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*5) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*4) THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*2)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*3)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'MT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*4) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*3) THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*2)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'CT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*3) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*2) THEN

                                 cnst.cnst_chng_pt_event_tms

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'ET' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*2) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*1) THEN

                                 cnst.cnst_chng_pt_event_tms+((1/24)*1)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'ST' THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                         END cnst_chng_pt_event_tms,

                        cnst.cnst_chng_rprtng_tm_zn_cd, cnst.run_nbr, cnst.train_nm, cnst.source_system_train_cnst_nbr, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.train_cnst_smry_id,  cnst.train_cnst_smry_vrsn_nbr, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, erme.tyes_train_id, erme.titan_nbr titan_nbr, TRUNC(erme.record_update_tms) sadb_receiving_dt,

                        TRUNC(erme.edw_update_tms) edw_receiving_dt, erme.estrn_stnd_report_tms report_est_tms

                  FROM atm_eqpmv.eqpmv_rfeqp_mvmnt_event erme,

                       atm_eqpmv.eqpmv_eqpmt_event_type eeet,

                       atm_train.train_cnst_smry cnst

                  WHERE erme.eqpmt_event_type_id = eeet.eqpmt_event_type_id AND erme.titan_nbr = cnst.titan_nbr AND erme.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND erme.estrn_stnd_report_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND erme.estrn_stnd_report_tms < to_date('$$IncrLoadEnd','DD-Mon-YYYY') AND cnst.cnst_chng_pt_event_tms  >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND cnst.cnst_chng_pt_event_tms < to_date('$$IncrLoadEnd','DD-Mon-YYYY') AND cnst.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND eeet.aar_stndrd_event_cd IN ('ARIL','ARRI','DFLC','ICHR','ICHD') AND cnst.chng_cnst_smry_status_cd IN ('ARRIVED','DEPARTED','PASSING-OUT','PASSING-IN')

                  --AND cnst.run_nbr IS NOT NULL

                  AND erme.event_status_cd <> 'D' AND DECODE(eeet.aar_stndrd_event_cd, 'ICHR', erme.scac_cd, erme.rpt_scac_cd) = 'CPRS') cnst, atm_train.train_cnst_dtl_rail_eqpt cd

            WHERE cnst.train_cnst_smry_id = cd.train_cnst_smry_id AND cnst.train_cnst_smry_vrsn_nbr = cd.train_cnst_smry_vrsn_nbr AND cnst.mark_cd = cd.mark_cd AND cnst.eqpun_nbr = cd.eqpun_nbr AND cnst.cycle_serial_nbr = cd.cycle_serial_nbr AND cd.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY'))

      WHERE report_tms >= cnst_chng_pt_event_tms OR cnst_chng_pt_event_tms = min_cnst_chng_pt_event_tms)

WHERE train_cnst_smry_id = max_train_cnst_smry_id AND train_cnst_smry_vrsn_nbr = max_train_cnst_smry_vrsn_nbr

UNION ALL

SELECT *

FROM (SELECT 'LCMTV' mvmnt_type, run_nbr, mark_cd,  eqpun_nbr, load_empty_ind, cycle_serial_nbr, train_nm, source_system_train_cnst_nbr, lead_lcmtv_mark_cd, lead_lcmtv_eqpun_nbr, aar_stndrd_event_cd, report_tms, report_time_zone, fsac_cd, offline_scac_cd, cnst_origin_scac_cd, cnst_origin_fsac_cd, cnst_dstntn_scac_cd, cnst_dstntn_fsac_cd, titan_nbr, tyes_train_id, cnst_chng_pt_event_tms as last_rprtd_event_tms, cnst_chng_rprtng_tm_zn_cd as last_rprtd_rprtng_tm_zn_cd, train_cnst_smry_id, train_cnst_smry_vrsn_nbr, MAX(train_cnst_smry_id) OVER (PARTITION BY event_id) max_train_cnst_smry_id, MAX(train_cnst_smry_vrsn_nbr) OVER (PARTITION BY train_cnst_smry_id, event_id) max_train_cnst_smry_vrsn_nbr, sadb_receiving_dt, edw_receiving_dt, report_est_tms, event_id

      FROM (SELECT  cnst.run_nbr, cnst.mark_cd, cnst.eqpun_nbr, cnst.load_empty_ind, cnst.cycle_serial_nbr, cnst.train_nm, cnst.source_system_train_cnst_nbr, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, cnst.aar_stndrd_event_cd, cnst.report_tms, cnst.report_time_zone, cnst.fsac_cd, null offline_scac_cd, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.titan_nbr, cnst.tyes_train_id, cnst.cnst_chng_pt_event_tms, MIN(cnst.cnst_chng_pt_event_tms) OVER (PARTITION BY cnst.titan_nbr, cnst.mark_cd, cnst.eqpun_nbr) min_cnst_chng_pt_event_tms, cnst.cnst_chng_rprtng_tm_zn_cd, cnst.train_cnst_smry_id, cnst.train_cnst_smry_vrsn_nbr, cnst.event_id, cnst.sadb_receiving_dt, cnst.edw_receiving_dt, cnst.report_est_tms

            FROM (SELECT /*+ full(lme) */ lme.event_id, lme.mark_cd, eeet.aar_stndrd_event_cd, null cycle_serial_nbr, lme.fsac_cd, lme.scac_cd, lme.eqpun_nbr,

                         CASE WHEN lme.report_time_zone_cd = 'PT' THEN

                            CASE WHEN lme.report_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*5) AND next_day(trunc(to_date('01-NOV-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*4) THEN

                                 lme.report_tms-((1/24)*2)

                                 ELSE

                                 lme.report_tms-((1/24)*3)

                             END

                            WHEN lme.report_time_zone_cd = 'MT' THEN

                            CASE WHEN lme.report_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*4) AND next_day(trunc(to_date('01-NOV-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*3) THEN

                                 lme.report_tms-((1/24)*1)

                                 ELSE

                                 lme.report_tms-((1/24)*2)

                             END

                            WHEN lme.report_time_zone_cd = 'CT' THEN

                            CASE WHEN lme.report_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*3) AND next_day(trunc(to_date('01-NOV-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*2) THEN

                                 lme.report_tms

                                 ELSE

                                 lme.report_tms-((1/24)*1)

                             END

                            WHEN lme.report_time_zone_cd = 'ET' THEN

                            CASE WHEN lme.report_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*2) AND next_day(trunc(to_date('01-NOV-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*1) THEN

                                 lme.report_tms+((1/24)*1)

                                 ELSE

                                 lme.report_tms

                             END

                            WHEN lme.report_time_zone_cd = 'ST' THEN

                                 lme.report_tms-((1/24)*1)

                         END report_tms,

                         lme.report_time_zone_cd report_time_zone,

                         null load_empty_ind,

                         CASE WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'PT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*5) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*4) THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*2)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*3)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'MT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*4) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*3) THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*2)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'CT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*3) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*2) THEN

                                 cnst.cnst_chng_pt_event_tms

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'ET' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*2) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*1) THEN

                                 cnst.cnst_chng_pt_event_tms+((1/24)*1)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'ST' THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                         END cnst_chng_pt_event_tms,

                        cnst.cnst_chng_rprtng_tm_zn_cd, cnst.run_nbr, cnst.train_nm, cnst.source_system_train_cnst_nbr, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.train_cnst_smry_id, cnst.train_cnst_smry_vrsn_nbr, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, lme.tyes_train_id, TO_CHAR(lme.titan_nbr) titan_nbr, TRUNC(lme.record_update_tms) sadb_receiving_dt, TRUNC(lme.edw_update_tms) edw_receiving_dt, lme.report_tms report_est_tms

                  FROM atm_lcmtv.lcmtv_mvmnt_event lme,

                       atm_eqpmv.eqpmv_eqpmt_event_type eeet,

                       atm_train.train_cnst_smry cnst

                  WHERE lme.eqpmt_event_type_id = eeet.eqpmt_event_type_id AND lme.titan_nbr = cnst.titan_nbr AND lme.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND lme.report_tms >= to_date('$$IncrLoadStart','DD-Mon-YYYY') AND lme.report_tms < to_date('$$IncrLoadEnd','DD-Mon-YYYY') AND cnst.cnst_chng_pt_event_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY')  and cnst.cnst_chng_pt_event_tms < to_date('$$IncrLoadEnd','DD-Mon-YYYY') AND eeet.aar_stndrd_event_cd IN ('ARIL','ARRI','DFLC','ICHR','ICHD') AND cnst.chng_cnst_smry_status_cd IN ('ARRIVED', 'DEPARTED','PASSING-OUT','PASSING-IN')

                  --AND cnst.run_nbr IS NOT NULL

                  AND lme.scac_cd = 'CPRS') cnst, atm_train.train_cnst_dtl_rail_eqpt cd

            WHERE cnst.train_cnst_smry_id = cd.train_cnst_smry_id AND cnst.train_cnst_smry_vrsn_nbr = cd.train_cnst_smry_vrsn_nbr AND cnst.mark_cd = cd.mark_cd AND cnst.eqpun_nbr = cd.eqpun_nbr AND cd.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY'))

      WHERE report_tms >= cnst_chng_pt_event_tms OR cnst_chng_pt_event_tms = min_cnst_chng_pt_event_tms)

WHERE train_cnst_smry_id = max_train_cnst_smry_id AND train_cnst_smry_vrsn_nbr = max_train_cnst_smry_vrsn_nbr

UNION ALL

SELECT *

FROM (SELECT 'EQPMV' mvmnt_type, run_nbr, mark_cd,  eqpun_nbr, load_empty_ind, cycle_serial_nbr, train_nm, source_system_train_cnst_nbr, lead_lcmtv_mark_cd, lead_lcmtv_eqpun_nbr, aar_stndrd_event_cd, report_tms, report_time_zone,

             fsac_cd,  offline_scac_cd, cnst_origin_scac_cd, cnst_origin_fsac_cd, cnst_dstntn_scac_cd, cnst_dstntn_fsac_cd, NVL(titan_nbr,0) titan_nbr, tyes_train_id, cnst_chng_pt_event_tms as last_rprtd_event_tms, cnst_chng_rprtng_tm_zn_cd as last_rprtd_rprtng_tm_zn_cd, train_cnst_smry_id, train_cnst_smry_vrsn_nbr, MAX(train_cnst_smry_id) OVER (PARTITION BY event_id) max_train_cnst_smry_id, MAX(train_cnst_smry_vrsn_nbr) OVER (PARTITION BY train_cnst_smry_id, event_id) max_train_cnst_smry_vrsn_nbr, sadb_receiving_dt, edw_receiving_dt, report_est_tms, event_id

      FROM (SELECT  cnst.run_nbr, cnst.mark_cd, cnst.eqpun_nbr, cnst.load_empty_ind, cnst.cycle_serial_nbr, cnst.train_nm, cnst.source_system_train_cnst_nbr, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, cnst.aar_stndrd_event_cd, cnst.report_tms, cnst.report_time_zone, DECODE(cnst.aar_stndrd_event_cd, 'ICHR', cnst.fsac_cd, cnst.rpt_fsac_cd) fsac_cd, DECODE(cnst.aar_stndrd_event_cd, 'ICHR', cnst.rpt_scac_cd, cnst.scac_cd) offline_scac_cd, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.titan_nbr, cnst.tyes_train_id, cnst.cnst_chng_pt_event_tms, MIN(cnst.cnst_chng_pt_event_tms) OVER (PARTITION BY cnst.titan_nbr, cnst.mark_cd, cnst.eqpun_nbr, cnst.cycle_serial_nbr) min_cnst_chng_pt_event_tms, cnst.cnst_chng_rprtng_tm_zn_cd, cnst.train_cnst_smry_id, cnst.train_cnst_smry_vrsn_nbr, cnst.event_id, cnst.sadb_receiving_dt, cnst.edw_receiving_dt, cnst.report_est_tms

            FROM (SELECT /*+ full(erme) */ erme.event_id, erme.mark_cd, eeet.aar_stndrd_event_cd, erme.cycle_serial_nbr, erme.rpt_fsac_cd, erme.fsac_cd, erme.rpt_scac_cd, erme.scac_cd, erme.eqpun_nbr, erme.report_tms, erme.report_time_zone, erme.load_empty_ind,

                         CASE WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'PT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*5) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*4) THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*2)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*3)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'MT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*4) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*3) THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*2)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'CT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*3) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*2) THEN

                                 cnst.cnst_chng_pt_event_tms

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'ET' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*2) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*1) THEN

                                 cnst.cnst_chng_pt_event_tms+((1/24)*1)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'ST' THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                         END cnst_chng_pt_event_tms,

                        cnst.cnst_chng_rprtng_tm_zn_cd, cnst.run_nbr, cnst.train_nm, cnst.source_system_train_cnst_nbr, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd,

                        cnst.train_cnst_smry_id,  cnst.train_cnst_smry_vrsn_nbr, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, erme.tyes_train_id, erme.titan_nbr titan_nbr, TRUNC(erme.record_update_tms) sadb_receiving_dt,

                        TRUNC(erme.edw_update_tms) edw_receiving_dt, erme.estrn_stnd_report_tms report_est_tms

                  FROM atm_eqpmv.eqpmv_rfeqp_mvmnt_event erme,

                       atm_eqpmv.eqpmv_eqpmt_event_type eeet,

   (SELECT cnst.train_cnst_smry_id, cnst.train_cnst_smry_vrsn_nbr, cnst_d.mark_cd, cnst_d.eqpun_nbr, cnst_d.cycle_serial_nbr, cnst.last_rprtd_scac_cd, cnst.last_rprtd_fsac_cd, cnst.last_rprtd_event_tms, cnst.lead_lcmtv_mark_cd,

                                 cnst.lead_lcmtv_eqpun_nbr, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd,cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.cnst_chng_pt_event_tms, cnst.cnst_chng_rprtng_tm_zn_cd, cnst.source_system_train_cnst_nbr, cnst.run_nbr, cnst.train_nm

                          FROM atm_train.train_cnst_dtl_rail_eqpt cnst_d,

                               atm_train.train_cnst_smry cnst

                          WHERE cnst.train_cnst_smry_id = cnst_d.train_cnst_smry_id AND cnst.train_cnst_smry_vrsn_nbr = cnst_d.train_cnst_smry_vrsn_nbr AND cnst.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY')

                          AND cnst.cnst_chng_pt_event_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND cnst.cnst_chng_pt_event_tms < to_date('$$IncrLoadEnd','DD-Mon-YYYY') AND cnst.chng_cnst_smry_status_cd IN ('ARRIVED','DEPARTED','PASSING-OUT','PASSING-IN') AND cnst_d.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY')) cnst

                  WHERE erme.eqpmt_event_type_id = eeet.eqpmt_event_type_id AND erme.mark_cd = cnst.mark_cd(+) AND erme.eqpun_nbr = cnst.eqpun_nbr(+) AND erme.cycle_serial_nbr = cnst.cycle_serial_nbr(+) AND erme.rpt_scac_cd = cnst.last_rprtd_scac_cd(+) AND erme.rpt_fsac_cd = cnst.last_rprtd_fsac_cd(+) AND erme.estrn_stnd_report_tms = cnst.last_rprtd_event_tms(+) AND erme.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY')

                  AND erme.estrn_stnd_report_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND erme.estrn_stnd_report_tms < to_date('$$IncrLoadEnd','DD-Mon-YYYY') AND eeet.aar_stndrd_event_cd IN ('ARIL','ARRI','DFLC','ICHR','ICHD') AND erme.titan_nbr is null AND erme.event_status_cd <> 'D' AND DECODE(eeet.aar_stndrd_event_cd, 'ICHR', erme.scac_cd, erme.rpt_scac_cd) = 'CPRS') cnst)

      WHERE report_tms >= NVL(cnst_chng_pt_event_tms, report_tms) OR cnst_chng_pt_event_tms = min_cnst_chng_pt_event_tms) WHERE NVL(train_cnst_smry_id,0) = NVL(max_train_cnst_smry_id,0) AND NVL(train_cnst_smry_vrsn_nbr,0) = NVL(max_train_cnst_smry_vrsn_nbr,0)

UNION ALL

SELECT *

FROM (SELECT 'LCMTV' mvmnt_type, run_nbr, mark_cd,  eqpun_nbr, load_empty_ind, cycle_serial_nbr, train_nm, source_system_train_cnst_nbr, lead_lcmtv_mark_cd, lead_lcmtv_eqpun_nbr, aar_stndrd_event_cd, report_tms, report_time_zone,

             fsac_cd, offline_scac_cd, cnst_origin_scac_cd, cnst_origin_fsac_cd, cnst_dstntn_scac_cd, cnst_dstntn_fsac_cd, NVL(titan_nbr,0) titan_nbr, tyes_train_id, cnst_chng_pt_event_tms as last_rprtd_event_tms, cnst_chng_rprtng_tm_zn_cd as last_rprtd_rprtng_tm_zn_cd, train_cnst_smry_id, train_cnst_smry_vrsn_nbr,
			 MAX(train_cnst_smry_id) OVER (PARTITION BY event_id) max_train_cnst_smry_id,            
			 MAX(train_cnst_smry_vrsn_nbr) OVER (PARTITION BY train_cnst_smry_id, event_id) max_train_cnst_smry_vrsn_nbr, 
			 
			 sadb_receiving_dt, edw_receiving_dt, report_est_tms, event_id

      FROM (SELECT  cnst.run_nbr, cnst.mark_cd, cnst.eqpun_nbr, cnst.load_empty_ind, cnst.cycle_serial_nbr, cnst.train_nm, cnst.source_system_train_cnst_nbr, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, cnst.aar_stndrd_event_cd, cnst.report_tms, cnst.report_time_zone, cnst.fsac_cd, null offline_scac_cd, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.titan_nbr, cnst.tyes_train_id, cnst.cnst_chng_pt_event_tms, MIN(cnst.cnst_chng_pt_event_tms) OVER (PARTITION BY cnst.titan_nbr, cnst.mark_cd, cnst.eqpun_nbr) min_cnst_chng_pt_event_tms, cnst.cnst_chng_rprtng_tm_zn_cd, cnst.train_cnst_smry_id, cnst.train_cnst_smry_vrsn_nbr, cnst.event_id, cnst.sadb_receiving_dt, cnst.edw_receiving_dt, cnst.report_est_tms

            FROM (SELECT /*+ full(lme) */ lme.event_id, lme.mark_cd, eeet.aar_stndrd_event_cd, null cycle_serial_nbr, lme.fsac_cd, lme.scac_cd, lme.eqpun_nbr,

                         CASE WHEN lme.report_time_zone_cd = 'PT' THEN

                            CASE WHEN lme.report_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*5) AND next_day(trunc(to_date('01-NOV-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*4) THEN

                                 lme.report_tms-((1/24)*2)

                                 ELSE

                                 lme.report_tms-((1/24)*3)

                             END

                            WHEN lme.report_time_zone_cd = 'MT' THEN

                            CASE WHEN lme.report_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*4) AND next_day(trunc(to_date('01-NOV-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*3) THEN

                                 lme.report_tms-((1/24)*1)

                                 ELSE

                                 lme.report_tms-((1/24)*2)

                             END

                            WHEN lme.report_time_zone_cd = 'CT' THEN

                            CASE WHEN lme.report_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*3) AND next_day(trunc(to_date('01-NOV-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*2) THEN

                                 lme.report_tms

                                 ELSE

                                 lme.report_tms-((1/24)*1)

                             END

                            WHEN lme.report_time_zone_cd = 'ET' THEN

                            CASE WHEN lme.report_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*2) AND next_day(trunc(to_date('01-NOV-'||to_char(lme.report_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*1) THEN

                                 lme.report_tms+((1/24)*1)

                                 ELSE

                                 lme.report_tms

                             END

                            WHEN lme.report_time_zone_cd = 'ST' THEN

                                 lme.report_tms-((1/24)*1)

                         END report_tms,

                         lme.report_time_zone_cd report_time_zone,

                         null load_empty_ind,

                         CASE WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'PT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*5) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*4) THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*2)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*3)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'MT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*4) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*3) THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*2)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'CT' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*3) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*2) THEN

                                 cnst.cnst_chng_pt_event_tms

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'ET' THEN

                            CASE WHEN cnst.cnst_chng_pt_event_tms BETWEEN next_day(trunc(to_date('01-MAR-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+7+((1/24)*2) AND next_day(trunc(to_date('01-NOV-'||to_char(cnst.cnst_chng_pt_event_tms,'YYYY'), 'DD-MON-YYYY'),'MON')-1,'sun')+((1/24)*1) THEN

                                 cnst.cnst_chng_pt_event_tms+((1/24)*1)

                                 ELSE

                                 cnst.cnst_chng_pt_event_tms

                             END

                            WHEN cnst.cnst_chng_rprtng_tm_zn_cd = 'ST' THEN

                                 cnst.cnst_chng_pt_event_tms-((1/24)*1)

                         END cnst_chng_pt_event_tms,

                        cnst.cnst_chng_rprtng_tm_zn_cd, cnst.run_nbr, cnst.train_nm, cnst.source_system_train_cnst_nbr, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.train_cnst_smry_id, cnst.train_cnst_smry_vrsn_nbr, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, lme.tyes_train_id, TO_CHAR(lme.titan_nbr) titan_nbr, TRUNC(lme.record_update_tms) sadb_receiving_dt, TRUNC(lme.edw_update_tms) edw_receiving_dt, lme.report_tms report_est_tms

                  FROM atm_lcmtv.lcmtv_mvmnt_event lme, atm_eqpmv.eqpmv_eqpmt_event_type eeet,

                       (SELECT cnst.train_cnst_smry_id, cnst.train_cnst_smry_vrsn_nbr, cnst_d.mark_cd, cnst_d.eqpun_nbr, cnst_d.cycle_serial_nbr, cnst.last_rprtd_scac_cd, cnst.last_rprtd_fsac_cd, cnst.last_rprtd_event_tms, cnst.lead_lcmtv_mark_cd, cnst.lead_lcmtv_eqpun_nbr, cnst.cnst_origin_scac_cd, cnst.cnst_origin_fsac_cd, cnst.cnst_dstntn_scac_cd, cnst.cnst_dstntn_fsac_cd, cnst.cnst_chng_pt_event_tms, cnst.cnst_chng_rprtng_tm_zn_cd, cnst.source_system_train_cnst_nbr, cnst.run_nbr, cnst.train_nm

                          FROM atm_train.train_cnst_dtl_rail_eqpt cnst_d,

                               atm_train.train_cnst_smry cnst

                          WHERE cnst.train_cnst_smry_id = cnst_d.train_cnst_smry_id AND cnst.train_cnst_smry_vrsn_nbr = cnst_d.train_cnst_smry_vrsn_nbr AND cnst.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY')

                          AND cnst.cnst_chng_pt_event_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND cnst.cnst_chng_pt_event_tms < to_date('$$IncrLoadEnd','DD-Mon-YYYY') AND cnst.chng_cnst_smry_status_cd IN ('ARRIVED','DEPARTED','PASSING-OUT','PASSING-IN') AND cnst_d.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY')) cnst

                  WHERE lme.eqpmt_event_type_id = eeet.eqpmt_event_type_id AND lme.mark_cd = cnst.mark_cd(+) AND lme.eqpun_nbr = cnst.eqpun_nbr(+) AND lme.scac_cd = cnst.last_rprtd_scac_cd(+) AND lme.fsac_cd = cnst.last_rprtd_fsac_cd(+) AND lme.report_tms = cnst.last_rprtd_event_tms(+) AND lme.titan_nbr IS NULL AND lme.edw_update_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND lme.report_tms >= to_date('$$IncrLoadStart', 'DD-Mon-YYYY') AND lme.report_tms < to_date('$$IncrLoadEnd','DD-Mon-YYYY') AND eeet.aar_stndrd_event_cd IN ('ARIL','ARRI','DFLC','ICHR','ICHD') AND lme.scac_cd = 'CPRS') cnst)

      WHERE report_tms >= NVL(cnst_chng_pt_event_tms, report_tms) OR cnst_chng_pt_event_tms = min_cnst_chng_pt_event_tms) WHERE NVL(train_cnst_smry_id,0) = NVL(max_train_cnst_smry_id,0) AND NVL(train_cnst_smry_vrsn_nbr,0) = NVL(max_train_cnst_smry_vrsn_nbr,0)
