select 

CDRE.TRAIN_CNST_SMRY_ID, CDRE.TRAIN_CNST_SMRY_VRSN_NBR

,CDRE.SQNC_NBR

,cdre.mark_cd, cdre.eqpun_nbr

,cdre.cnst_chng_fsac_cd, cdre.cnst_chng_event_tms

--cdre.*

from D_BRONZE.SADB.TRAIN_CNST_DTL_RAIL_EQPT CDRE,

where 1=1

and cdre.train_cnst_smry_id = 16325104435

and cdre.train_cnst_smry_vrsn_nbr IN (6)

and cdre.mark_cd = 'TTGX'

and cdre.eqpun_nbr = '0000705466'

;
 
 
select CDRE.TRAIN_CNST_SMRY_ID, CDRE.TRAIN_CNST_SMRY_VRSN_NBR, CDRE.SQNC_NBR, COUNT(1) numrecs

from D_BRONZE.SADB.TRAIN_CNST_DTL_RAIL_EQPT CDRE

group by CDRE.TRAIN_CNST_SMRY_ID, CDRE.TRAIN_CNST_SMRY_VRSN_NBR, CDRE.SQNC_NBR

having COUNT(1) > 1

order by numrecs desc, CDRE.TRAIN_CNST_SMRY_ID, CDRE.TRAIN_CNST_SMRY_VRSN_NBR, CDRE.SQNC_NBR

;

 
D_BRONZE.SADB.TRAIN_CNST_DTL_RAIL_EQPT CDRE table is having duplicate value
 
