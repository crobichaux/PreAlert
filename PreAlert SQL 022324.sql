
--Sent to BChan 022624 for conversion into RWB SQL Report

with #enc as (select peh.pat_id, peh.PAT_ENC_CSN_ID, peh.HOSP_ADMSN_TIME
    from clarity.dbo.pat_enc_hsp peh
    where 1=1
        AND format(peh.hosp_admsn_time, 'yyyyMMdd') = FORMAT(DateAdd(dd, -1, getdate()), 'yyyyMMdd')
        AND peh.PAT_ENC_CSN_ID is not null
        AND peh.ADT_PATIENT_STAT_C = 2
        AND peh.ADT_PATIENT_STAT_C <> (6)),
--Pt Demographic Information
    #pat as (select pat.pat_id,
                    pat.PAT_FIRST_NAME,
                    pat.PAT_LAST_NAME,
                    floor((DATEDIFF(day, pat.birth_date, CURRENT_TIMESTAMP) / 365.25)) as current_age,
                    sex.NAME as gender
    from clarity.dbo.PATIENT pat
    left join clarity.dbo.ZC_SEX sex on sex.RCPT_MEM_SEX_C = pat.SEX_C
    inner join #enc enc on pat.pat_id = enc.pat_id),
--Encounter Information (from pat_enc_hsp)
    #encinfo as (select enc.pat_id,     --mrn, hospital, financial class, primary insurance, admit source, length of stay (from clarity.dbo.length_of_stay)
                        peh.PAT_ENC_CSN_ID,
                        pat.PAT_MRN_ID as mrn,
                        zps.name as hosp_location,
                        dep.external_name as facility,
                        dep.DEPARTMENT_NAME as unit,
                        --facility hospital area id clarity_loc
                        zas.name as admit_source,
                        dis.name as discharge_disposition,
                        convert(varchar, peh.HOSP_ADMSN_TIME, 112) as admit_day, ---112 converts to yyyyMMdd
                        peh.HOSP_ADMSN_TIME as admit_timestamp,
                        --convert(varchar, peh.HOSP_DISCH_TIME, 112) as discharge_day,
                        --peh.HOSP_DISCH_TIME as discharge_timestamp,
                        los.LENGTH_OF_STAY_DAYS as los,
                        har.ACCT_FIN_CLASS_C as financial_class_id, --financial class ID
                        fc.name as financial_class_desc             --financial class desc
                        --primary insurance plan
                        --primary group name
                        --secondary insurance plan
                        --secondary group name
    from #enc enc
    left join clarity.dbo.pat_enc_hsp peh on enc.PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
    left join clarity.dbo.PATIENT pat on pat.PAT_ID = enc.PAT_ID
    left join clarity.dbo.ZC_PAT_SERVICE zps on zps.HOSP_SERV_C = peh.HOSP_SERV_C
    left join clarity.dbo.clarity_dep dep on dep.department_id = peh.department_id
    left join clarity.dbo.ZC_ADM_SOURCE zas on zas.ADMIT_SOURCE_C = peh.ADMIT_SOURCE_C
    left join clarity.dbo.LENGTH_OF_STAY los on los.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
    left join clarity.dbo.ZC_DISCH_DISP dis on dis.DISCH_DISP_C = peh.DISCH_DISP_C
    left join Clarity.dbo.HSP_ACCOUNT har on peh.HSP_ACCOUNT_ID = har.HSP_ACCOUNT_ID
    left join Clarity.dbo.ZC_FINANCIAL_CLASS fc on har.ACCT_FIN_CLASS_C = fc.FINANCIAL_CLASS),
--ICU location this admission (first 24h)
    #iculoc as (select enc.PAT_ENC_CSN_ID,
                       loc.IN_DTTM as icu_admit_time, --IN_DTTM is when they physically enter ICU
                       loc.OUT_DTTM as icu_discharge,  --OUT_DTTM is when they physically exit ICU
                       ceiling(datediff(day, loc.OUT_DTTM, loc.IN_DTTM)) as icu_los_days
    from #enc enc
    left join clarity.dbo.F_PAT_ICU_LOCATION loc on enc.PAT_ENC_CSN_ID = loc.PAT_ENC_CSN_ID
    where 1=1
        and loc.IN_DTTM BETWEEN enc.HOSP_ADMSN_TIME and DateAdd(HH, 24, enc.HOSP_ADMSN_TIME)),
    --Total # of Hospitalizations in last 365 days
    --Join on pat_id and pull all encounters filtering by class (inpatient) and admit date (now-365), then count # of rows
    #hosp as (select enc.pat_id, hsp.PAT_ENC_CSN_ID
    from  #enc enc
    left join clarity.dbo.pat_enc_hsp hsp on hsp.PAT_ID = enc.PAT_ID
    left Join Clarity.dbo.HSD_BASE_CLASS_MAP m ON hsp.ADT_PAT_CLASS_C = m.ACCT_CLASS_MAP_C
    where 1=1
        and m.BASE_CLASS_MAP_C IN (1) --inpatient
        and hsp.ADT_PATIENT_STAT_C <> (6)  --removing hospital outpatient visits
        --and hsp.adt_pat_class_c = 101  --inpatient via ZC_PAT_CLASS
        and format(hsp.hosp_admsn_time, 'yyyyMMdd') >= FORMAT(DateAdd(dd, -367, getdate()), 'yyyyMMdd')
        and format(hsp.hosp_admsn_time, 'yyyyMMdd') <= FORMAT(DateAdd(dd, -2, getdate()), 'yyyyMMdd')),
    #hosp2 as (select hosp.pat_id, count(hosp.PAT_ENC_CSN_ID) as prev_hosp
        from #hosp hosp
        group by hosp.pat_id),
--Prev ICU stay in last 365 days
    --Hybrid of the laast 2 pieces - ICU length of stay during the last 365 day hospitalizations
    #icu as (select hosp.PAT_ID, abs(ceiling(datediff(day, loc.OUT_DTTM, loc.IN_DTTM))) as icu_los_days
    from #enc enc
    left join #hosp hosp on hosp.PAT_ID = enc.PAT_ID
    left join clarity.dbo.F_PAT_ICU_LOCATION loc on hosp.PAT_ENC_CSN_ID = loc.PAT_ENC_CSN_ID
    where 1=1
    and format(loc.IN_DTTM,'yyyyMMdd')  >= FORMAT(DateAdd(dd, -367, enc.HOSP_ADMSN_TIME), 'yyyyMMdd')),
    #icu2 as (select pat_id, sum(icu_los_days) as icu_los_days
              from #icu
              group by pat_id),
--Total/Mean LOS in last 365 days
    --Similar but not filtered to ICU - can prob use the hosp_enc table and length_of_stay table
    #hospa as (select enc.pat_id, hsp.PAT_ENC_CSN_ID
    from  #enc enc
    left join clarity.dbo.pat_enc_hsp hsp on hsp.PAT_ID = enc.PAT_ID
    left Join Clarity.dbo.HSD_BASE_CLASS_MAP m ON hsp.ADT_PAT_CLASS_C = m.ACCT_CLASS_MAP_C
    where 1=1
        and m.BASE_CLASS_MAP_C IN (1) --inpatient
        and hsp.ADT_PATIENT_STAT_C <> (6)  --removing hospital outpatient visits
        --and hsp.adt_pat_class_c = 101  --inpatient via ZC_PAT_CLASS
        and format(hsp.hosp_admsn_time, 'yyyyMMdd') >= FORMAT(DateAdd(dd, -367, getdate()), 'yyyyMMdd')
        and format(hsp.hosp_admsn_time, 'yyyyMMdd') <= FORMAT(DateAdd(dd, -2, getdate()), 'yyyyMMdd')),
    #hosplos as (select hosp.pat_id,
                        ISNULL(sum(los.length_of_stay_days),0) as hosp_total_los,
                        ISNULL(sum(los.length_of_stay_days)/count(distinct hosp.PAT_ENC_CSN_ID),0) as hosp_avg_los
    from #hospa hosp
    left join clarity.dbo.length_of_stay los on hosp.PAT_ENC_CSN_ID = los.PAT_ENC_CSN_ID
    group by hosp.pat_id),
--Immunocompromised ICD (grouper?) in last 365 days
    --ICD codes are in OneDrive.  Look for grouper that is generic? They weren't specific on this
    #imm as (select enc.PAT_ID, 'YES' as imm_comp_prev
    from #enc enc
    left join Clarity.dbo.PROBLEM_LIST lpl ON lpl.PAT_ID = enc.PAT_ID
    left join Clarity.dbo.CLARITY_EDG edg ON edg.DX_ID = lpl.DX_ID
    where 1=1
        and lpl.NOTED_DATE <= DateAdd(dd, -2, enc.HOSP_ADMSN_TIME)
        and (lpl.NOTED_END_DATE >= DATEADD(dd, -367, enc.HOSP_ADMSN_TIME) OR NOTED_END_DATE IS NULL)
        and (edg.CURRENT_ICD10_LIST in ('D89', 'D70', 'D71', 'D72.0', 'D72.81', 'D72.89', 'D72.9', 'D75.81',
                    'D47.4', 'D75.89', 'R76', 'R89.4', 'Z85', 'C7A', 'C7B', 'D3A', 'T86', 'Z94',
                    'Z98.85', 'D86', 'E85', 'E85.0', 'M04', 'E85.1', 'E85.3', 'E85.8', 'G35', 'G36',
                    'G37.1', 'G37.3', 'G37.8', 'G37.9', 'G61.0', 'G61.9', 'I40', 'M30', 'T78.40', 'J67.9',
                    'J84.01', 'J84.02', 'J84.09', 'L93.0', 'L93.2', 'M32', 'L94', 'M35.8', 'M35.9', 'M12.9',
                    'M01.X0', 'M02.10', 'M11', 'M46', 'M31.5', 'M35.3')  or
                    edg.CURRENT_ICD10_LIST between 'B20' and 'B24' or
                    edg.CURRENT_ICD10_LIST between 'C88' and 'C96' or
                    edg.CURRENT_ICD10_LIST between 'R83.4' and 'R87.4' or
                    edg.CURRENT_ICD10_LIST between 'C00' and 'C07' or
                    edg.CURRENT_ICD10_LIST between 'C11' and 'C19' or
                    edg.CURRENT_ICD10_LIST between 'C22' and 'C80' or
                    edg.CURRENT_ICD10_LIST between 'D00' and 'D49' or
                    edg.CURRENT_ICD10_LIST between 'M05' and 'M14')),
--Infection ICD Code lst 365 days
    --select * from clarity.dbo.v_cube_d_diagnosis where diagnosis_id in (select distinct cmpl_dx_recs_id as diagnosis_id from clarity.dbo.GROUPER_DX_RECORDS where GROUPER_ID = '1138015')
    #inf as (select enc.PAT_ID, 1 as inf_prev
        from #enc enc
        left join Clarity.dbo.PROBLEM_LIST lpl ON lpl.PAT_ID = enc.PAT_ID
        left join Clarity.dbo.CLARITY_EDG edg ON edg.DX_ID = lpl.DX_ID
        left join Clarity.dbo.GROUPER_DX_RECORDs gdr on edg.dx_id = gdr.cmpl_dx_recs_id
        where lpl.NOTED_DATE <= DateAdd(dd, -2, enc.HOSP_ADMSN_TIME) and (lpl.NOTED_END_DATE >= DATEADD(dd, -367, enc.HOSP_ADMSN_TIME) OR NOTED_END_DATE IS NULL)
            and gdr.GROUPER_ID = '1138015'),
--Prior Vent in last 365 days F_VENT_EPISODES
    #vent as (select enc.PAT_ID, vent.VENT_START_FSD_ID as vent_id
    from clarity.dbo.f_vent_episodes vent
    inner join #enc enc on vent.PAT_ID = enc.PAT_ID
    left join clarity.dbo.PAT_ENC_HSP peh on peh.PAT_ID = vent.PAT_ID
    where  1=1
    --and FORMAT(vent.VENT_START_DTTM, 'MM/dd/yy') between DateAdd(dd, -2, FORMAT(peh.hosp_admsn_time, 'MM/dd/yy')) and DateAdd(dd, -367, FORMAT(peh.hosp_admsn_time, 'MM/dd/yy'))
        and format(peh.hosp_admsn_time, 'yyyyMMdd') >= FORMAT(DateAdd(dd, -367, getdate()), 'yyyyMMdd')
      --  and format(hsp.HOSP_ADMSN_TIME, 'yyyyMMdd') is not null
        and format(peh.hosp_admsn_time, 'yyyyMMdd') <= FORMAT(DateAdd(dd, -2, getdate()), 'yyyyMMdd')
    and vent.INPATIENT_DATA_ID = peh.INPATIENT_DATA_ID),
--Abx DoT in last 365 days (use InPart Code)
    #FirstWords AS (select distinct thr.IMPORTED_SHORT_NAME as FirstWord
    from clarity.dbo.V_CUBE_D_MEDICATION vcd
        left join clarity.dbo.RX_MED_THREE thr on thr.MEDICATION_ID = vcd.MEDICATION_ID
    where  THERAPEUTIC_CLASS = 'ANTIBIOTICS'),
    #medsa as (SELECT distinct medication_id, medication_name
        FROM clarity.dbo.V_CUBE_D_MEDICATION
        WHERE EXISTS (SELECT 1 FROM #FirstWords
            WHERE #FirstWords.FirstWord = upper(SUBSTRING(LTRIM(clarity.dbo.V_CUBE_D_MEDICATION.medication_name),1,
                (CHARINDEX(' ',LTRIM(clarity.dbo.V_CUBE_D_MEDICATION.medication_name) + ' ')-1))))),
    #FirstWords2 as (SELECT DISTINCT
        UPPER(SUBSTRING(Medication_name, 1, CHARINDEX(' ', Medication_name + ' ') - 1)) AS FirstWord
    FROM clarity.dbo.V_CUBE_D_MEDICATION
    WHERE THERAPEUTIC_CLASS = 'ANTIBIOTICS'),
    #medsb as (SELECT medication_id, MEDICATION_NAME
    FROM clarity.dbo.V_CUBE_D_MEDICATION
        WHERE EXISTS (SELECT 1
            FROM #FirstWords2
            WHERE #FirstWords2.FirstWord = UPPER(SUBSTRING(clarity.dbo.V_CUBE_D_MEDICATION.Medication_name, 1, CHARINDEX(' ', clarity.dbo.V_CUBE_D_MEDICATION.Medication_name + ' ') - 1)))),
    #meds as (select * from #medsa union select * FROM #medsb),
    #meds2 as (select medication_id, MEDICATION_NAME from #meds where
            MEDICATION_NAME like '%aztreonam%' OR MEDICATION_NAME like '%amoxicil%' OR MEDICATION_NAME like '%ampicillin%'  OR MEDICATION_NAME like '%carbenicillin%'
            OR MEDICATION_NAME like '%cefaclor%'  OR MEDICATION_NAME like '%cefadroxil%' OR MEDICATION_NAME like '%cefamandole%' OR MEDICATION_NAME like '%cefazolin%'
            OR MEDICATION_NAME like '%cefdinir%' OR MEDICATION_NAME like '%cefditoren%' OR MEDICATION_NAME like '%cefepime%' OR MEDICATION_NAME like '%cefiderocol%'
            OR MEDICATION_NAME like '%cefixime%' OR MEDICATION_NAME like '%cefoperazone%' OR MEDICATION_NAME like '%cefotaxime%' OR MEDICATION_NAME like '%cefotetan%'
            OR MEDICATION_NAME like '%cefoxitin%' OR MEDICATION_NAME like '%cefpodoxime%' OR MEDICATION_NAME like '%cefprozil%' OR MEDICATION_NAME like '%ceftaroline%'
            OR MEDICATION_NAME like '%ceftazidime%' OR MEDICATION_NAME like '%ceftibuten%' OR MEDICATION_NAME like '%eftizoxime%' OR MEDICATION_NAME like '%ceftolozane%'
            OR MEDICATION_NAME like '%ceftriaxone%' OR MEDICATION_NAME like '%cefuroxime%' OR MEDICATION_NAME like '%cephalexin%' OR MEDICATION_NAME like '%cloxacillin%'
            OR MEDICATION_NAME like '%dicloxacillin%' OR MEDICATION_NAME like '%doripenem%' OR MEDICATION_NAME like '%ertapenem%' OR MEDICATION_NAME like '%imipenem%'
            OR MEDICATION_NAME like '%loracarbef%' OR MEDICATION_NAME like '%meropenem%' OR MEDICATION_NAME like '%nafcillin%' OR MEDICATION_NAME like '%oxacillin%'
            OR MEDICATION_NAME like '%pen G%' OR MEDICATION_NAME like '%penicillin G%' OR MEDICATION_NAME like '%penicillin V%' OR MEDICATION_NAME like '%piperacillin%'
            OR MEDICATION_NAME like '%ticarcilli%' OR MEDICATION_NAME like '%cefolozane%'),
    #abx as (Select distinct peh.pat_id
                  , peh.PAT_ENC_CSN_ID
                  , upper(SUBSTRING(LTRIM(vcd.medication_name),1,
                        (CHARINDEX(' ',LTRIM(vcd.medication_name) + ' ')-1))) as first_name
                  , FORMAT(mar.TAKEN_TIME, 'MM/dd/yy') AS med_order_day
             FROM Clarity.dbo.PAT_ENC_HSP peh
                      Inner Join Clarity.dbo.ORDER_MED om ON peh.PAT_ENC_CSN_ID = om.PAT_ENC_CSN_ID
                      Inner Join #enc enc on enc.pat_id = om.PAT_ID
                      Inner Join Clarity.dbo.PATIENT pat ON enc.PAT_ID = pat.PAT_ID
                      Left Outer Join Clarity.dbo.CLARITY_MEDICATION cm ON om.MEDICATION_ID = cm.MEDICATION_ID
                      left outer join Clarity.dbo.RX_MED_THREE m3 on om.MEDICATION_ID = m3.MEDICATION_ID
                      left outer join Clarity.dbo.ZC_SIMPLE_GENERIC z on m3.SIMPLE_GEN_NAM_C = z.SIMPLE_GENERIC_C
                      Left Outer Join Clarity.dbo.ZC_MED_UNIT zu ON om.DOSE_UNIT_C = zu.DISP_QTYUNIT_C
                      Left Outer Join Clarity.dbo.IP_FREQUENCY f ON om.HV_DISCR_FREQ_ID = f.FREQ_ID
                      Left Outer Join Clarity.dbo.ZC_DISPENSE_ROUTE r ON om.MED_ROUTE_C = r.DISPENSE_ROUTE_C
                      Left Outer Join Clarity.dbo.ZC_ORDER_STATUS zos ON om.ORDER_STATUS_C = zos.ORDER_STATUS_C
                      Left Outer Join Clarity.dbo.MAR_ADMIN_INFO mar ON om.ORDER_MED_ID = mar.ORDER_MED_ID
                      Left Outer Join Clarity.dbo.ZC_MAR_RSLT zma ON mar.MAR_ACTION_C = zma.RESULT_C
                      Left Outer Join Clarity.dbo.ZC_MED_UNIT zad ON mar.DOSE_UNIT_C = zad.DISP_QTYUNIT_C
                      Left Outer Join Clarity.dbo.ZC_ADMIN_ROUTE zar ON mar.ROUTE_C = zar.MED_ROUTE_C
                      Left Outer Join Clarity.dbo.V_CUBE_D_MEDICATION vcd on vcd.MEDICATION_ID = cm.MEDICATION_ID
             WHERE 1 = 1
               AND om.ORDERING_MODE_C = 2          --inpatient
               AND om.ORDER_CLASS_C NOT IN (3, 45) --historical
               AND om.PROVIDER_TYPE_C = 1          --authorizing
               AND (om.MED_ROUTE_C in
                    (4, 5, 6, 7, 11, 12, 15, 17, 28, 35, 68, 75, 82, 85, 86, 89, 102, 117, 118, 119, 122, 123, 127, 128,
                     164, 171, 179, 182, 202, 209, 210, 230, 253, 254)
                    OR (zar.title = 'INTRAVENOUS') OR (zar.title = 'CONTIN. INTRAVENOUS INFUSION'))
               AND (cm.MEDICATION_ID in (select medication_id from #meds) or CM.THERA_CLASS_C IN ('12'))
               AND (FORMAT(om.ORDER_INST, 'MM/dd/yy') >= DateAdd(dd, -367, sysdatetime())
                        AND FORMAT(om.ORDER_INST, 'MM/dd/yy') <= DateAdd(dd, -2, sysdatetime()))
               AND mar.TAKEN_TIME is not null
               AND mar.MAR_ACTION_C not in (2,3,4,5,8,9,10,16,110,101,104,110,112,121,127,128,129,130,131,133,134,135,136,138,140,141,143,144,145,146,147,148,15,151,152,153,154,98,99)
               AND (vcd.medication_name not like 'APNO%' AND vcd.medication_name not like 'DEXAMETHASONE%' AND vcd.medication_name not like 'HIPREX%' AND vcd.medication_name not like 'METH/ME%' AND vcd.medication_name not like 'METH/MEBLUE/SOD%'
                    AND vcd.medication_name not like 'METHADEX%' AND vcd.medication_name not like 'METHEN-HYOS-ME%' AND vcd.medication_name not like 'METHEN-HYOSC-M.BLUE-SAL-NAPHOS%' AND vcd.medication_name not like 'METHEN-HYOSC-M.BLUE-SAL-SODIUM%'
                    AND vcd.medication_name not like 'METHEN-M.BLUE-S.PHOS-PHSAL-HYO%' AND vcd.medication_name not like 'METHEN-SOD%' AND vcd.medication_name not like 'METHEN/MBLUE/SAL/SOD%' AND vcd.medication_name not like 'METHENAM%'
                    AND vcd.medication_name not like 'METHENAM/BENZ%' AND vcd.medication_name not like 'METHENAM/M.BLUE/SALICYL/HYOSCY%' AND vcd.medication_name not like 'METHENAM/MBLU/BA/SAL/ATROP/HYO%' AND vcd.medication_name not like 'METHENAM/SOD%'
                    AND vcd.medication_name not like 'METHENAMIN%' AND vcd.medication_name not like 'METHENAMINE%' AND vcd.medication_name not like 'METHENAMINE-M.BLUE-SOD%' AND vcd.medication_name not like 'METHENAMINE-SOD%'
                    AND vcd.medication_name not like 'METHENAMINE-SODIUM%' AND vcd.medication_name not like 'METHENAMINE/SODIUM%' AND vcd.medication_name not like 'METHIONINE%' AND vcd.medication_name not like 'POLY%' AND vcd.medication_name not like 'SSD%'
                    AND vcd.medication_name not like 'SSS%' AND vcd.medication_name not like 'URO%' AND vcd.medication_name not like 'URO-458%' AND vcd.medication_name not like 'URO-MP%' AND vcd.medication_name not like 'URO-SP%' AND vcd.medication_name not like 'UROAV-B%'
                    AND vcd.medication_name not like 'UROCAR%')),
    #dot as (select distinct abx.pat_id, abx.med_order_day, abx.first_name, 1 as dot
        from #abx abx
        inner join #enc enc on abx.PAT_ID = enc.PAT_ID),
    #dot2 as (select dot.pat_id, count(distinct concat(dot.med_order_day, dot.first_name)) as prev_dot
               from #dot dot
               group by dot.pat_id),
    #dotcurrent as (select abx.pat_enc_csn_id, abx.med_order_day
               from #abx abx
               inner join #enc enc on abx.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID),
--Previous Beta-Lactam (last 365 days)
    #blactam as (Select peh.pat_id
        , peh.PAT_ENC_CSN_ID
		, peh.HSP_ACCOUNT_ID
		, om.ORDER_MED_ID					AS order_med_id
		, om.MEDICATION_ID					AS medication_id
        ,upper(SUBSTRING(LTRIM(cdm.medication_name),1,
            (CHARINDEX(' ',LTRIM(cdm.medication_name) + ' ')-1))) as first_name
		, m3.MEDICATION_NAME				AS med_name
        , FORMAT(mar.TAKEN_TIME , 'MM/dd/yy') AS med_order_day
        FROM #enc enc
        Inner Join Clarity.dbo.PAT_ENC_HSP peh on enc.pat_id = peh.PAT_ID
		Inner Join Clarity.dbo.ORDER_MED om ON peh.PAT_ENC_CSN_ID = om.PAT_ENC_CSN_ID
		Inner Join Clarity.dbo.PATIENT pat ON enc.PAT_ID = pat.PAT_ID
		Left Outer Join Clarity.dbo.CLARITY_MEDICATION cm ON om.MEDICATION_ID = cm.MEDICATION_ID
		left outer join Clarity.dbo.RX_MED_THREE m3 on om.MEDICATION_ID = m3.MEDICATION_ID
		left outer join Clarity.dbo.ZC_SIMPLE_GENERIC z on m3.SIMPLE_GEN_NAM_C = z.SIMPLE_GENERIC_C
		Left Outer Join Clarity.dbo.ZC_MED_UNIT zu ON om.DOSE_UNIT_C = zu.DISP_QTYUNIT_C
		Left Outer Join Clarity.dbo.IP_FREQUENCY f ON om.HV_DISCR_FREQ_ID = f.FREQ_ID
		Left Outer Join Clarity.dbo.ZC_DISPENSE_ROUTE r ON om.MED_ROUTE_C = r.DISPENSE_ROUTE_C
		Left Outer Join Clarity.dbo.ZC_ORDER_STATUS zos ON om.ORDER_STATUS_C = zos.ORDER_STATUS_C
		Left Outer Join Clarity.dbo.MAR_ADMIN_INFO mar ON om.ORDER_MED_ID = mar.ORDER_MED_ID
		Left Outer Join Clarity.dbo.ZC_MAR_RSLT zma ON mar.MAR_ACTION_C = zma.RESULT_C
		Left Outer Join Clarity.dbo.ZC_MED_UNIT zad ON mar.DOSE_UNIT_C = zad.DISP_QTYUNIT_C
		Left Outer Join Clarity.dbo.ZC_ADMIN_ROUTE zar ON mar.ROUTE_C = zar.MED_ROUTE_C
        left outer join clarity.dbo.v_cube_d_medication  cdm on cdm.MEDICATION_ID = cm.MEDICATION_ID
        WHERE	1=1
		AND om.ORDERING_MODE_C = 2						--inpatient
		AND om.ORDER_CLASS_C NOT IN (3,45)				--historical
		AND om.PROVIDER_TYPE_C = 1						--authorizing
        AND (om.MED_ROUTE_C in (4, 5, 6, 7, 11, 12, 15, 17, 28, 35, 68, 75, 82, 85, 86, 89, 102, 117, 118, 119, 122, 123, 127, 128,
                     164, 171, 179, 182, 202, 209, 210, 230, 253, 254)
				OR (zar.title = 'INTRAVENOUS') OR (zar.title = 'CONTIN. INTRAVENOUS INFUSION')
                OR (zar.title is null AND om.MED_ROUTE_C is null))
        AND mar.TAKEN_TIME is not null
 --       AND CM.THERA_CLASS_C IN ('12')
        AND (cdm.PHARMACEUTICAL_CLASS in ('BETALACTAMS', 'CEPHALOSPORIN ANTIBIOTICS - 1ST GENERATION', 'CEPHALOSPORIN ANTIBIOTICS - 2ND GENERATION', 'CEPHALOSPORIN ANTIBIOTICS - 3RD GENERATION',
                'CEPHALOSPORIN ANTIBIOTICS - 4TH GENERATION', 'CEPHALOSPORIN ANTIBIOTICS - SIDEROPHORE', 'CEPHALOSPORINS - 5TH GENERATION', 'CARBAPENEM ANTIBIOTICS (THIENAMYCINS)')
            OR m3.medication_name like '%aztreonam%' OR m3.medication_name like '%amoxicil%' OR m3.medication_name like '%ampicillin%'  OR m3.medication_name like '%carbenicillin%'
            OR m3.medication_name like '%cefaclor%'  OR m3.medication_name like '%cefadroxil%' OR m3.medication_name like '%cefamandole%' OR m3.medication_name like '%cefazolin%'
            OR m3.medication_name like '%cefdinir%' OR m3.medication_name like '%cefditoren%' OR m3.medication_name like '%cefepime%' OR m3.medication_name like '%cefiderocol%'
            OR m3.medication_name like '%cefixime%' OR m3.medication_name like '%cefoperazone%' OR m3.medication_name like '%cefotaxime%' OR m3.medication_name like '%cefotetan%'
            OR m3.medication_name like '%cefoxitin%' OR m3.medication_name like '%cefpodoxime%' OR m3.medication_name like '%cefprozil%' OR m3.medication_name like '%ceftaroline%'
            OR m3.medication_name like '%ceftazidime%' OR m3.medication_name like '%ceftibuten%' OR m3.medication_name like '%eftizoxime%' OR m3.medication_name like '%ceftolozane%'
            OR m3.medication_name like '%ceftriaxone%' OR m3.medication_name like '%cefuroxime%' OR m3.medication_name like '%cephalexin%' OR m3.medication_name like '%cloxacillin%'
            OR m3.medication_name like '%dicloxacillin%' OR m3.medication_name like '%doripenem%' OR m3.medication_name like '%ertapenem%' OR m3.medication_name like '%imipenem%'
            OR m3.medication_name like '%loracarbef%' OR m3.medication_name like '%meropenem%' OR m3.medication_name like '%nafcillin%' OR m3.medication_name like '%oxacillin%'
            OR m3.medication_name like '%pen G%' OR m3.medication_name like '%penicillin G%' OR m3.medication_name like '%penicillin V%' OR m3.medication_name like '%piperacillin%'
            OR m3.medication_name like '%ticarcilli%' OR m3.medication_name like '%cefolozane%' or om.medication_id in (select medication_id from #meds2))
        AND FORMAT(om.ORDER_INST , 'MM/dd/yy') >= DateAdd(dd, -367, peh.HOSP_ADMSN_TIME)
        AND FORMAT(om.ORDER_INST, 'MM/dd/yy') <= DateAdd(dd, -2, sysdatetime())),
    #bdot as (select abx.pat_id, abx.med_order_day, abx.first_name, 1 as dot
        from #blactam abx
        inner join #enc enc on abx.PAT_ID = enc.PAT_ID),
    #bdot2 as (select dot.pat_id, count(distinct concat(dot.med_order_day, dot.first_name)) as prev_dot
               from #bdot dot
               group by dot.pat_id),
--GI Surg Proc last 365 days
    #gi as (select enc.pat_id,
             case when eap.proc_code in ('GI47','GI52','GI53','GI59','GI4','GI16','GI9','GI25','GI32','GI40','GI41','GI6','GI10',
             'GI14','GI18','GI5','GI50','3200118','3200119','3610164','7500066','7500067',
             '7500073','7500074','7500076','2700026','2700061','3700001','7500012','7500022',
             '7500023','7500033','7500039','7500050','7500055','3600034','7504323501','7504537901',
             '3614328401','3604799902','7504470501','7504535001','3068750701','75043235PB','75044388PB',
             '75044389PB','75044391PB','75044394PB','75045300PB','75045303PB','75045305PB','75045330PB',
             '75045331PB','75045378PB','75045380PB','75045390PB','3614755301','3614755401','3614755501',
             '3614755601','7504323502','7504323601','7504323701','7504323702','7504326001','7504326101',
             '7504326201','7504326301','7504326401','7504326501','7504327401','7504327501','7504327601',
             '7504327701','7504327801','7504436001','7504436002','7504436101','7504436102','7504436302',
             '7504436401','7504436402','7504436501','7504436502','7504436601','7504436602','7504436901',
             '7504436902','7504437002','7504437202','7504437302','7504437602','7504437702','7504437802',
             '7504437902','7504438001','7504438201','7504438501','7504438601','7504438801','7504438901','7504439001',
             '7504439101','7504439201','7504439401','7504440101','7504440201','7504440401','7504440501','7504440601',
             '7504440602','7504440701','7504440702','7504530001','7504530301','7504530501','7504530701','7504530801',
             '7504530901','7504531501','7504531701','7504532001','7504532101','7504532701','7504533001','7504533101',
             '7504533201','7504533301','7504533401','7504533501','7504533801','7504534001','7504534101','7504534102',
             '7504534201','7504534202','7504534601','7504534701','7504537801','7504537902','7504538001','7504538101',
             '7504538201','7504538401','7504538501','7504538601','7504538901','7504539001','7504539102','7504539202',
             '7504539301','7504539801','7504660001','7504660401','7504660601','7504660801','7504661001','7504661101',
             '7504661201','7504661401','7504661501','7509101001','7509101002','7509102201','7509111001','7509112201',
             '4904325301','3204450001','4904436401','7504297501','490G012001','750G012001','4905586701','4900780T01',
             '4904497001','4904479901','7614660401','3614420501','3614660401','76157425PB','762G037803','4904418001',
             '4904756201','4904756301','4904756401','4904932401','4904965101','4904965201','4904965401','4905866101',
             'IMG757','IMG758','IMG765','IMG2500','IMG4000','IMG2559','IMGCNV1503','IMGCNV1907','IMGCNV0984','IMGCNV0985',
             'IMGCNV0986','IMGCNV0987','IMGCNV0988','IMGCNV3831','IMGCNV3832','IMGCNV3833','IMGCNV3834','IMG2571','IMG2572',
             'IMG2573','IMG2574','IMG2575','IMG62501','IMG1986','IMG2331','IMG2331CHG','IMG2673CHG','IMG2674CHG','IMG2675CHG',
             'IMG2676CHG','IMG2677CHG','IMG654','IR627','IMGCNV3598','IMGCNV2298','IMGCNV2299','IMGCNV2574','IMGCNV0146','IMGCNV0147',
             'IMGCNV0200','IMGCNV0201','IMGCNV0202','IMGCNV1475','IMGCNV2692','IMGCNV2693','IMGCNV2694','IMGCNV2695','IMGCNV2808',
             'IMGCNV3268','IMGCNV3269','IMGCNV3270','IMGCNV3271','PRO147','PRO50','90912','90913','43234','43235','43236','43237',
             '43256','43258','43260','43261','43262','43263','43264','43265','43267','43268','43269','43271','43272','43289',
             '43659','44140','44141','44143','44144','44145','44146','44147','44155','44156','44160','44203','44204','44205',
             '44206','44207','44210','44212','44238','44239','44360','44361','44363','44364','44365','44366','44369','44370',
             '44372','44373','44378','44379','44388','44389','44390','44391','44392','44393','44394','44397','44950','44970',
             '44979','45330','45331','45332','45333','45334','45339','45340','45341','45342','45345','45355','45378','45379',
             '45380','45383','45387','47554','47562','47563','47570','47579','47600','47605','47610','47612','47721','49321',
             '49329','49650','A43234','A43235','A43236','A43237','A43256','A43258','A43260','A43261','A43262','A43263','A43264',
             'A43265','A43267','A43268','A43269','A43271','A43272','A43274','A43275','A43276','A43277','A43278','A44204','A44205',
             'A44206','A44207','A44208','A44210','A44211','A44213','A44238','A44360','A44361','A44363','A44364','A44365','A44366',
             'A44369','A44385','A44386','A44388','A44389','A44390','A44391','A44392','A44393','A44394','A44397','A44950','A44955',
             'A44960','A44970','A45121','A45300','A45303','A45305','A45307','A45308','A45309','A45315','A45320','A45321','A45327',
             'A45330','A45331','A45332','A45333','A45334','A45335','A45337','A45338','A45339','A45340','A45341','A45342','A45345',
             'A45355','A45378','A45379','A45380','A45382','A45383','A45384','A45385','A45386','A45387','A45391','A45392','A47550',
             'A47553','A47554','A47555','A47556','A47562','A47563','A47564','A47721','A47741','43274','43275','43276','43277','43278',
             '49654','44180','44186','44187','44188','44213','45400','45402','45499','44157','49326','44401','44402','44403','44404',
             '44405','44406','44407','44408','45346','45347','45350','45388','45389','45390','45393','45398','Z1935','Z3037','Z1623',
             'Z1325','Z1880','Z2361','91122','91299','0651T','3130F','2145Z','2153Z','0397T','C9779','C7541','C7542','C7543','C7544',
             'G0106','G0120','G0122','G6019','G6020','G6022','G6023','G6024','G6025','G0342','G4006','SUR202','SUR8','SUR806','SUR807',
             'SUR1052','SUR765','SUR761','SUR299','SUR446','SUR466','SUR753','SUR755','SUR756','SUR757','SUR1295','SUR38','SHX174','SHX188',
             'SHX1034','SHX963','SHX54','SHX55','SHX59','SHX60','SHX88','SHX149','SHX162','SHX298','SHX537','SHX777','SHX852','SHX853','SHX854',
             'SHX855','SHX856','SHX1738','SHX1775','SHX1918','SHX1939','SHX1952','SHX2159','SHX2164','SHX2309','SHX2335','SHX1521','SHX1522',
             'SHX1523','SHX1524','SHX1526','SHX1529','SHX1530','SHX1531','SHX1532','SHX1580','SHX1371','SHX1374','SHX1375','SHX1376','SHX1377',
             'SHX1378','SHX1379','SHX1380','SUR275','SUR276','SUR758','SUR762','SHX2408','SHX2415','SHX2602','SHX2603','SHX2604','SHX2605','SHX2606',
             'SHX2667','SHX1648','SHX1649','SUR768','SUR770','SUR790','SHX1020','SHX400','SHX462','O129790','O129798')
             then 'YES' else 'NO' end as prev_GI
             from #enc enc
             left join Clarity.dbo.ORDER_PROC ord ON enc.PAT_ENC_CSN_ID = ord.PAT_ENC_CSN_ID
             left join Clarity.dbo.CLARITY_EAP eap ON ord.PROC_ID = eap.PROC_ID
             left join Clarity.dbo.CLARITY_EAP_OT ot on ord.PROC_ID = ot.PROC_ID
             left join clarity.dbo.CLARITY_EAP_OT cpt on eap.proc_id = cpt.proc_id
             where ot.CONTACT_DATE between enc.HOSP_ADMSN_TIME and DateAdd(dd, -367, enc.HOSP_ADMSN_TIME)), --CONTACT_DATE = contact date for procedure
--Elixhauser
    #dx as (select distinct enc.PAT_ID
                           ,      enc.PAT_ENC_CSN_ID
                           ,      edg.REF_BILL_CODE  AS dx_code
                           ,      edg.DX_NAME        AS dx_name
                           ,      vcd.diagnosis_code AS dx_parent
             from #enc enc
                      left join clarity.dbo.f_diagnosis_info dx on dx.pat_id = enc.PAT_ID
        left join clarity.dbo.clarity_edg edg on edg.DX_ID = dx.DX_ID
        left join clarity.dbo.clarity_edg edg2 on edg2.CURRENT_ICD10_LIST = edg.CURRENT_ICD10_LIST
        left join clarity.dbo.v_cube_d_diagnosis vcd on vcd.diagnosis_id = edg2.PARENT_DX_ID
                where vcd.diagnosis_code is not null
                AND format(dx.LAST_DATE_ENC_DX, 'yyyyMMdd') > FORMAT(DateAdd(dd, -366, getdate()), 'yyyyMMdd')),
    #cci AS (SELECT a.PAT_ID,
        CASE WHEN dx.dx_parent in('I099','I110','I130','I132','I255','I420','I425','I426',
                'I427','I428', 'I429','I43','I50','P290') THEN 'chf' -- Congestive Heart Failure
            WHEN dx.dx_parent in ('I441','I442','I443','I456','I459','I47',
                'I48','I49','R000','R001','R008','T821','Z450','Z950') THEN 'arr' -- Arrhythmia
            WHEN dx.dx_parent in ('A520','I05','I06','I07','I08','I091','I098','I34','I35','I36','I37',
                'I38','I39','Q230','Q231','Q232','Q233','Z952','Z953','Z954') THEN 'vad' -- Valvular Disease
            WHEN dx.dx_parent in ('I26','I27','I280','I288','I289') THEN 'pcd' -- Pulm Circulation Disorders
            WHEN dx.dx_parent in ('I70','I71','I731','I738','I739','I771','I790','I792','K551','K558',
                'K559','Z958','Z959') THEN 'pvd' -- Peripheral Vascular Disease
            WHEN dx.dx_parent in ('I10') THEN 'hyp' --Hypertension Uncomplicated
            WHEN dx.dx_parent in ('I11','I12','I13','I15') THEN 'hyc' -- Hypertension Complicated
            WHEN dx.dx_parent in ('G041','G114','G801','G802','G81','G82','G830','G831','G832','G833',
                'G834','G839') THEN 'par' -- Paralysis
            WHEN dx.dx_parent in ('G10','G11','G12','G13','G20','G21','G22','G254','G255','G312','G318',
                'G319','G32','G35','G36','G37','G40','G41','G931','G934','R470','R56') THEN 'ond' -- Other Neuro Disorders
            WHEN dx.dx_parent in ('I278','I279','J40','J41','J42','J43','J44','J45','J46','J47','J60','J61',
                'J62','J63','J64','J65','J66','J67','J684','J701','J703') THEN 'cpd' --Chronic Pulmonary Disease
            WHEN dx.dx_parent in ('E100','E101','E109','E110','E111','E119','E120','E121','E129','E130',
                'E131','E139','E140','E141','E149') THEN 'dmu' -- Diabetes uncomplicated
            WHEN dx.dx_parent in ('E102','E103','E104','E105','E106','E107','E108','E112','E113','E114','E115',
                'E116','E117','E118','E122','E123','E124','E125','E126','E127','E128','E132','E133','E134','E135',
                'E136','E137','E138','E142','E143','E144','E145','E146','E147','E148') THEN 'dmc' -- Diabetes Complicated
            WHEN dx.dx_parent in ('E00','E01','E02','E03','E890') THEN 'hyt' -- Hypothyroidism
            WHEN dx.dx_parent in ('I120','I131','N18','N19','N250','Z490','Z491','Z492','Z940','Z992') THEN 'ren' -- Renal Failure
            WHEN dx.dx_parent in ('B18','I85','I864','I982','K70','K711','K713','K714','K715','K717','K72','K73',
                'K74','K760','K762','K763','K764','K765','K766','K767','K768','K769','Z944') THEN 'liv' -- Liver Disease
            WHEN dx.dx_parent in ('K257','K259','K267','K269','K277','K279','K287','K289') THEN 'pud' -- Peptic Ulcer Disease
            WHEN dx.dx_parent in ('B20','B21','B22','B24') THEN 'hiv' -- AIDS/HIV
            WHEN dx.dx_parent in ('C81','C82','C83','C84','C85','C88','C96','C900','C902') THEN 'lym' -- Lymphoma
            WHEN dx.dx_parent in ('C77','C78','C79','C80') THEN 'can' -- Metastatic Cancer
            WHEN dx.dx_parent in ('C00','C01','C02','C03','C04','C05','C06','C07','C08','C09','C10','C11','C12','C13',
                'C14','C15','C16','C17','C18','C19','C20','C21','C22','C23','C24','C25','C26','C30','C31','C32','C33',
                'C34','C37','C38','C39','C40','C41','C43','C45','C46','C47','C48','C49','C50','C51','C52','C53','C54',
                'C55','C56','C57','C58','C60','C61','C62','C63','C64','C65','C66','C67','C68','C69','C70','C71','C72',
                'C73','C74','C75','C76','C97') THEN 'tum' -- Solid Tumor without Metastasis
            WHEN dx.dx_parent in ('L940','L941','L943','M05','M06','M08','M120','M123','M30','M310','M311','M312','M313',
                'M32','M33','M34','M35','M45','M461','M468','M469') THEN 'rhe' -- Rheumatoid Arthritis / collagen
            WHEN dx.dx_parent in ('D65','D66','D67','D68','D691','D693','D694','D695','D696') THEN 'col' -- Collagulopathy
            WHEN dx.dx_parent in ('E66') THEN 'obe' -- Obesity
            WHEN dx.dx_parent in ('E40','E41','E42','E43','E44','E45','E46','R634','R64') THEN 'wtl' -- Weight Loss
            WHEN dx.dx_parent in ('E222','E86','E87') THEN 'fed' -- Fluid and Electrolyte Disorders
            WHEN dx.dx_parent in ('D500') THEN 'bla' -- Blood Loss Anemia
            WHEN dx.dx_parent in ('D508','D509','D51','D52','D53') THEN 'ane' -- Deficiency Anemia
            WHEN dx.dx_parent in ('F10','E52','G621','I426','K292','K700','K703','K709','T51','Z502','Z714',
                'Z721') THEN 'alc' -- Alcohol Abuse
            WHEN dx.dx_parent in ('F11','F12','F13','F14','F15','F16','F18','F19','Z715','Z722') THEN 'dru' -- Drug Abuse
            WHEN dx.dx_parent in ('F20','F22','F23','F24','F25','F28','F29','F302','F312','F315') THEN 'psy' -- Psychoses
            WHEN dx.dx_parent in ('F204','F313','F314','F315','F32','F33','F341','F412','F432') THEN 'dep' -- Depression
            else null
    end as cci_cat
    from #enc a
    left join #dx dx on a.PAT_ENC_CSN_ID = dx.PAT_ENC_CSN_ID),
    #ccia as (select distinct pat_id, cci_cat
    from #cci),
    #ccib as (select pat_id,
                      CASE WHEN cci.cci_cat in('chf') THEN 7 -- Congestive Heart Failure
            WHEN cci.cci_cat in ('arr') THEN 5 -- Arrhythmia
            WHEN cci.cci_cat in ('vad') THEN -1 -- Valvular Disease
            WHEN cci.cci_cat in ('pcd') THEN 4 -- Pulm Circulation Disorders
            WHEN cci.cci_cat in ('pvd') THEN 2 -- Peripheral Vascular Disease
            WHEN cci.cci_cat in ('hyp') THEN 0 --Hypertension Uncomplicated
            WHEN cci.cci_cat in ('hyc') THEN 0 -- Hypertension Complicated
            WHEN cci.cci_cat in ('par') THEN 7 -- Paralysis
            WHEN cci.cci_cat in ('ond') THEN 6 -- Other Neuro Disorders
            WHEN cci.cci_cat in ('cpd') THEN 3 --Chronic Pulmonary Disease
            WHEN cci.cci_cat in ('dmu') THEN 0 -- Diabetes uncomplicated
            WHEN cci.cci_cat in ('dmc') THEN 0 -- Diabetes Complicated
            WHEN cci.cci_cat in ('hyt') THEN 0 -- Hypothyroidism
            WHEN cci.cci_cat in ('ren') THEN 5 -- Renal Failure
            WHEN cci.cci_cat in ('liv') THEN 11 -- Liver Disease
            WHEN cci.cci_cat in ('pud') THEN 0 -- Peptic Ulcer Disease
            WHEN cci.cci_cat in ('hiv') THEN 0 -- AIDS/HIV
            WHEN cci.cci_cat in ('lym') THEN 9 -- Lymphoma
            WHEN cci.cci_cat in ('can') THEN 12 -- Metastatic Cancer
            WHEN cci.cci_cat in ('tum') THEN 4 -- Solid Tumor without Metastasis
            WHEN cci.cci_cat in ('rhe') THEN 0 -- Rheumatoid Arthritis / collagen
            WHEN cci.cci_cat in ('col') THEN 3 -- Collagulopathy
            WHEN cci.cci_cat in ('obe') THEN -4 -- Obesity
            WHEN cci.cci_cat in ('wtl') THEN 6 -- Weight Loss
            WHEN cci.cci_cat in ('fed') THEN 5 -- Fluid and Electrolyte Disorders
            WHEN cci.cci_cat in ('bla') THEN -2 -- Blood Loss Anemia
            WHEN cci.cci_cat in ('ane') THEN -2 -- Deficiency Anemia
            WHEN cci.cci_cat in ('alc') THEN 0 -- Alcohol Abuse
            WHEN cci.cci_cat in ('dru') THEN -7 -- Drug Abuse
            WHEN cci.cci_cat in ('psy') THEN 0 -- Psychoses
            WHEN cci.cci_cat in ('dep') THEN -3 -- Depression
            else 0
    end as cci_score
              from #ccia cci),
    #cci2 as (SELECT  cci.pat_id, SUM(cci.cci_score) AS cci_score
        FROM #ccib cci
        group by cci.pat_id),
--Diabetes
    #dm as (SELECT a.PAT_ID, 'YES' as diabetes_present, 1 as diabetes_num
    from #enc a
    left join #dx dx on a.PAT_ID = dx.PAT_ID
        where dx.dx_parent in ('E10', 'E11', 'E12', 'E13','E14', 'E15', 'E16')),
--CRE in the past
    #cre_lab as (SELECT distinct e.PAT_ID
    , op.ORDER_PROC_ID
	, specimen_collect_time = FORMAT(op2.SPECIMN_TAKEN_TIME,'yyyyMMdd HH:mm')	--same as FORMAT(sdm.SPEC_DTM_COLLECTED,'yyyyMMdd HH:mm')
	, res.RESULT_ID
	, rm.CULT_ORG_ID
    , corg.RECORD_TYPE_C
    , corg.external_name
    , 1 as cre_prev
	FROM #enc e
	Inner Join Clarity.dbo.PAT_ENC_HSP peh ON e.PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
	Inner Join Clarity.dbo.ORDER_PROC op ON e.pat_id = op.pat_id
	Inner Join Clarity.dbo.ORDER_PROC_2 op2 ON op.ORDER_PROC_ID = op2.ORDER_PROC_ID
	Left Outer Join Clarity.dbo.RES_DB_MAIN res ON op.ORDER_PROC_ID = res.RES_ORDER_ID
	Left Outer Join Clarity.dbo.RES_COMPONENTS rc ON res.RESULT_ID = rc.RESULT_ID --AND rc.COMPONENT_ID<>164		--lab status
	inner Join Clarity.dbo.ORDER_RESULTS rst ON res.RES_ORDER_ID = rst.ORDER_PROC_ID
	AND rc.COMPONENT_ID = rst.COMPONENT_ID AND rst.COMPONENT_ID<>164			--LABSTATUS
	Left Outer Join Clarity.dbo.RES_MICRO_CULTURE rm ON rc.RESULT_ID = rm.RESULT_ID			--by result_ID. same RESULT_ID may have 1+ CULT_ORG_ID(ORGANISM_ID). CULT_RSLTD_INS_DTTM
		AND rc.LINE = rm.LINE
	left outer join Clarity.dbo.CLARITY_ORGANISM corg ON corg.ORGANISM_ID = rm.CULT_ORG_ID
	WHERE	1=1
			and op.ORDER_TYPE_C in (3)	--Microbiology
			AND rst.ORD_VALUE IS NOT NULL
            AND rm.CULT_ORG_ID IS NOT NULL
	        AND corg.record_type_c = '4'
            AND corg.external_name like '%CRE%'),
    #cre as (select distinct pat_id, 1 as cre_prev
             from clarity.dbo.INFECTIONS
             where 1=1
             AND INFECTION_TYPE_C = '85'
             AND INF_STATUS_C <> '2'
             AND INF_STATUS_C <> '4'),
--Pull it all together....
    #prealert as (select enc.pat_id as patient_id,
--     einfo.patient_embi_nbr
       enc.PAT_ENC_CSN_ID as encounter_nbr,
       einfo.mrn as medical_record_number,
       pat.pat_first_name as patient_first_name,
       pat.pat_last_name as patient_last_name,
       pat.current_age as patient_age_at_visit,
       pat.gender,
       einfo.unit as unit,
       einfo.facility,
       einfo.admit_source as admit_type,
       einfo.admit_day,
       einfo.admit_timestamp,
       einfo.financial_class_id,
       einfo.financial_class_desc,
       case when hosp.prev_hosp is null then 0 else hosp.prev_hosp end as prior_hosp,
       ISNULL(los.hosp_total_los, 0) as los_prev,
       ISNULL(los.hosp_avg_los, 0) as los_prev_mean,
       case when icu.icu_los_days > 0 then 'YES' else 'NO' end as icu_prev,
       case when icu.icu_los_days > 0 then 1 else 0 end as icu_prev_num,
       case when loc.icu_los_days > 0 then 'YES' else 'NO' end as icu_24h,
       ISNULL(imm.imm_comp_prev,'NO') as imm_comp_prev,
       case when inf.inf_prev = 1 then 'YES' else 'NO' end as inf_prev,
       case when inf.inf_prev = 1 then 1 else 0 end as inf_prev_num,
       case when vent.vent_id is not null then 'YES' else 'NO' end as vent_prev,
       case when dot.prev_dot is null then 0 else dot.prev_dot end as prev_dot,
       case when dot.prev_dot is null then 0 else log(dot.prev_dot) end as prev_dot_log,
       case when bdot.prev_dot is null then 0 else bdot.prev_dot end as prev_blactam,
       ISNULL(dm.diabetes_present,'NO') as diabetes_present,
       case when dm.diabetes_num = 1 then 1 else 0 end as diabetes_num,
       cre.cre_prev,
       crelab.cre_prev as cre_lab_prev,
       cci.cci_score
       --count(concat(dot.PAT_ID, dot.med_order_day, dot.medication_id)) as prev_dot,
       --count(concat(dot1.PAT_ENC_CSN_ID, dot1.med_order_day, dot.medication_id)) as enc_dot,
       --concat(dot.PAT_ID, dot.med_order_day) as prev_dot_name,  --testing
       --concat(dot1.PAT_ENC_CSN_ID, dot1.med_order_day) as enc_dot_name  --testing
--     cre
from #enc enc
left join #pat pat on pat.pat_id = enc.PAT_ID
left join #encinfo einfo on einfo.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
left join #hosp2 hosp on enc.PAT_ID = hosp.PAT_ID
left join #hosplos los on enc.pat_id = los.pat_id
left join #icu2 icu on enc.pat_id = icu.PAT_ID
left join #iculoc loc on enc.PAT_ENC_CSN_ID = loc.PAT_ENC_CSN_ID
left join #imm imm on enc.PAT_ID = imm.PAT_ID
left join #inf inf on enc.pat_id = inf.pat_id
left join #vent vent on enc.pat_id = vent.PAT_ID
left join #dot2 dot on enc.PAT_ID = dot.PAT_ID
left join #bdot2 bdot on enc.pat_id = bdot.PAT_ID
left join #dotcurrent dot1 on enc.PAT_ENC_CSN_ID = dot1.PAT_ENC_CSN_ID
LEFT JOIN #cci2 as cci on enc.PAT_ID = cci.PAT_ID
left join #dm as dm on dm.PAT_ID = enc.pat_id
left join #cre as cre on cre.pat_id = enc.pat_id
left join #cre_lab as crelab on crelab.pat_id = enc.pat_id
group by enc.pat_id,
--     einfo.patient_embi_nbr
       enc.PAT_ENC_CSN_ID,
       einfo.mrn,
       pat.pat_first_name,
       pat.pat_last_name,
       pat.current_age,
       pat.gender,
       einfo.unit,
       einfo.facility,
       einfo.los,
       einfo.admit_source,
       einfo.discharge_disposition,
       einfo.admit_day,
       einfo.admit_timestamp,
       einfo.financial_class_id,
       einfo.financial_class_desc,
       hosp.prev_hosp,
       ISNULL(los.hosp_total_los,0),
       ISNULL(los.hosp_avg_los,0),
       case when icu.icu_los_days > 0 then 'YES' else 'NO' end,
       case when icu.icu_los_days > 0 then 1 else 0 end,
       case when loc.icu_los_days > 0 then 'YES' else 'NO' end,
       ISNULL(imm.imm_comp_prev,'NO'),
       case when inf.inf_prev = 1 then 'YES' else 'NO' end,
       case when inf.inf_prev = 1 then 1 else 0 end,
       case when vent.vent_id is not null then 'YES' else 'NO' end,
       dot.prev_dot,
       case when dot.prev_dot is null then 0 else log(dot.prev_dot) end,
       case when bdot.prev_dot is null then 0 else bdot.prev_dot end,
       ISNULL(dm.diabetes_present,'NO'),
       case when dm.diabetes_num = 1 then 1 else 0 end,
       cre.cre_prev,
       crelab.cre_prev,
       cci.cci_score),
--Calculate risk score
    #prealert2 as (select pre.patient_id, pre.encounter_nbr, pre.medical_record_number, pre.patient_first_name,
       pre.patient_last_name, pre.patient_age_at_visit, pre.gender, pre.unit, pre.facility,
       pre.admit_type, pre.admit_day, pre.admit_timestamp, pre.financial_class_desc,
       pre.prior_hosp, pre.los_prev, pre.los_prev_mean, pre.icu_prev, --pre.icu_24h, pre.imm_comp_prev,
       pre.inf_prev, pre.vent_prev, pre.prev_dot, pre.prev_blactam,
       pre.diabetes_present, pre.cre_prev, pre.cre_lab_prev, pre.cci_score,
       ((-10.7102)+1.058*cast(pre.inf_prev_num as int) + 0.7964*cast(pre.icu_prev_num as int) + 0.0129*cast(pre.patient_age_at_visit as int) + 0.0338*cast(pre.cci_score as int) + 0.0143*cast(pre.los_prev_mean as int) + 0.3348*cast(pre.prev_dot_log as numeric) + 0.5179*cast(pre.diabetes_num as int)) as score--,
    from #prealert pre)
select pre.*, round((exp(pre.score)/(1+exp(pre.score))*100),2) as prealert_score
from #prealert2 pre
       ;

--OLD VERSION
--((-10.1571)+1.4302*cast(pre.inf_prev_num as int) + 0.8062*cast(pre.icu_prev_num as int) + 0.0209*cast(pre.prior_hosp as int) + 0.2153*cast(pre.cci_score as int) + 0.0146*cast(pre.los_prev_mean as int) + 0.0214*cast(pre.prev_blactam as int)) as score--,
--Version 01/16/24
--((-9.8147)+1.4157*cast(pre.inf_prev_num as int) + 0.8817*cast(pre.icu_prev_num as int) + 0.0245*cast(pre.prior_hosp as int) + 0.0387*cast(pre.cci_score as int) + 0.0151*cast(pre.los_prev_mean as int) + 0.0197*cast(pre.prev_blactam as int) + 0.549*cast(pre.diabetes_present as int)) as score--,
--VERSION 02/15/24
--((-10.7102)+1.058*cast(pre.inf_prev_num as int) + 0.7964*cast(pre.icu_prev_num as int) + 0.0129*cast(pre.current_age) + 0.0338*cast(pre.cci_score as int) + 0.0143*cast(pre.los_prev_mean as int) + 0.3348*cast(pre.prev_dot_log as int) + 0.5179*cast(pre.diabetes_present as int)) as score,