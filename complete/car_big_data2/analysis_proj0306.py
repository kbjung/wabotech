#-*- coding: utf-8 -*-
import os
import traceback
import time
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from sklearn import datasets, linear_model
import pyexasol

class AnalysisTaskProj0306:
    """
    분석과제 03~06 관련 파이썬 기반 분석기능을 구현한 클래스
    """

    def __init__(self):
        """
        생성자

        :param dbUtil: DB 유틸리티 객체
        """
        
        self.dbUtil = pyexasol.connect(dsn='dev.openankus.org:8563', user='sys',password='djslzja', compression=True)
        self.dbUtil2 = pyexasol.connect(dsn='dev.openankus.org:8563', user='sys',password='djslzja', compression=True,schema='vsysb')

        try:

            # 시작 시간 저장
            start_time = time.time()
            print("<STD_BD_CAR_INSP_HST 데이터프레임 생성>")
            # STD_BD_CAR_INSP_HST 공용 데이터 로드
            sql_stmt = """
                                SELECT
            						CO_1_ITM_JGMT_YN, CO_1_ITM_MEVLU, CO_1_ITM_PRMT_VAL, CO_CO2_VAL
            						, DRVNG_DSTNC, EQMT_CD_NM, EXHST_GAS_CERT_NO, HC_1_ITM_JGMT_YN
            						, HC_1_ITM_MEVLU, HC_1_ITM_PRMT_VAL, HC_CO2_VAL
            						, INSP_MSRMT_MTHD, INSP_PASAGE_YN, INSP_SE, INSP_YMD
            						, LAW_CO_PRMT_VAL, LAW_HC_PRMT_VAL, LAW_NOX_PRMT_VAL, LAW_SMO_PRMT_VAL
            						, NMOG_NOX_PRMT_VAL, NMOG_NOX_VAL, NMOG_PRMT_VAL, NMOG_VAL
            						, NO_CO2_VAL, NOX_JGMT_YN, NOX_MEVLU, NOX_PRMT_VAL
            						, PM_PRMT_VAL, PM_VAL, SMO_1_ITM_JGMT_YN, SMO_1_ITM_MEVLU
            						, SMO_1_ITM_PRMT_VAL, VHRNO, VIN, VSP
                                FROM 
                                    "vsysb".STD_BD_CAR_INSP_HST
                                """

            self.INSP = self.dbUtil.export_to_pandas(sql_stmt)
            self.INSP['CO_1_ITM_MEVLU'] = pd.to_numeric(self.INSP['CO_1_ITM_MEVLU'], errors='coerce')
            self.INSP['CO_1_ITM_PRMT_VAL'] = pd.to_numeric(self.INSP['CO_1_ITM_PRMT_VAL'], errors='coerce')
            self.INSP['CO_CO2_VAL'] = pd.to_numeric(self.INSP['CO_CO2_VAL'], errors='coerce')
            self.INSP['DRVNG_DSTNC'] = self.INSP['DRVNG_DSTNC'].astype('float')
            self.INSP['HC_1_ITM_MEVLU'] = pd.to_numeric(self.INSP['HC_1_ITM_MEVLU'], errors='coerce')
            self.INSP['HC_1_ITM_PRMT_VAL'] = pd.to_numeric(self.INSP['HC_1_ITM_PRMT_VAL'], errors='coerce')
            self.INSP['HC_CO2_VAL'] = pd.to_numeric(self.INSP['HC_CO2_VAL'], errors='coerce')
            self.INSP['INSP_YMD'] = pd.to_numeric(self.INSP['INSP_YMD'], errors='coerce')
            self.INSP['INSP_YMD'] = pd.to_datetime(self.INSP['INSP_YMD'], format='%Y%m%d', errors = 'coerce')
            self.INSP['NMOG_NOX_PRMT_VAL'] = pd.to_numeric(self.INSP['NMOG_NOX_PRMT_VAL'], errors='coerce')
            self.INSP['NMOG_NOX_VAL'] = pd.to_numeric(self.INSP['NMOG_NOX_VAL'], errors='coerce')
            self.INSP['NMOG_PRMT_VAL'] = pd.to_numeric(self.INSP['NMOG_PRMT_VAL'], errors='coerce')
            self.INSP['NMOG_VAL'] = pd.to_numeric(self.INSP['NMOG_VAL'], errors='coerce')
            self.INSP['NO_CO2_VAL'] = pd.to_numeric(self.INSP['NO_CO2_VAL'], errors='coerce')
            self.INSP['NOX_MEVLU'] = pd.to_numeric(self.INSP['NOX_MEVLU'], errors='coerce')
            self.INSP['NOX_PRMT_VAL'] = pd.to_numeric(self.INSP['NOX_PRMT_VAL'], errors='coerce')
            self.INSP['PM_PRMT_VAL'] = pd.to_numeric(self.INSP['PM_PRMT_VAL'], errors='coerce')
            self.INSP['PM_VAL'] = pd.to_numeric(self.INSP['PM_VAL'], errors='coerce')
            self.INSP['SMO_1_ITM_MEVLU'] = pd.to_numeric(self.INSP['SMO_1_ITM_MEVLU'], errors='coerce')
            self.INSP['SMO_1_ITM_PRMT_VAL'] = pd.to_numeric(self.INSP['SMO_1_ITM_PRMT_VAL'], errors='coerce')
            self.INSP['VSP'] = pd.to_numeric(self.INSP['VSP'], errors='coerce')
            
            # 종료시간 저장
            end_time = time.time()
            print("</STD_BD_CAR_INSP_HST 데이터프레임 생성 (수행시간: %d초)>" % (end_time - start_time))

            start_time = time.time()
            print("<STD_BD_CAR 데이터프레임 생성>")
            # STD_BD_CAR 공용 데이터 로드
            sql_stmt = """
                        SELECT /*+ PARALLEL(2) */
                            ACQS_YMD, BSPL_STDG_CD, EXHST_GAS_CERT_NO
                            , EXHST_GAS_GRD_CD_NM, FRST_REG_YMD, FUEL_CD_NM
                            , FUEL_UP_CD_NM, STDG_CTPV_NM, VHCL_ERSR_YN
                            , VHCL_FBCTN_YMD, VHCTY_CD_NM, VHRNO, VIN, YRIDNW
                        FROM 
                            "vsysb".STD_BD_CAR
            """
            self.CAR = self.dbUtil.export_to_pandas(sql_stmt)
            self.CAR['BSPL_STDG_CD'] = pd.to_numeric(self.CAR['BSPL_STDG_CD'], errors='coerce')
            self.CAR['FRST_REG_YMD'] = pd.to_numeric(self.CAR['FRST_REG_YMD'], errors='coerce')
            self.CAR['FRST_REG_YMD'] = pd.to_datetime(self.CAR['FRST_REG_YMD'], format='%Y%m%d', errors = 'coerce')
            self.CAR['VHCL_FBCTN_YMD'] = pd.to_numeric(self.CAR['VHCL_FBCTN_YMD'], errors='coerce')
            self.CAR['VHCL_FBCTN_YMD'] = pd.to_datetime(self.CAR['VHCL_FBCTN_YMD'], format='%Y%m%d', errors = 'coerce')
            self.CAR['YRIDNW'] = pd.to_numeric(self.CAR['YRIDNW'], errors='coerce')
            end_time = time.time()
            print("</STD_BD_CAR 데이터프레임 생성 (수행시간: %d초)>" % (end_time - start_time))

            start_time = time.time()
            print("<STD_BD_EXHST_GAS_CERT_NO_DAT 데이터프레임 생성>")
            sql_stmt = """
                                SELECT
                                    *
                                FROM 
                                    "vsysb".STD_BD_EXHST_GAS_CERT_NO_DAT
                                """

            self.EXHST_GAS_CERT = self.dbUtil.export_to_pandas(sql_stmt)
            self.EXHST_GAS_CERT['CERT_YR'] = self.EXHST_GAS_CERT['CERT_YR'].astype('float')
            self.EXHST_GAS_CERT['RPRS_YRIDNW'] = self.EXHST_GAS_CERT['RPRS_YRIDNW'].astype('float')
            end_time = time.time()
            print("</STD_BD_EXHST_GAS_CERT_NO_DAT 데이터프레임 생성 (수행시간: %d초)>" % (end_time - start_time))

            start_time = time.time()
            print("<STD_BD_LWEM_ACTN_HST 데이터프레임 생성>")
            sql_stmt = """
                               SELECT /*+  PARALLEL(2) */
                                   *
                               FROM
                                   "vsysb".STD_BD_LWEM_ACTN_HST
                                WHERE ACTN_SE IN ('조기폐차', '저감장치부착', '저감장치클리닝')
                               """
            self.LWEM_ACTN_HST = self.dbUtil.export_to_pandas(sql_stmt)
            self.LWEM_ACTN_HST['ACTN_YMD'] = pd.to_numeric(self.LWEM_ACTN_HST['ACTN_YMD'], errors='coerce')
            self.LWEM_ACTN_HST['ACTN_YMD'] = pd.to_datetime(self.LWEM_ACTN_HST['ACTN_YMD'], format='%Y%m%d', errors = 'coerce')
            self.LWEM_ACTN_HST['BF_SMO_CCNTRA'] = pd.to_numeric(self.LWEM_ACTN_HST['BF_SMO_CCNTRA'], errors='coerce')
            self.LWEM_ACTN_HST['PST_SMO_CCNTRA'] = pd.to_numeric(self.LWEM_ACTN_HST['PST_SMO_CCNTRA'], errors='coerce')
            print("</STD_BD_LWEM_ACTN_HST 데이터프레임 생성 (수행시간: %d초)>" % (end_time - start_time))

        except Exception as e:
            tb_str = traceback.format_exc()
            print(tb_str)
            raise e

    def roundTraditional(self, val, digits):
        # 기존의 round 함수는 앞의 자리가 홀수일 경우일 때만 올리므로, 반올림 함수 생성
        return round(val + 10**(-(digits + 2)), digits)


    def proj03_analysis_01(self):
        """
        인증시험 분석 01(구현 분석가: 김강민)
        :return:
        """

        # # STD_BD_CAR_INSP_HST 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT /*+ PARALLEL(2) */
        #                 VIN, VHRNO, INSP_MSRMT_MTHD, INSP_YMD, CO_1_ITM_MEVLU, HC_1_ITM_MEVLU, NOX_MEVLU, SMO_1_ITM_MEVLU, LAW_CO_PRMT_VAL, LAW_HC_PRMT_VAL, LAW_NOX_PRMT_VAL, LAW_SMO_PRMT_VAL, INSP_SE, NMOG_PRMT_VAL, NMOG_VAL,  NMOG_NOX_PRMT_VAL, NMOG_NOX_VAL, PM_PRMT_VAL,
        #                 PM_VAL, EXHST_GAS_CERT_NO, CO_1_ITM_PRMT_VAL, NOX_PRMT_VAL
        #             FROM
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 ((INSP_SE = '정기검사' OR INSP_SE = '정밀검사') AND INSP_PASAGE_YN = 'Y')
        #                 OR INSP_SE = '인증시험'
        #             """
        #
        # INSP = self.dbUtil.export_to_pandas(sql_stmt)
        
        INSP = self.INSP[((self.INSP['INSP_SE'].isin(['정기검사', '정밀검사'])) & (self.INSP['INSP_PASAGE_YN'] == 'Y')) | (self.INSP['INSP_SE'] == '인증시험')].copy()



        # # STD_BD_CAR 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT /*+ PARALLEL(2) */
        #                 VIN, VHRNO, FUEL_UP_CD_NM, VHCL_FBCTN_YMD, EXHST_GAS_CERT_NO
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             WHERE
        #                 VHCL_ERSR_YN = 'N'
        #             """
        #
        # CAR = self.dbUtil.export_to_pandas(sql_stmt)

        CAR = self.CAR[self.CAR['VHCL_ERSR_YN'] == 'N'].copy()



        # # STD_BD_EXHST_GAS_CERT_NO_DAT 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT
        #                 *
        #             FROM
        #                 "vsysb".STD_BD_EXHST_GAS_CERT_NO_DAT
        #             """
        #
        # EXDAT = self.dbUtil.export_to_pandas(sql_stmt)

        EXDAT = self.EXHST_GAS_CERT.copy()



        # 배출가스별 SI = 측정치 / 허용치 (현재 허용치에 값들이 완전하게 존재하는 것이 아니므로 법령 기준으로 입력한 허용치 사용) 계산
        INSP['CO_SI'] = INSP['CO_1_ITM_MEVLU'] / INSP['LAW_CO_PRMT_VAL']
        INSP['HC_SI'] = INSP['HC_1_ITM_MEVLU'] / INSP['LAW_HC_PRMT_VAL']
        INSP['NOX_SI'] = INSP['NOX_MEVLU'] / INSP['LAW_NOX_PRMT_VAL']
        INSP['SMOKE_SI'] = INSP['SMO_1_ITM_MEVLU'] / INSP['LAW_SMO_PRMT_VAL']

        CERT = INSP[INSP['INSP_SE'] == '인증시험']
        INSP = INSP[(INSP['INSP_SE'].isin(['정기검사', '정밀검사']))]
        INSP.drop(columns=['EXHST_GAS_CERT_NO'], inplace=True)


        INSP.sort_values(by=['VIN', 'VHRNO', 'INSP_YMD'], inplace=True)

        # 검사이력 데이터에서 인증시험에서 사용하는 부하검사 데이터만 추출
        insp = INSP[INSP['INSP_MSRMT_MTHD'].isin(['부하검사(ASM-Idling)', '부하검사(ASM2525)', '부하검사(LUG DOWN)', '부하검사(KD-147)'])]

        insp = insp.merge(CAR, how='left', on = ['VIN', 'VHRNO'])


        # 연료가 없는 경우 제거
        insp = insp[insp['FUEL_UP_CD_NM'].notnull()]

        # 차대번호, 차량번호, 검사방법 기준으로 가장 최근 검사이력만 추출
        insp.drop_duplicates(['VIN', 'VHRNO', 'INSP_MSRMT_MTHD'], keep='last', inplace=True)

        # 인증시험 데이터에서 배출가스인증번호가 제작차 인 경우만 추출('-'(하이픈)이 3개이고 첫번째 '-' 다음 글자가 2개인 경우)
        cert = CERT[(CERT['EXHST_GAS_CERT_NO'].str.split('-').str[1].str.len() == 2) & (
                    CERT['EXHST_GAS_CERT_NO'].str.split('-').str.len() == 4)]


        # 인증시험의 허용치가 0인 경우 제거
        cert = cert[(cert['CO_1_ITM_PRMT_VAL'] != 0) & (cert['NMOG_PRMT_VAL'] != 0) & (cert['NOX_PRMT_VAL'] != 0) & (
                    cert['NMOG_NOX_PRMT_VAL'] != 0) & (cert['PM_PRMT_VAL'] != 0)]

        # 인증시험의 SI 계산
        # NOX_SI의 경우 NULL값이 대다수이므로 NMOG_NOX_SI로 대체
        cert['CO_SI'] = cert['CO_1_ITM_MEVLU'] / cert['CO_1_ITM_PRMT_VAL']
        cert['HC_SI'] = cert['NMOG_VAL'] / cert['NMOG_PRMT_VAL']
        # cert['NOX_SI'] = cert['NOX_MEVLU'] / cert['NOX_PRMT_VAL']
        cert['NMOG_NOX_SI'] = cert['NMOG_NOX_VAL'] / cert['NMOG_NOX_PRMT_VAL']
        cert['SMOKE_SI'] = cert['PM_VAL'] / cert['PM_PRMT_VAL']

        # 배출가스 인증번호와 검사방법에 대해서는 하나의 시리얼 번호(시리얼 번호가 가장 작은 경우)만 존재
        cert = cert.merge(cert.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD']).size().reset_index().sort_values(
            by=['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD']).drop_duplicates(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD'],
                                                                         keep='first').loc[:,
                          ['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD']], how='inner')

        # 배출가스별 SI에 순위 부여
        cert['CO_ORD'] = cert.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD'])['CO_SI'].rank(method='dense',
                                                                                              ascending=False,
                                                                                              na_option='bottom')
        cert['HC_ORD'] = cert.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD'])['HC_SI'].rank(method='dense',
                                                                                              ascending=False,
                                                                                              na_option='bottom')
        cert['NMOG_NOX_ORD'] = cert.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD'])['NMOG_NOX_SI'].rank(
            method='dense', ascending=False, na_option='bottom')
        cert['SMOKE_ORD'] = cert.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD'])['SMOKE_SI'].rank(method='dense',
                                                                                                    ascending=False,
                                                                                                    na_option='bottom')

        # 배출가스별 순위의 곱이 작은 경우만 추출
        cert['TOTL_ORD'] = cert['CO_ORD'] * cert['HC_ORD'] * cert['NMOG_NOX_ORD'] * cert['SMOKE_ORD']

        cert.sort_values(by=['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'TOTL_ORD'], inplace=True)

        cert = cert.drop_duplicates(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'TOTL_ORD'], keep='first')

        # CVS-75검사를 받은 배출가스 인증번호와 매핑되는 검사 방법인 '부하검사(ASM-Idling)' 과 '부하검사(ASM2525)' 이면서 연료가 휘발유 혹은 가스인 경우만 추출
        cvs75 = insp[
            ((insp['INSP_MSRMT_MTHD'] == '부하검사(ASM-Idling)') | (insp['INSP_MSRMT_MTHD'] == '부하검사(ASM2525)')) & (
                        (insp['FUEL_UP_CD_NM'] == '휘발유') | (insp['FUEL_UP_CD_NM'] == '가스'))].drop(
            columns=['CO_SI', 'HC_SI', 'SMOKE_SI', 'NOX_SI']).merge(cert[cert['INSP_MSRMT_MTHD'] == 'CVS-75'].loc[:,
                                                                    ['EXHST_GAS_CERT_NO', 'CO_SI', 'HC_SI',
                                                                     'NMOG_NOX_SI', 'SMOKE_SI']], how='inner',
                                                                    on='EXHST_GAS_CERT_NO')

        # ECE15+EUDC검사를 받은 배출가스 인증번호와 매핑되는 검사 방법인 '부하검사(LUG DOWN)', 연료가 경유이면서 제작연도가 20130101 ~ 20170931인 경우만 추출
        ec = insp[(insp['INSP_MSRMT_MTHD'] == '부하검사(LUG DOWN)') & (insp['FUEL_UP_CD_NM'] == '경유') & (
                    insp['VHCL_FBCTN_YMD'] >= 20130101) & (insp['VHCL_FBCTN_YMD'] <= 20170931)].drop(
            columns=['CO_SI', 'HC_SI', 'SMOKE_SI', 'NOX_SI']).merge(cert[cert['INSP_MSRMT_MTHD'] == 'ECE15+EUDC'].loc[:,
                                                                    ['EXHST_GAS_CERT_NO', 'CO_SI', 'HC_SI',
                                                                     'NMOG_NOX_SI', 'SMOKE_SI']], how='inner',
                                                                    on='EXHST_GAS_CERT_NO')

        # WLTC검사를 받은 배출가스 인증번호와 매핑되는 검사 방법인 '부하검사(KD-147)', 연료가 경유이면서 제작연도가 20171001 이후인 경우만 추출
        wltc = insp[(insp['INSP_MSRMT_MTHD'] == '부하검사(KD-147)') & (insp['FUEL_UP_CD_NM'] == '경유') & (
                    insp['VHCL_FBCTN_YMD'] >= 20171001)].drop(
            columns=['CO_SI', 'HC_SI', 'SMOKE_SI', 'NOX_SI']).merge(cert[cert['INSP_MSRMT_MTHD'] == 'WLTC'].loc[:,
                                                                    ['EXHST_GAS_CERT_NO', 'CO_SI', 'HC_SI',
                                                                     'NMOG_NOX_SI', 'SMOKE_SI']], how='inner',
                                                                    on='EXHST_GAS_CERT_NO')

        CVS = pd.concat([cvs75, ec, wltc])

        # 인증시험 데이터의 배출가스 인증번호별 배출가스 SI의 평균
        cvs = CVS.groupby(['EXHST_GAS_CERT_NO'])[['CO_SI', 'HC_SI', 'NMOG_NOX_SI', 'SMOKE_SI']].agg(
            TEST_AVRG_CO_SI=('CO_SI', 'mean'), TEST_AVRG_HC_SI=('HC_SI', 'mean'),
            TEST_AVRG_NOX_SI=('NMOG_NOX_SI', 'mean'), TEST_AVRG_SMOKE_SI=('SMOKE_SI', 'mean')).reset_index()

        # 배출가스 SI별 구간 구분
        def unit_1(g):
            cvs_s = (cvs[g] >= 0) & (cvs[g] < 0.1)
            cvs.loc[cvs_s, g + '_SCOP'] = '0 <= x < 0.1'
            cvs_s = (cvs[g] >= 0.1) & (cvs[g] < 0.2)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.1 <= x < 0.2'
            cvs_s = (cvs[g] >= 0.2) & (cvs[g] < 0.3)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.2 <= x < 0.3'
            cvs_s = (cvs[g] >= 0.3) & (cvs[g] < 0.4)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.3 <= x < 0.4'
            cvs_s = (cvs[g] >= 0.4) & (cvs[g] < 0.5)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.4 <= x < 0.5'
            cvs_s = (cvs[g] >= 0.5) & (cvs[g] < 0.6)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.5 <= x < 0.6'
            cvs_s = (cvs[g] >= 0.6) & (cvs[g] < 0.7)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.6 <= x < 0.7'
            cvs_s = (cvs[g] >= 0.7) & (cvs[g] < 0.8)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.7 <= x < 0.8'
            cvs_s = (cvs[g] >= 0.8) & (cvs[g] < 0.9)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.8 <= x < 0.9'
            cvs_s = (cvs[g] >= 0.9) & (cvs[g] <= 1)
            cvs.loc[cvs_s, g + '_SCOP'] = '0.9 <= x <= 1'

        unit_1('TEST_AVRG_CO_SI')
        unit_1('TEST_AVRG_HC_SI')
        unit_1('TEST_AVRG_NOX_SI')
        unit_1('TEST_AVRG_SMOKE_SI')
        
        cvs_exdat = cvs.merge(EXDAT, how='left')
        
        # 테이블 생성 연월일
        cvs_exdat['LOAD_DT'] = datetime.date.today()

        
        table_nm = 'std_bd_cert_test_avrg'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(cvs_exdat.columns):
            if 'float' in cvs_exdat[column].dtype.name:
                sql += column+' float'
            elif 'int' in cvs_exdat[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(cvs_exdat.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)
        
        # 데이터프레임을 DB에 적재
        self.dbUtil2.import_from_pandas(cvs_exdat, table_nm)

        # STD_BD_EXHST_GAS_CERT_NO_ANL 테이블 중 필요 컬럼만 추출
        sql_stmt = """
                    SELECT
                        *
                    FROM 
                        "vsysb".STD_BD_EXHST_GAS_CERT_NO_ANL
                    """

        ANL = self.dbUtil.export_to_pandas(sql_stmt)

        # 인증시험데이터가 존재하는 배출가스 인증번호의 그래프 좌표 산출
        anl_t = ANL.merge(cvs, how='inner', on='EXHST_GAS_CERT_NO')
        
        # 테이블 생성 연월일
        anl_t['LOAD_DT'] = datetime.date.today()

        
        table_nm = 'std_bd_cert_test_slope'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(anl_t.columns):
            if 'float' in anl_t[column].dtype.name:
                sql += column+' DOUBLE PRECISION'
            elif 'int' in anl_t[column].dtype.name:
                sql += column+' DOUBLE PRECISION'
            else:
                sql += column+' varchar(255)'

            if len(anl_t.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)
        # 데이터프레임을 DB에 적재
        self.dbUtil2.import_from_pandas(anl_t, table_nm)

        anl = ANL.drop_duplicates(['EXHST_GAS_CERT_NO', 'TRGT_ATTR'])

        # 차량 별 최근 검사 이력 데이터 추출
        insp_f = INSP.drop_duplicates(['VIN', 'VHRNO'], keep='last')

        insp_f = insp_f.merge(CAR, how='left', on=['VIN', 'VHRNO'])

        insp_f = insp_f.merge(
            anl.loc[:, ['EXHST_GAS_CERT_NO', 'RPRS_MKR', 'RPRS_VHCNM']].drop_duplicates(['EXHST_GAS_CERT_NO']),
            how='inner', on='EXHST_GAS_CERT_NO')

        # 인증시험별 배출가스인증번호의 기울기가 양수인 경우만 추출
        cvs75_exno = pd.DataFrame(cvs75['EXHST_GAS_CERT_NO'].unique(), columns=['EXHST_GAS_CERT_NO'])
        cvs75_exno = cvs75_exno.merge(
            anl.loc[:, ['EXHST_GAS_CERT_NO', 'RPRS_MKR', 'RPRS_VHCNM']].drop_duplicates(['EXHST_GAS_CERT_NO']),
            how='inner')

        ec_exno = pd.DataFrame(ec['EXHST_GAS_CERT_NO'].unique(), columns=['EXHST_GAS_CERT_NO'])
        ec_exno = ec_exno.merge(
            anl.loc[:, ['EXHST_GAS_CERT_NO', 'RPRS_MKR', 'RPRS_VHCNM']].drop_duplicates(['EXHST_GAS_CERT_NO']),
            how='inner')

        wltc_exno = pd.DataFrame(wltc['EXHST_GAS_CERT_NO'].unique(), columns=['EXHST_GAS_CERT_NO'])
        wltc_exno = wltc_exno.merge(
            anl.loc[:, ['EXHST_GAS_CERT_NO', 'RPRS_MKR', 'RPRS_VHCNM']].drop_duplicates(['EXHST_GAS_CERT_NO']),
            how='inner')

        cvs75_f = cert[cert['INSP_MSRMT_MTHD'] == 'CVS-75'].merge(cvs75_exno, how='inner')
        ec_f = cert[cert['INSP_MSRMT_MTHD'] == 'ECE15+EUDC'].merge(ec_exno, how='inner')
        wltc_f = cert[cert['INSP_MSRMT_MTHD'] == 'WLTC'].merge(wltc_exno, how='inner')

        cert_f = pd.concat([cvs75_f, ec_f, wltc_f])
        cert_f.reset_index(drop=True, inplace=True)

        # 배출가스 SI별 구간 구분
        def unit_2(g, data):
            data_s = (data[g] >= 0) & (data[g] < 0.2)
            data.loc[data_s, g + '_SCOP'] = 'ABOVE_ZERO_UD_TWOTS'
            data_s = (data[g] >= 0.2) & (data[g] < 0.4)
            data.loc[data_s, g + '_SCOP'] = 'ABOVE_TWOTS_UD_FOURTS'
            data_s = (data[g] >= 0.4) & (data[g] < 0.6)
            data.loc[data_s, g + '_SCOP'] = 'ABOVE_FOURTS_UD_SIXTS'
            data_s = (data[g] >= 0.6) & (data[g] < 0.8)
            data.loc[data_s, g + '_SCOP'] = 'ABOVE_SIXTS_UD_EIGHTTS'
            data_s = (data[g] >= 0.8) & (data[g] <= 1)
            data.loc[data_s, g + '_SCOP'] = 'ABOVE_EIGHTTS_UD_ONE'

        unit_2('CO_SI', cert_f)
        unit_2('HC_SI', cert_f)
        unit_2('NMOG_NOX_SI', cert_f)
        unit_2('SMOKE_SI', cert_f)

        # 배출가스 인증번호별 구간의 개수 산출
        cert_co = cert_f.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM',
                                  'CO_SI_SCOP']).size().unstack().reset_index()
        cert_co = cert_co.merge(cert_f.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM'])[
                                    'CO_SI'].mean().reset_index(name='AVRG_EXHST_GAS_SI'), how='left')
        cert_co['EXHST_GAS'] = 'CO'

        cert_hc = cert_f.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM',
                                  'HC_SI_SCOP']).size().unstack().reset_index()
        cert_hc = cert_hc.merge(cert_f.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM'])[
                                    'HC_SI'].mean().reset_index(name='AVRG_EXHST_GAS_SI'), how='left')
        cert_hc['EXHST_GAS'] = 'HC'

        cert_nox = cert_f.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM',
                                   'NMOG_NOX_SI_SCOP']).size().unstack().reset_index()
        cert_nox = cert_nox.merge(cert_f.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM'])[
                                      'NMOG_NOX_SI'].mean().reset_index(name='AVRG_EXHST_GAS_SI'), how='left')
        cert_nox['EXHST_GAS'] = 'NOX'

        cert_smoke = cert_f.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM',
                                     'SMOKE_SI_SCOP']).size().unstack().reset_index()
        cert_smoke = cert_smoke.merge(cert_f.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM'])[
                                       'SMOKE_SI'].mean().reset_index(name='AVRG_EXHST_GAS_SI'), how='left')
        cert_smoke['EXHST_GAS'] = 'SMOKE'

        cert_g = pd.concat([cert_co, cert_hc, cert_nox, cert_smoke])
        cert_g['DAT_SE'] = 'CERT_TEST_DAT'

        # 검사이력 데이터와 인증시험 데이터 모두 존재하는 배출가스 인증번호의 검사 이력데이터만 추출
        insp_cvs = insp_f[
            (insp_f['INSP_MSRMT_MTHD'].isin(['부하검사(ASM-Idling)', '부하검사(ASM2525)'])) & (insp_f['FUEL_UP_CD_NM'].isin(['휘발유', '가스']))].merge(
            cert_f[cert_f['INSP_MSRMT_MTHD'] == 'CVS-75'].loc[:, ['EXHST_GAS_CERT_NO']], how='inner')
        insp_cvs['INSP_MSRMT_MTHD'] = '부하검사(AMS-Idling), 부하검사(ASM2525)'

        insp_ec = insp_f[(insp_f['INSP_MSRMT_MTHD'] == '부하검사(LUG DOWN)') & (insp_f['FUEL_UP_CD_NM'] == '경유') & (
                    insp_f['VHCL_FBCTN_YMD'] >= '20130101') & (insp_f['VHCL_FBCTN_YMD'] <= '20170930')].merge(
            cert_f[cert_f['INSP_MSRMT_MTHD'] == 'ECE15+EUDC'].loc[:, ['EXHST_GAS_CERT_NO']], how='inner')

        insp_wltc = insp_f[(insp_f['INSP_MSRMT_MTHD'] == '부하검사(KD-147)') & (insp_f['FUEL_UP_CD_NM'] == '경유') & (
                    insp_f['VHCL_FBCTN_YMD'] >= '20171001')].merge(
            cert_f[cert_f['INSP_MSRMT_MTHD'] == 'WLTC'].loc[:, ['EXHST_GAS_CERT_NO']], how='inner')

        ic = pd.concat([insp_cvs, insp_ec, insp_wltc])

        # 배출가스 SI별 구간 구분
        unit_2('CO_SI', ic)
        unit_2('HC_SI', ic)
        unit_2('NOX_SI', ic)
        unit_2('SMOKE_SI', ic)

        # 배출가스 인증번호별 구간의 개수 산출
        ic_co = ic.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM',
                            'CO_SI_SCOP']).size().unstack().reset_index()
        ic_co = ic_co.merge(
            ic.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM'])['CO_SI'].mean().reset_index(
                name='AVRG_EXHST_GAS_SI'), how='left')
        ic_co['EXHST_GAS'] = 'CO'

        ic_hc = ic.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM',
                            'HC_SI_SCOP']).size().unstack().reset_index()
        ic_hc = ic_hc.merge(
            ic.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM'])['HC_SI'].mean().reset_index(
                name='AVRG_EXHST_GAS_SI'), how='left')
        ic_hc['EXHST_GAS'] = 'HC'

        ic_nox = ic.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM',
                             'NOX_SI_SCOP']).size().unstack().reset_index()
        ic_nox = ic_nox.merge(
            ic.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM'])['NOX_SI'].mean().reset_index(
                name='AVRG_EXHST_GAS_SI'), how='left')
        ic_nox['EXHST_GAS'] = 'NOX'

        ic_smoke = ic.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM',
                               'SMOKE_SI_SCOP']).size().unstack().reset_index()
        ic_smoke = ic_smoke.merge(ic.groupby(['EXHST_GAS_CERT_NO', 'INSP_MSRMT_MTHD', 'RPRS_MKR', 'RPRS_VHCNM'])[
                                      'SMOKE_SI'].mean().reset_index(name='AVRG_EXHST_GAS_SI'), how='left')
        ic_smoke['EXHST_GAS'] = 'SMOKE'

        ic_g = pd.concat([ic_co, ic_hc, ic_nox, ic_smoke])
        ic_g['DAT_SE'] = 'INSP_DAT'

        ic_f = pd.concat([ic_g, cert_g])

        # 데이터프레임 틀 생성
        fr = ic_f.drop_duplicates(['EXHST_GAS_CERT_NO']).loc[:,
             ['EXHST_GAS_CERT_NO', 'RPRS_MKR', 'RPRS_VHCNM']].reset_index(drop=True)

        def fr_m(gas, ds):
            data = fr.copy()
            data['EXHST_GAS'] = gas
            data['DAT_SE'] = ds
            return data

        fr_f = pd.concat(
            [fr_m('CO', 'INSP_DAT'), fr_m('HC', 'INSP_DAT'), fr_m('NOX', 'INSP_DAT'), fr_m('SMOKE', 'INSP_DAT'),
             fr_m('CO', 'CERT_TEST_DAT'), fr_m('HC', 'CERT_TEST_DAT'), fr_m('NOX', 'CERT_TEST_DAT'),
             fr_m('SMOKE', 'CERT_TEST_DAT'), ])

        ic_f = fr_f.merge(ic_f, how='left')
        
        # 테이블 생성 연월일
        ic_f['LOAD_DT'] = datetime.date.today()

        
        table_nm = 'std_bd_insp_cert_test_htmap'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(ic_f.columns):
            if 'float' in ic_f[column].dtype.name:
                sql += column+' float'
            elif 'int' in ic_f[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(ic_f.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)
        
        # 데이터프레임을 DB에 적재
        self.dbUtil2.import_from_pandas(ic_f, table_nm)

        # 1438sec (24min)




    def proj03_analysis_02(self):
        """
        인증시험 분석-02(구현 분석가: 김강민)
        :return:
        """

        # STD_BD_CAR_INSP_HST 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             select
        #                 EXHST_GAS_CERT_NO, INSP_MSRMT_MTHD, CO_1_ITM_MEVLU, NMOG_VAL, NOX_MEVLU, PM_VAL
        #             from
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 INSP_MSRMT_MTHD IN ('CVS-75', 'WLTC')
        #            """
        # INSP = self.dbUtil.export_to_pandas(sql_stmt)

        INSP = self.INSP[(self.INSP['INSP_MSRMT_MTHD'].isin(['CVS-75', 'WLTC']))].copy()



        # EXHST_GAS_CERT_NO 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT
        #                 EXHST_GAS_CERT_NO, CERT_YR
        #             FROM
        #                 "vsysb".STD_BD_EXHST_GAS_CERT_NO_DAT
        #             WHERE
        #                 EXHST_GAS_CERT_NO IS NOT NULL
        #             """
        #
        # EXNO = self.dbUtil.export_to_pandas(sql_stmt)
        EXNO = self.EXHST_GAS_CERT[self.EXHST_GAS_CERT['EXHST_GAS_CERT_NO'].notnull()].copy()

        # EXHST_GAS_CERT_NO 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT
        #                 EXHST_GAS_CERT_NO, VHCL_FBCTN_YMD
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             WHERE
        #                 EXHST_GAS_CERT_NO IS NOT NULL
        #             """
        #
        # CAR = self.dbUtil.export_to_pandas(sql_stmt)
        CAR = self.CAR[self.CAR['EXHST_GAS_CERT_NO'].notnull()]

        CAR['VHCL_FBCTN_YR'] = CAR['VHCL_FBCTN_YMD'].dt.year

        car = CAR.groupby(['EXHST_GAS_CERT_NO', 'VHCL_FBCTN_YR']).size().reset_index().drop_duplicates(
            ['EXHST_GAS_CERT_NO'], keep='first').drop(columns=0)

        insp = INSP.merge(EXNO, how='left', on = ['EXHST_GAS_CERT_NO'])

        insp = insp.merge(car, how='left', on = ['EXHST_GAS_CERT_NO'])

        # 배출가스 인증번호의 첫 글자 추출
        insp['EXHST_GAS_CERT_NO_FW'] = insp['EXHST_GAS_CERT_NO'].str[0]

        # 배출가스 인증번호의 첫 글자와 제작일자를 통해서 인증연도 추측
        insp['AS'] = insp['EXHST_GAS_CERT_NO_FW'].apply(lambda x: ord(x))

        insp_s = (insp['EXHST_GAS_CERT_NO_FW'] >= 'J') & (insp['EXHST_GAS_CERT_NO_FW'] <= 'N') & (
            insp['CERT_YR'].isnull())
        insp.loc[insp_s, 'CERT_YR'] = (insp['AS'] - ord('J') + 1868) + self.roundTraditional(
            (abs((insp['AS'] - ord('J') + 1868) - insp['VHCL_FBCTN_YR']) / 30), 0) * 30

        insp_s = (insp['EXHST_GAS_CERT_NO_FW'] == 'P') & (insp['CERT_YR'].isnull())
        insp.loc[insp_s, 'CERT_YR'] = (insp['AS'] - ord('P') + 1873) + self.roundTraditional(
            (abs((insp['AS'] - ord('P') + 1873) - insp['VHCL_FBCTN_YR']) / 30), 0) * 30

        insp_s = (insp['EXHST_GAS_CERT_NO_FW'] >= 'R') & (insp['EXHST_GAS_CERT_NO_FW'] <= 'T') & (
            insp['CERT_YR'].isnull())
        insp.loc[insp_s, 'CERT_YR'] = (insp['AS'] - ord('R') + 1874) + self.roundTraditional(
            (abs((insp['AS'] - ord('R') + 1874) - insp['VHCL_FBCTN_YR']) / 30), 0) * 30

        insp_s = (insp['EXHST_GAS_CERT_NO_FW'] >= 'V') & (insp['EXHST_GAS_CERT_NO_FW'] <= 'Y') & (
            insp['CERT_YR'].isnull())
        insp.loc[insp_s, 'CERT_YR'] = (insp['AS'] - ord('V') + 1877) + self.roundTraditional(
            (abs((insp['AS'] - ord('V') + 1877) - insp['VHCL_FBCTN_YR']) / 30), 0) * 30

        insp_s = (insp['EXHST_GAS_CERT_NO_FW'] >= 'A') & (insp['EXHST_GAS_CERT_NO_FW'] <= 'H') & (
            insp['CERT_YR'].isnull())
        insp.loc[insp_s, 'CERT_YR'] = (insp['AS'] - ord('A') + 1890) + self.roundTraditional(
            (abs((insp['AS'] - ord('A') + 1890) - insp['VHCL_FBCTN_YR']) / 30), 0) * 30

        insp_s = (insp['EXHST_GAS_CERT_NO_FW'] >= '1') & (insp['EXHST_GAS_CERT_NO_FW'] <= '9') & (
            insp['CERT_YR'].isnull())
        insp.loc[insp_s, 'CERT_YR'] = (insp['AS'] - ord('1') + 1881) + self.roundTraditional(
            (abs((insp['AS'] - ord('1') + 1881) - insp['VHCL_FBCTN_YR']) / 30), 0) * 30

        insp_s = insp['CERT_YR'] > datetime.date.today().year
        
        # 22.10.17 수정
        insp.loc[insp_s, 'CERT_YR'] = insp['CERT_YR'] - 30
        # 22.10.17 수정


        # 검사방법및 인증연도별 집계
        t = insp.groupby(['INSP_MSRMT_MTHD', 'CERT_YR']).agg(AVRG_CO_MEVLU=('CO_1_ITM_MEVLU', 'mean'),
                                                             CO_CNT=('CO_1_ITM_MEVLU', 'count'),
                                                             AVRG_HC_MEVLU=('NMOG_VAL', 'mean'),
                                                             HC_CNT=('NMOG_VAL', 'count'),
                                                             AVRG_NOX_MEVLU=('NOX_MEVLU', 'mean'),
                                                             NOX_CNT=('NOX_MEVLU', 'count'),
                                                             AVRG_SMOKE_MEVLU=('PM_VAL', 'mean'),
                                                             SMOKE_CNT=('PM_VAL', 'count'), ).reset_index()
        
        # 테이블 생성 연월일
        t['LOAD_DT'] = datetime.date.today()
        
        
        table_nm = 'std_bd_cert_test_cert_yr'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(t.columns):
            if 'float' in t[column].dtype.name:
                sql += column+' float'
            elif 'int' in t[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(t.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)
        
        # 데이터프레임을 DB에 적재
        self.dbUtil2.import_from_pandas(t, table_nm)
        # 1188sec (20min)



    def proj04_analysis_01(self):
        """
        운행제한 분석-1(구현 분석가: 김강민)
        :return:
        """

        # STD_BD_RUN_LMT_HST 테이블 중 필요 컬럼만 추출
        sql_stmt = """
                    SELECT
                        *
                    FROM 
                        "vsysb".STD_BD_RUN_LMT_HST
                    """

        LEZ = self.dbUtil.export_to_pandas(sql_stmt)
        LEZ = dbUtil.export_to_pandas(sql_stmt)
        LEZ['CRDN_RGN_CD'] = pd.to_numeric(LEZ['CRDN_RGN_CD'], errors='coerce')
        LEZ['DSCL_DT'] = LEZ['DSCL_DT'].astype(np.int64)
        LEZ['DSCL_DT'] = pd.to_datetime(LEZ['DSCL_DT'], format='%Y%m%d%H%M%S', errors = 'coerce')

        # STD_BD_CAR 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT
        #                 VIN, VHRNO, BSPL_STDG_CD, VHCTY_CD_NM
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             WHERE
        #                 VHCL_ERSR_YN = 'N'
        #             """
        #
        # CAR = dbUtil.export_to_pandas(sql_stmt)
        CAR = self.CAR[self.CAR['VHCL_ERSR_YN'] == 'N'].copy()

        # 법정동 코드 추가

        # STD_BJCD_INFO 테이블
        sql_stmt = """
                    SELECT
                        *
                    FROM 
                        "vsysd".STD_BJCD_INFO
                    WHERE
                        ABL_YN = '존재'
                    """

        cd = self.dbUtil.export_to_pandas(sql_stmt)

        # 법정동 시군구명이 5글자 이상인 경우 ' '으로 구분한 뒤 첫 단어만 선택
        cd_o5 = cd['STDG_SGG_NM'].str.len() >= 5
        cd.loc[cd_o5, 'STDG_SGG_NM'] = cd['STDG_SGG_NM'].str.split(' ', 1, expand=True)[0]

        # 등록지명이 시로 끝나지 않을 경우 시, 군 혹은 면까지 표현
        cd_r = cd['STDG_CTPV_NM'].str[-1:] == '시'
        cd.loc[cd_r, 'REG_RGN_NM'] = cd['STDG_CTPV_NM']
        cd_r = cd['STDG_CTPV_NM'].str[-1:] != '시'
        cd.loc[cd_r, 'REG_RGN_NM'] = cd['STDG_CTPV_NM'] + ' ' + cd['STDG_SGG_NM']

        cd['REG_RGN_CD'] = cd['STDG_CTPV_CD'] * 1000 + cd['STDG_SGG_CD']
        cd['REG_RGN_CD'] = pd.to_numeric(cd['REG_RGN_CD'], errors='coerce')

        cdf = cd.groupby(['REG_RGN_CD', 'REG_RGN_NM']).size().reset_index().loc[:, ['REG_RGN_CD', 'REG_RGN_NM']]

        # 법정동 코드 시군구코드로 추출
        CAR['REG_RGN_CD'] = CAR['BSPL_STDG_CD'] // 100000

        car = CAR.merge(cdf, how='left', on = ['REG_RGN_CD'])

        # 2020/01/01 이후 데이터만 사용
        LEZ = LEZ[LEZ['DSCL_DT'] >= '20200101']
        
        # 적발 연월일 열 생성
        LEZ['DSCL_YMD'] = LEZ['DSCL_DT'].dt.strftime('%Y-%m-%d')

        test = LEZ.merge(cdf, how='left', left_on='CRDN_RGN_CD', right_on='REG_RGN_CD')

        test.rename(columns={'REG_RGN_NM': 'CRDN_RGN_NM'}, inplace=True)

        test.drop(columns='REG_RGN_CD', inplace=True)

        test = test.merge(car, how='left', on = ['VIN', 'VHRNO'])

        # 적발 시간 추출
        test['DSCL_HMS'] = test['DSCL_DT'].dt.strftime('%H:%M:%S')

        # 적발 시간 구분
        test['DSCL_HUR_SE'] = '06~11'

        test_1116 = (test['DSCL_HMS'] > '11:00:00') & (test['DSCL_HMS'] <= '16:00:00')

        test.loc[test_1116, 'DSCL_HUR_SE'] = '11~16'

        test_1621 = (test['DSCL_HMS'] > '16:00:00') & (test['DSCL_HMS'] <= '21:00:00')

        test.loc[test_1621, 'DSCL_HUR_SE'] = '16~21'

        test_2106 = (test['DSCL_HMS'] > '21:00:00') | (test['DSCL_HMS'] <= '06:00:00')

        test.loc[test_2106, 'DSCL_HUR_SE'] = '21~06'

        # 그룹별 한 번만 걸린 차량 선별
        test.drop_duplicates(['VIN', 'VHRNO', 'REG_RGN_NM', 'CRDN_RGN_NM', 'DSCL_YMD', 'DSCL_HUR_SE', 'VHCTY_CD_NM'],
                             inplace=True)

        test_f = test.groupby(
            ['REG_RGN_NM', 'CRDN_RGN_NM', 'DSCL_YMD', 'DSCL_HUR_SE', 'VHCTY_CD_NM']).size().reset_index(
            name='RUN_LMT_VHCL_CNT')
        
        
        test_f['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_run_lmt_vhcl_cnt'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(test_f.columns):
            if 'float' in test_f[column].dtype.name:
                sql += column+' float'
            elif 'int' in test_f[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(test_f.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)
        
        # 데이터프레임을 DB에 적재
        # 테이블 생성 연월일
        self.dbUtil2.import_from_pandas(test_f, table_nm)

        # 총 차량 수, 적발 차량 수, 미적발 차량 수 데이터프레임 생성
        df = pd.DataFrame([[len(car), len(
            test[(test['VIN'].notnull()) & (test['VHRNO'].notnull()) & (test['REG_RGN_NM'].notnull())].drop_duplicates(
                ['VIN', 'VHRNO'])),
                            len(car) - len(test[(test['VIN'].notnull()) & (test['VHRNO'].notnull()) & (
                                test['REG_RGN_NM'].notnull())].drop_duplicates(['VIN', 'VHRNO']))]],
                          columns=['ALL_RUN_VHCL_CNT', 'OBS_VHCL_CNT', 'UN_OBS_VHCL_CNT'])
        
        # 테이블 생성 연월일
        df['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_run_vhcl_cnt'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(df.columns):
            if 'float' in df[column].dtype.name:
                sql += column+' float'
            elif 'int' in df[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(df.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)
        
        self.dbUtil2.import_from_pandas(df, table_nm)

        # 2032sec (34min)



    def proj04_analysis_02(self):
        """
        등급별 측정치 총합(구현 분석가: 김강민)
        :return:
        """

        # STD_BD_CAR_INSP_HST 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT /*+ PARALLEL(2) */
        #                 VIN, VHRNO, INSP_YMD, CO_1_ITM_MEVLU, HC_1_ITM_MEVLU, NOX_MEVLU, SMO_1_ITM_MEVLU, INSP_SE, INSP_PASAGE_YN
        #             FROM
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 (INSP_SE = '정기검사' OR INSP_SE = '정밀검사' OR INSP_SE = 'RSD수시점검')
        #                 AND INSP_PASAGE_YN IS NOT NULL
        #             """
        #
        # INSP = self.dbUtil.export_to_pandas(sql_stmt)

        INSP = self.INSP[(self.INSP['INSP_SE'].isin(['정기검사', '정밀검사', 'RSD수시점검'])) & (self.INSP['INSP_PASAGE_YN'].notnull())].copy()


        # STD_BD_CAR 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT /*+ PARALLEL(2) */
        #                 VIN, VHRNO, EXHST_GAS_GRD_CD_NM, STDG_CTPV_NM
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             WHERE
        #                 VHCL_ERSR_YN = 'N'
        #             """
        #
        # CAR = self.dbUtil.export_to_pandas(sql_stmt)
        CAR = self.CAR[self.CAR['VHCL_ERSR_YN'] == 'N'].copy()


        # 배출가스 등급 통일화
        CAR['EXHST_GAS_GRD_CD_NM'] = CAR['EXHST_GAS_GRD_CD_NM'].apply(lambda x: 'X' if x == 'X'
        else ('1' if (x == '1' or x == 'T1')
              else ('2' if (x == '2' or x == 'T2')
                    else ('3' if (x == '3' or x == 'T3')
                          else ('4' if (x == '4' or x == 'T4')
                                else ('5' if (x == '5' or x == 'T5')
                                      else (x)))))))

        # 배출가스 등급 및 시도명
        insp = INSP.merge(CAR, how='left', on=['VIN', 'VHRNO'])

        # 검사이력의 년, 월 컬럼 생성
        insp['INSP_Y'] = insp['INSP_YMD'].dt.year
        insp['INSP_M'] = insp['INSP_YMD'].dt.month

        # RSD수시점검의 등급별 측정치
        insp_r = insp[insp['INSP_SE'] == 'RSD수시점검'].groupby(
            ['STDG_CTPV_NM', 'INSP_Y', 'INSP_M', 'EXHST_GAS_GRD_CD_NM']).agg(
            CO_INSP_CNT=('CO_1_ITM_MEVLU', 'count'), HC_INSP_CNT=('HC_1_ITM_MEVLU', 'count'),
            NOX_INSP_CNT=('NOX_MEVLU', 'count'), SMOKE_INSP_CNT=('SMO_1_ITM_MEVLU', 'count'),
            CO_TOTL_MEVLU=('CO_1_ITM_MEVLU', 'sum'), HC_TOTL_MEVLU=('HC_1_ITM_MEVLU', 'sum'),
            NOX_TOTL_MEVLU=('NOX_MEVLU', 'sum'), SMOKE_TOTL_MEVLU=('SMO_1_ITM_MEVLU', 'sum'),
            TOTL_INSP_CNT=('INSP_PASAGE_YN', 'count')).reset_index()

        insp_r = insp_r.merge(insp[(insp['INSP_SE'] == 'RSD수시점검') & (insp['INSP_PASAGE_YN'] == 'N')].groupby(
            ['STDG_CTPV_NM', 'INSP_Y', 'INSP_M', 'EXHST_GAS_GRD_CD_NM']).agg(
            N_PASAGE_INSP_CNT=('INSP_PASAGE_YN', 'count')).reset_index(), how='left')

        insp_r.loc[insp_r['N_PASAGE_INSP_CNT'] != insp_r['N_PASAGE_INSP_CNT'], 'N_PASAGE_INSP_CNT'] = 0

        # 정기검사, 정밀검사의 등급별 측정치
        insp_j = insp[insp['INSP_SE'] != 'RSD수시점검'].groupby(
            ['STDG_CTPV_NM', 'INSP_Y', 'INSP_M', 'EXHST_GAS_GRD_CD_NM']).agg(
            CO_INSP_CNT=('CO_1_ITM_MEVLU', 'count'), HC_INSP_CNT=('HC_1_ITM_MEVLU', 'count'),
            NOX_INSP_CNT=('NOX_MEVLU', 'count'), SMOKE_INSP_CNT=('SMO_1_ITM_MEVLU', 'count'),
            CO_TOTL_MEVLU=('CO_1_ITM_MEVLU', 'sum'), HC_TOTL_MEVLU=('HC_1_ITM_MEVLU', 'sum'),
            NOX_TOTL_MEVLU=('NOX_MEVLU', 'sum'), SMOKE_TOTL_MEVLU=('SMO_1_ITM_MEVLU', 'sum'),
            TOTL_INSP_CNT=('INSP_PASAGE_YN', 'count')).reset_index()

        insp_j = insp_j.merge(insp[(insp['INSP_SE'] != 'RSD수시점검') & (insp['INSP_PASAGE_YN'] == 'N')].groupby(
            ['STDG_CTPV_NM', 'INSP_Y', 'INSP_M', 'EXHST_GAS_GRD_CD_NM']).agg(
            N_PASAGE_INSP_CNT=('INSP_PASAGE_YN', 'count')).reset_index(), how='left')

        insp_j.loc[insp_j['N_PASAGE_INSP_CNT'] != insp_j['N_PASAGE_INSP_CNT'], 'N_PASAGE_INSP_CNT'] = 0
        
        # 테이블 생성 연월일
        insp_r['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_rsd_grd_mevlu'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(insp_r.columns):
            if 'float' in insp_r[column].dtype.name:
                sql += column+' float'
            elif 'int' in insp_r[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(insp_r.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(insp_r, table_nm)
        
        # 테이블 생성 연월일
        insp_j['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_fdrm_precisn_grd_mevlu'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(insp_j.columns):
            if 'float' in insp_j[column].dtype.name:
                sql += column+' float'
            elif 'int' in insp_j[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(insp_j.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(insp_j, table_nm)
        # 2208sec (37min)

    def proj04_analysis_03(self):
        """
        총량분석-01(구현 분석가: 김강민)
        :return:
        """

        # STD_BD_CAR_INSP_HST 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT
        #                 VIN, VHRNO, INSP_YMD, DRVNG_DSTNC, INSP_SE, EQMT_CD_NM, VSP, CO_CO2_VAL, HC_CO2_VAL, NO_CO2_VAL, CO_1_ITM_MEVLU, HC_1_ITM_MEVLU, NOX_MEVLU, SMO_1_ITM_MEVLU
        #             FROM
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 ((INSP_SE = '정기검사' OR INSP_SE = '정밀검사') AND INSP_PASAGE_YN = 'Y' AND DRVNG_DSTNC IS NOT NULL)
        #                 OR INSP_SE = 'RSD수시점검'
        #             """
        #
        # INSP = self.dbUtil.export_to_pandas(sql_stmt)

        INSP = self.INSP[((self.INSP['INSP_SE'].isin(['정기검사', '정밀검사'])) & (self.INSP['INSP_PASAGE_YN'] == 'Y') & (self.INSP['DRVNG_DSTNC'].notnull())) 
                           | (self.INSP['INSP_SE'] == 'RSD수시점검')].copy()


        # STD_BD_CAR 테이블 중 필요 컬럼만 추출
        # sql_stmt = """
        #             SELECT
        #                 VIN, VHRNO, EXHST_GAS_CERT_NO, EXHST_GAS_GRD_CD_NM, BSPL_STDG_CD, FUEL_UP_CD_NM, VHCTY_CD_NM, VHCL_FBCTN_YMD
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             WHERE
        #                 VHCL_ERSR_YN = 'N'
        #             """
        #
        # CAR = self.dbUtil.export_to_pandas(sql_stmt)
        CAR = self.CAR[self.CAR['VHCL_ERSR_YN'] == 'N'].copy()


        # 제원관리 테이블

        # 제작사별 제원관리 필터링
        # gt1 = pd.read_excel('(주) 에프엠케이_제원관리.xlsx')
        # gt2 = pd.read_excel('(주) 참존임포트_제원관리.xlsx')
        # gt3 = pd.read_excel('㈜볼보자동차코리아_제원관리.xlsx')
        # gt4 = pd.read_excel('(주)재규어랜드로버코리아_제원관리.xlsx')
        # gt5 = pd.read_excel('기아자동차(주)_제원관리.xlsx')
        # gt6 = pd.read_excel('르노삼성자동차(주)_제원관리.xlsx')
        # gt7 = pd.read_excel('메르세데스-벤츠코리아(주)_제원관리.xlsx')
        # gt8 = pd.read_excel('스바루코리아(주)_제원관리.xlsx')
        # gt9 = pd.read_excel('쌍용자동차㈜_제원관리.xlsx')
        # gt10 = pd.read_excel('씨엑스씨 모터스_제원관리.xlsx')
        # gt11 = pd.read_excel('아우디폭스바겐코리아(주)_제원관리.xlsx')
        # gt12 = pd.read_excel('에프씨에이코리아 주식회사_제원관리.xlsx')
        # gt13 = pd.read_excel('포드세일즈서비스코리아(유)_제원관리.xlsx')
        # gt14 = pd.read_excel('포르쉐코리아 주식회사_제원관리.xlsx')
        # gt15 = pd.read_excel('한국닛산(주)_제원관리.xlsx')
        # gt16 = pd.read_excel('한국지엠(주)_제원관리.xlsx')
        # gt17 = pd.read_excel('한국토요타자동차(주)_제원관리.xlsx')
        # gt18 = pd.read_excel('한불모터스(주)_제원관리.xlsx')
        # gt19 = pd.read_excel('현대자동차(주)_제원관리.xlsx')
        # gt20 = pd.read_excel('혼다코리아(주)_제원관리.xlsx')
        # gt21 = pd.read_excel('BMW코리아(주)_제원관리.xlsx')
        # gt22 = pd.read_excel('GM코리아㈜_제원관리.xlsx')

        # gt = pd.concat([gt1, gt2, gt3, gt4, gt5, gt6, gt7, gt8, gt9, gt10,
        #           gt11, gt12, gt13, gt14, gt15, gt16, gt17, gt18, gt19, gt20,
        #           gt21, gt22])

        # gt.rename(columns = {'배출가스\n인증번호' : 'EXHST_GAS_CERT_NO', '제작사' : 'MKR', '사용연료': 'FUEL_UP_CD_NM'}, inplace = True)

        # gt['EXHST_GAS_CERT_NO'] = gt[['EXHST_GAS_CERT_NO']].fillna('X')

        # gtt1 = gt.groupby(['EXHST_GAS_CERT_NO']).size().sort_values().reset_index(name = 'EMIS_CRTCNO_ACTO_CNT')

        # gtt4 = gt.groupby(['EXHST_GAS_CERT_NO', 'FUEL_UP_CD_NM', 'MKR']).size().sort_values().reset_index(name = 'EMIS_CRTCNO_FUEL_ACTO_CNT')

        # gtt5 = pd.merge(gtt4, gtt1, how = 'left')

        # 같은 배출가스 인증번호임에도 연료가 다른 경우 제거
        # gt_e = gtt5[gtt5['EMIS_CRTCNO_FUEL_ACTO_CNT'] == gtt5['EMIS_CRTCNO_ACTO_CNT']].loc[:, ['EXHST_GAS_CERT_NO']]

        # gt_f = pd.merge(gt, gt_e, how = 'inner')

        # 배출가스 인증번호 별 'CVS-75' 평균 계산
        # gt_fm = gt_f.groupby(['EXHST_GAS_CERT_NO'])['CVS-75'].mean().reset_index(name = 'AVRG_CVS_75')

        # gt_fm.to_csv('STD_BD_MNFCTR_ACTO_MANP_MNG.csv', index = False)

        # STD_BD_MNFCTR_ACTO_MANP_MNG 테이블
        sql_stmt = """
                    SELECT
                        *
                    FROM 
                        "vsysd".STD_BD_MNFCTR_ACTO_MANP_MNG
                    """

        gt_fm = self.dbUtil.export_to_pandas(sql_stmt)

        # 법정동 코드 추가

        # STD_BJCD_INFO 테이블
        sql_stmt = """
                    SELECT
                        *
                    FROM 
                        "vsysd".STD_BJCD_INFO
                    WHERE
                        ABL_YN = '존재'
                    """

        cd = self.dbUtil.export_to_pandas(sql_stmt)

        # 시군구 명이 None인 경우 공백으로 대체
        cd['STDG_SGG_NM'] = cd['STDG_SGG_NM'].apply(lambda x: '' if (x is None or x is np.nan)
        else x)

        cd['STDG_CTSGG_CD'] = cd['STDG_CTPV_CD']*1000 + cd['STDG_SGG_CD']
        cd['STDG_CTSGG_NM'] = cd['STDG_CTPV_NM'] + ' ' + cd['STDG_SGG_NM']

        cd['STDG_CTSGG_CD'] = cd['STDG_CTSGG_CD'].astype(np.float64)

        cdf = cd.groupby(['STDG_CTSGG_CD', 'STDG_CTSGG_NM']).size().reset_index().loc[:,
              ['STDG_CTSGG_CD', 'STDG_CTSGG_NM']]
        cdf.rename(columns={'STDG_CTSGG_CD': 'STDG_SGG_CD', 'STDG_CTSGG_NM': 'STDG_SGG_NM'}, inplace=True)
        cdf.drop_duplicates(['STDG_SGG_CD'], keep='first', inplace=True)

        # 법정동 코드 시군구코드로 추출
        CAR['STDG_SGG_CD'] = CAR['BSPL_STDG_CD'] // 100000

        car = CAR.merge(cdf, how='left', on = ['STDG_SGG_CD'])

        # 유로 구분
        euro = (car['VHCL_FBCTN_YMD'] >= '20000101') & (car['VHCL_FBCTN_YMD'] <= '20041231') & (
                car['FUEL_UP_CD_NM'] == '휘발유')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO3', 0.13]

        euro = (car['VHCL_FBCTN_YMD'] >= '20000101') & (car['VHCL_FBCTN_YMD'] <= '20041231') & (
                car['FUEL_UP_CD_NM'] == '가스')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO3', 0.13]

        euro = (car['VHCL_FBCTN_YMD'] >= '20000101') & (car['VHCL_FBCTN_YMD'] <= '20041231') & (
                car['FUEL_UP_CD_NM'] == '경유')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO3', 0.15]

        euro = (car['VHCL_FBCTN_YMD'] >= '20050101') & (car['VHCL_FBCTN_YMD'] <= '20090831') & (
                car['FUEL_UP_CD_NM'] == '휘발유')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO4', 0.22]

        euro = (car['VHCL_FBCTN_YMD'] >= '20050101') & (car['VHCL_FBCTN_YMD'] <= '20090831') & (
                car['FUEL_UP_CD_NM'] == '가스')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO4', 0.22]

        euro = (car['VHCL_FBCTN_YMD'] >= '20050101') & (car['VHCL_FBCTN_YMD'] <= '20090831') & (
                car['FUEL_UP_CD_NM'] == '경유')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO4', 0.22]

        euro = (car['VHCL_FBCTN_YMD'] >= '20090101') & (car['VHCL_FBCTN_YMD'] <= '20140831') & (
                car['FUEL_UP_CD_NM'] == '휘발유')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO5', 0.26]

        euro = (car['VHCL_FBCTN_YMD'] >= '20090101') & (car['VHCL_FBCTN_YMD'] <= '20140831') & (
                car['FUEL_UP_CD_NM'] == '가스')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO5', 0.26]

        euro = (car['VHCL_FBCTN_YMD'] >= '20090101') & (car['VHCL_FBCTN_YMD'] <= '20140831') & (
                car['FUEL_UP_CD_NM'] == '경유')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO5', 0.30]

        euro = (car['VHCL_FBCTN_YMD'] >= '20140901') & (car['FUEL_UP_CD_NM'] == '휘발유')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO6', 0.33]

        euro = (car['VHCL_FBCTN_YMD'] >= '20140901') & (car['FUEL_UP_CD_NM'] == '가스')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO6', 0.33]

        euro = (car['VHCL_FBCTN_YMD'] >= '20140901') & (car['FUEL_UP_CD_NM'] == '경유')
        car.loc[euro, ['EURO_SE', 'CO2_GAP']] = ['EURO6', 0.39]

        # 배출가스 등급 통일화
        car['EXHST_GAS_GRD_CD_NM'] = car['EXHST_GAS_GRD_CD_NM'].apply(lambda x: 'X' if x == 'X'
        else ('1' if (x == '1' or x == 'T1')
              else ('2' if (x == '2' or x == 'T2')
                    else ('3' if (x == '3' or x == 'T3')
                          else ('4' if (x == '4' or x == 'T4')
                                else ('5' if (x == '5' or x == 'T5')
                                      else (x)))))))

        # 총량 계산

        # 검사이력 데이터와 자동차 데이터 차량번호, 차대번호 기준으로 조인
        # 22.10.24 수정
        INSP.drop(columns = 'EXHST_GAS_CERT_NO', inplace = True)
        # 22.10.24 수정
        insp = INSP.merge(car, how='left', on=['VIN', 'VHRNO'])

        # """검사이력 데이터와 제조사별 제원관리 배출가스 인증번호 기준으로 조인"""
        insp = insp.merge(gt_fm, how='left', on='EXHST_GAS_CERT_NO')

        # 검사이력 데이터에서 정기검사, 정밀검사만 추출
        insp_j = insp[insp['INSP_SE'].isin(['정밀검사', '정기검사'])]

        # 검사이력 데이터 중에서 CVS_75데이터가 없는거 제외
        insp = insp[insp['AVRG_CVS_75'].notnull()]

        # 배출가스 / CO2 설정
        insp['CO_CO2'] = insp['CO_CO2_VAL'].apply(lambda x: np.nan if x is None
        else x)
        insp['HC_CO2'] = insp['HC_CO2_VAL'].apply(lambda x: np.nan if x is None
        else x)
        insp['NO_CO2'] = insp['NO_CO2_VAL'].apply(lambda x: np.nan if x is None
        else x)

        # 배출가스 / CO2 가 음수일 경우 제거 및 HC, NO의 단위가 ppm이므로 %로 변환
        # insp['CO_CO2'] = insp['CO_CO2'].apply(lambda x : 0 if x < 0
        #                                       else x)
        # insp['HC_CO2'] = insp['HC_CO2'].apply(lambda x : 0 if x < 0
        #                                       else x/10000)
        # insp['NO_CO2'] = insp['NO_CO2'].apply(lambda x : 0 if x < 0
        #                                       else x/10000)

        # 배출가스 / CO2 HC, NO의 단위가 ppm이므로 %로 변환
        insp['HC_CO2'] = insp['HC_CO2'] / 10000
        insp['NO_CO2'] = insp['NO_CO2'] / 10000

        # 연료별 NOX 계수
        insp['NOX_FUEL_COE'] = insp['FUEL_UP_CD_NM'].map({'휘발유': 1.87, '가스': 1.87, '경유': 1.92})

        # RSD수시점검에서 NOX의 측정치가 유효한 장비코드만 산출
        insp_nu = insp['EQMT_CD_NM'].str[-4:].isin(['5051', '5061', '5503', '5504', '5508', '5319'])

        # 공식을 이용한 배출가스와 연료별 값 산출
        insp['CO_FUEL'] = (28 * insp['CO_CO2'] * 860) / ((1 + insp['CO_CO2'] + (insp['HC_CO2'] * 6)) * 12)
        insp['HC_FUEL'] = (88 * insp['HC_CO2'] * 860) / ((1 + insp['CO_CO2'] + (insp['HC_CO2'] * 6)) * 12)
        insp.loc[insp_nu, 'NOX_FUEL'] = (30 * insp['NO_CO2'] * 1000) / (
                (1 + insp['CO_CO2'] + (6 * insp['HC_CO2'])) * (12 + insp['NOX_FUEL_COE']))

        # 연료별 계수 입력
        insp['FUEL_CO2'] = insp['FUEL_UP_CD_NM'].map({'휘발유': 0.000315457, '가스': 0.000315457, '경유': 0.000316456})

        # 배출가스별 양 산출
        insp['CO_QT'] = insp['CO_FUEL'] * insp['FUEL_CO2'] * insp['AVRG_CVS_75'] * (1 + insp['CO2_GAP'])
        insp['HC_QT'] = insp['HC_FUEL'] * insp['FUEL_CO2'] * insp['AVRG_CVS_75'] * (1 + insp['CO2_GAP'])
        insp['NOX_QT'] = insp['NOX_FUEL'] * insp['FUEL_CO2'] * insp['AVRG_CVS_75'] * (1 + insp['CO2_GAP'])

        # RSD VSP가 3이상 22이하가 아닐 경우 배출가스별 양 제거
        insp_rv_np = (insp['VSP'] < 3) | (insp['VSP'] > 22) | (insp['VSP'].isnull())
        insp.loc[insp_rv_np, ['CO_QT', 'HC_QT', 'NOX_QT']] = [np.nan, np.nan, np.nan]

        # 유로 구분, 배출가스 인증번호 , 연료, 배출가스 등급 별 배출가스의 양 평균 값 산출
        avrg_qt = insp[(insp['INSP_SE'] == 'RSD수시점검')].groupby(
            ['EURO_SE', 'EXHST_GAS_CERT_NO', 'FUEL_UP_CD_NM', 'EXHST_GAS_GRD_CD_NM'])[['CO_QT', 'HC_QT', 'NOX_QT']].agg(
            CNT=('CO_QT', 'count'), CO_EXHST_QT=('CO_QT', 'mean'), HC_EXHST_QT=('HC_QT', 'mean'),
            NOX_EXHST_QT=('NOX_QT', 'mean')).reset_index()

        # CLT(중심 극한 정리)에 의해서 개수가 30개 이상인 것만 추출
        avrg_qt = avrg_qt[avrg_qt['CNT'] >= 30]

        avrg_qt['CO_EXHST_QT'] = avrg_qt['CO_EXHST_QT'].apply(lambda x: 0 if x < 0
        else x)
        avrg_qt['HC_EXHST_QT'] = avrg_qt['HC_EXHST_QT'].apply(lambda x: 0 if x < 0
        else x)
        avrg_qt['NOX_EXHST_QT'] = avrg_qt['NOX_EXHST_QT'].apply(lambda x: 0 if x < 0
        else x)

        # 주행거리가 없는 경우 제거
        insp_j = insp_j[insp_j['DRVNG_DSTNC'].notnull()]

        # 한 차량이 통과를 한 날에는 검사를 더 이상 받지 않는다고 설정
        insp_j = insp_j.sort_values(by='INSP_YMD')
        insp_j = insp_j.drop_duplicates(['VIN', 'VHRNO', 'INSP_YMD'], keep='last')

        # 최근 통과한 검사 데이터 추출
        insp_af = insp_j.drop_duplicates(['VIN', 'VHRNO'], keep='last')

        # 최근 바로 이전 검사 데이터 추출
        insp_bf = insp_j.drop(index=insp_af.index)
        insp_bf.drop_duplicates(['VIN', 'VHRNO'], keep='last', inplace=True)

        insp_bf = insp_bf.loc[:, ['VIN', 'VHRNO', 'DRVNG_DSTNC', 'INSP_YMD']]

        insp_bf.rename(columns={'DRVNG_DSTNC': 'DRVNG_DSTNC_BF', 'INSP_YMD': 'INSP_YMD_BF'}, inplace=True)
        insp_af.rename(columns={'DRVNG_DSTNC': 'DRVNG_DSTNC_AF', 'INSP_YMD': 'INSP_YMD_AF'}, inplace=True)

        insp_t = insp_af.merge(insp_bf, how='left', on=['VIN', 'VHRNO'])

        # 일 평균 주행거리 추출
        insp_t['DF_DRVNG_DSTNC_M'] = (insp_t['DRVNG_DSTNC_AF'] - insp_t['DRVNG_DSTNC_BF']) / (
            (insp_t['INSP_YMD_AF'] - insp_t['INSP_YMD_BF']).dt.days)

        # 일 평균 주행거리가 음수인 경우 제거
        insp_t = insp_t[insp_t['DF_DRVNG_DSTNC_M'] >= 0]

        # 시군구명, 유로 구분, 차종, 연료, 배출가스 인증번호, 배출가스 등급 별 일 평균 주행거리의 평균과 차량 수 계산
        insp_dm = insp_t.groupby(
            ['STDG_SGG_NM', 'EURO_SE', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM', 'EXHST_GAS_CERT_NO', 'EXHST_GAS_GRD_CD_NM'])[
            ['DF_DRVNG_DSTNC_M']].agg(INSP_2_ABOVE_VHCL_CNT=('DF_DRVNG_DSTNC_M', 'count')).reset_index()

        insp_dm = insp_dm.merge(avrg_qt, how='left', on = ['EURO_SE', 'FUEL_UP_CD_NM', 'EXHST_GAS_CERT_NO', 'EXHST_GAS_GRD_CD_NM'])
        
        # 테이블 생성 연월일
        insp_dm['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_exhst_gas_cert_no_qt'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(insp_dm.columns):
            if 'float' in insp_dm[column].dtype.name:
                sql += column+' float'
            elif 'int' in insp_dm[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(insp_dm.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(insp_dm, table_nm)

        # 시도군구명, 유로 구분, 연료, 차종 별 배출가스 총량 평균 및 2번 이상 검사받은 차량 수
        insp_dm_1 = insp_dm.groupby(['STDG_SGG_NM', 'EURO_SE', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM']).agg(
            CO_AVRG_EXHST_QT=('CO_EXHST_QT', 'mean'), HC_AVRG_EXHST_QT=('HC_EXHST_QT', 'mean'),
            NOX_AVRG_EXHST_QT=('NOX_EXHST_QT', 'mean'),
            INSP_2_ABOVE_VHCL_CNT=('INSP_2_ABOVE_VHCL_CNT', 'sum')).reset_index()

        # 시도군구명, 유로 구분, 연료, 차종 별 차량 수
        insp_dm_2 = car.groupby(['STDG_SGG_NM', 'EURO_SE', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM']).size().reset_index(
            name='VHCL_CNT')

        insp_dm_f = insp_dm_1.merge(insp_dm_2, how='right', on = ['STDG_SGG_NM', 'EURO_SE', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM'])

        # 시도군구명, 연료, 차종, 유로 구분 별 일 평균주행거리의 평균
        insp_dm_3 = insp_t.groupby(['STDG_SGG_NM', 'FUEL_UP_CD_NM', 'EURO_SE', 'VHCTY_CD_NM']).agg(
            DY_AVRG_DRVNG_DSTNC=('DF_DRVNG_DSTNC_M', 'mean')).reset_index()
        insp_dm_3['DY_AVRG_DRVNG_DSTNC'] = self.roundTraditional(insp_dm_3['DY_AVRG_DRVNG_DSTNC'], 4)
        insp_dm_f = insp_dm_f.merge(insp_dm_3, how='left', on = ['STDG_SGG_NM', 'EURO_SE', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM'])

        # 배출가스별 총 량 계산
        insp_dm_f['CO_TOTL_QT'] = insp_dm_f['DY_AVRG_DRVNG_DSTNC'] * insp_dm_f['VHCL_CNT'] * insp_dm_f[
            'CO_AVRG_EXHST_QT']
        insp_dm_f['HC_TOTL_QT'] = insp_dm_f['DY_AVRG_DRVNG_DSTNC'] * insp_dm_f['VHCL_CNT'] * insp_dm_f[
            'HC_AVRG_EXHST_QT']
        insp_dm_f['NOX_TOTL_QT'] = insp_dm_f['DY_AVRG_DRVNG_DSTNC'] * insp_dm_f['VHCL_CNT'] * insp_dm_f[
            'NOX_AVRG_EXHST_QT']

        insp_dm_f = insp_dm_f.groupby(['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM']).agg(
            CO_AVRG_EXHST_QT=('CO_AVRG_EXHST_QT', 'mean'), HC_AVRG_EXHST_QT=('HC_AVRG_EXHST_QT', 'mean'),
            NOX_AVRG_EXHST_QT=('NOX_AVRG_EXHST_QT', 'mean'),
            INSP_2_ABOVE_VHCL_CNT=('INSP_2_ABOVE_VHCL_CNT', 'sum'), VHCL_CNT=('VHCL_CNT', 'sum'),
            CO_TOTL_QT=('CO_TOTL_QT', 'sum'), HC_TOTL_QT=('HC_TOTL_QT', 'sum'),
            NOX_TOTL_QT=('NOX_TOTL_QT', 'sum')).reset_index()

        # 시도군구, 연료, 차종 별 배출가스 측정치 합 및 1회 이상 검사 받은 차량 수
        insp_tg = insp_t.groupby(['STDG_SGG_NM', 'FUEL_UP_CD_NM', 'VHCTY_CD_NM'])[
            'CO_1_ITM_MEVLU', 'HC_1_ITM_MEVLU', 'NOX_MEVLU', 'SMO_1_ITM_MEVLU', 'DRVNG_DSTNC_AF', 'DF_DRVNG_DSTNC_M'].agg(
            CO_TOTL_MSRMT_VAL=('CO_1_ITM_MEVLU', 'sum'), CO_MSRMT_VHCL_CNT=('CO_1_ITM_MEVLU', 'count'),
            AVRG_CO_MSRMT_VAL=('CO_1_ITM_MEVLU', 'mean'),
            HC_TOTL_MSRMT_VAL=('HC_1_ITM_MEVLU', 'sum'), HC_MSRMT_VHCL_CNT=('HC_1_ITM_MEVLU', 'count'),
            AVRG_HC_MSRMT_VAL=('HC_1_ITM_MEVLU', 'mean'),
            NOX_TOTL_MSRMT_VAL=('NOX_MEVLU', 'sum'), NOX_MSRMT_VHCL_CNT=('NOX_MEVLU', 'count'),
            AVRG_NOX_MSRMT_VAL=('NOX_MEVLU', 'mean'),
            SMOKE_TOTL_MSRMT_VAL=('SMO_1_ITM_MEVLU', 'sum'), SMOKE_MSRMT_VHCL_CNT=('SMO_1_ITM_MEVLU', 'count'),
            AVRG_SMOKE_MSRMT_VAL=('SMO_1_ITM_MEVLU', 'mean'),
            INSP_1_ABOVE_VHCL_CNT=('DRVNG_DSTNC_AF', 'count')).reset_index()

        # 배출가스별 등급 개수
        insp_dm_f = insp_dm_f.merge(insp_t[insp_t['CO_1_ITM_MEVLU'].notnull()].groupby(
            ['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM', 'EXHST_GAS_GRD_CD_NM']).size().unstack().reset_index(),
                                    how='left')

        # 배출가스 등급 5 및 X가 없을 경우 해당 컬럼 생성
        if '1' not in insp_dm_f:
            insp_dm_f['1'] = np.nan

        if '2' not in insp_dm_f:
            insp_dm_f['2'] = np.nan

        if '3' not in insp_dm_f:
            insp_dm_f['3'] = np.nan

        if '4' not in insp_dm_f:
            insp_dm_f['4'] = np.nan

        if '5' not in insp_dm_f:
            insp_dm_f['5'] = np.nan

        if 'X' not in insp_dm_f:
            insp_dm_f['X'] = np.nan

        insp_dm_f.rename(columns={'1': 'CO_EXHST_GAS_1_GRD_VHCL_CNT', '2': 'CO_EXHST_GAS_2_GRD_VHCL_CNT',
                                  '3': 'CO_EXHST_GAS_3_GRD_VHCL_CNT', '4': 'CO_EXHST_GAS_4_GRD_VHCL_CNT',
                                  '5': 'CO_EXHST_GAS_5_GRD_VHCL_CNT', 'X': 'CO_EXHST_GAS_NTJGMT_VHCL_CNT'},
                         inplace=True)

        insp_dm_f = insp_dm_f.merge(insp_t[insp_t['HC_1_ITM_MEVLU'].notnull()].groupby(
            ['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM', 'EXHST_GAS_GRD_CD_NM']).size().unstack().reset_index(),
                                    how='left', on = ['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM'])

        # 배출가스 등급 5 및 X가 없을 경우 해당 컬럼 생성
        if '1' not in insp_dm_f:
            insp_dm_f['1'] = np.nan

        if '2' not in insp_dm_f:
            insp_dm_f['2'] = np.nan

        if '3' not in insp_dm_f:
            insp_dm_f['3'] = np.nan

        if '4' not in insp_dm_f:
            insp_dm_f['4'] = np.nan

        if '5' not in insp_dm_f:
            insp_dm_f['5'] = np.nan

        if 'X' not in insp_dm_f:
            insp_dm_f['X'] = np.nan

        insp_dm_f.rename(columns={'1': 'HC_EXHST_GAS_1_GRD_VHCL_CNT', '2': 'HC_EXHST_GAS_2_GRD_VHCL_CNT',
                                  '3': 'HC_EXHST_GAS_3_GRD_VHCL_CNT', '4': 'HC_EXHST_GAS_4_GRD_VHCL_CNT',
                                  '5': 'HC_EXHST_GAS_5_GRD_VHCL_CNT', 'X': 'HC_EXHST_GAS_NTJGMT_VHCL_CNT'},
                         inplace=True)

        insp_dm_f = insp_dm_f.merge(insp_t[insp_t['NOX_MEVLU'].notnull()].groupby(
            ['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM', 'EXHST_GAS_GRD_CD_NM']).size().unstack().reset_index(),
                                    how='left', on = ['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM'])

        # 배출가스 등급 5 및 X가 없을 경우 해당 컬럼 생성
        if '1' not in insp_dm_f:
            insp_dm_f['1'] = np.nan

        if '2' not in insp_dm_f:
            insp_dm_f['2'] = np.nan

        if '3' not in insp_dm_f:
            insp_dm_f['3'] = np.nan

        if '4' not in insp_dm_f:
            insp_dm_f['4'] = np.nan

        if '5' not in insp_dm_f:
            insp_dm_f['5'] = np.nan

        if 'X' not in insp_dm_f:
            insp_dm_f['X'] = np.nan

        insp_dm_f.rename(columns={'1': 'NOX_EXHST_GAS_1_GRD_VHCL_CNT', '2': 'NOX_EXHST_GAS_2_GRD_VHCL_CNT',
                                  '3': 'NOX_EXHST_GAS_3_GRD_VHCL_CNT', '4': 'NOX_EXHST_GAS_4_GRD_VHCL_CNT',
                                  '5': 'NOX_EXHST_GAS_5_GRD_VHCL_CNT', 'X': 'NOX_EXHST_GAS_NTJGMT_VHCL_CNT'},
                         inplace=True)

        insp_dm_f = insp_dm_f.merge(insp_t[insp_t['SMO_1_ITM_MEVLU'].notnull()].groupby(
            ['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM', 'EXHST_GAS_GRD_CD_NM']).size().unstack().reset_index(),
                                    how='left', on = ['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM'])

        # 배출가스 등급 5 및 X가 없을 경우 해당 컬럼 생성
        if '1' not in insp_dm_f:
            insp_dm_f['1'] = np.nan

        if '2' not in insp_dm_f:
            insp_dm_f['2'] = np.nan

        if '3' not in insp_dm_f:
            insp_dm_f['3'] = np.nan

        if '4' not in insp_dm_f:
            insp_dm_f['4'] = np.nan

        if '5' not in insp_dm_f:
            insp_dm_f['5'] = np.nan

        if 'X' not in insp_dm_f:
            insp_dm_f['X'] = np.nan

        insp_dm_f.rename(columns={'1': 'SMO_EXHST_GAS_1_GRD_VHCL_CNT', '2': 'SMO_EXHST_GAS_2_GRD_VHCL_CNT',
                                  '3': 'SMO_EXHST_GAS_3_GRD_VHCL_CNT', '4': 'SMO_EXHST_GAS_4_GRD_VHCL_CNT',
                                  '5': 'SMO_EXHST_GAS_5_GRD_VHCL_CNT', 'X': 'SMO_EXHST_GAS_NTJGMT_VHCL_CNT'},
                         inplace=True)

        insp_dm_f = insp_dm_f.merge(insp_tg, how='left', on = ['STDG_SGG_NM', 'VHCTY_CD_NM', 'FUEL_UP_CD_NM'])

        # 시도군구명에서 시 혹은 도 하나로만 되어 있는 경우 제거 후 고유한 시군구명 추출
        cdf_u = cdf[cdf['STDG_SGG_CD'] - (cdf['STDG_SGG_CD'] // 1000) * 1000 != 0].groupby(
            ['STDG_SGG_NM']).size().reset_index(name='CNT')
        cdf_u = cdf_u.loc[:, ['STDG_SGG_NM']]

        # 시각화를 위한 시도군구별 연료, 차종의 모든 경우를 데이터프레임화

        cdf_tmp = cdf_u.copy()
        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['휘발유', '승용']
        cdf_f = cdf_tmp.copy()

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['휘발유', '승합']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['휘발유', '화물']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['휘발유', '특수']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['가스', '승용']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['가스', '승합']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['가스', '화물']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['가스', '특수']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['경유', '승용']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['경유', '승합']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['경유', '화물']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        cdf_tmp[['FUEL_UP_CD_NM', 'VHCTY_CD_NM']] = ['경유', '특수']
        cdf_f = pd.concat([cdf_f, cdf_tmp])

        # NULL일 경우 0으로 대체
        insp_dm_ff = cdf_f.merge(insp_dm_f, how='left', on=['STDG_SGG_NM', 'FUEL_UP_CD_NM', 'VHCTY_CD_NM']).fillna(0)
        
        # 테이블 생성 연월일
        insp_dm_ff['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_sgg_exhst_gas_cert_no'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(insp_dm_ff.columns):
            if 'float' in insp_dm_ff[column].dtype.name:
                sql += column+' float'
            elif 'int' in insp_dm_ff[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(insp_dm_ff.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(insp_dm_ff, table_nm)
        # 3126sec(52min)


    def proj05_analysis_01(self):
        """
        RSD분석-1(구현 분석가: 김강민)
        :return:
        """

        # STD_BD_CAR_INSP_HST에서 필요한 컬럼 추출
        # sql_stmt = """
        #             select
        #                 VIN, VHRNO, INSP_MSRMT_MTHD, INSP_YMD, CO_1_ITM_MEVLU, HC_1_ITM_MEVLU, NOX_MEVLU, SMO_1_ITM_MEVLU, INSP_PASAGE_YN
        #             from
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 INSP_MSRMT_MTHD in ('부하검사(ASM2525)', '부하검사(ASM-Idling)', 'RSD', '부하검사(LUG DOWN)')
        #                 AND INSP_PASAGE_YN IS NOT NULL
        #            """
        # INSP = self.dbUtil.export_to_pandas(sql_stmt)
        INSP = self.INSP[(self.INSP['INSP_MSRMT_MTHD'].isin(['부하검사(ASM2525)', '부하검사(ASM-Idling)', 'RSD', '부하검사(LUG DOWN)'])) & (self.INSP['INSP_PASAGE_YN'].notnull())].copy()

        # STD_BD_CAR에서 필요한 컬럼 추출
        # sql_stmt = """
        #             select
        #                 VIN, VHRNO, YRIDNW, FUEL_CD_NM, VHCTY_CD_NM
        #             from
        #                 "vsysb".STD_BD_CAR
        #             WHERE
        #                 VHCL_ERSR_YN = 'N'
        #                 AND FUEL_CD_NM in ('휘발유', '휘발유 하이브리드', 'LPG(액화석유가스)', '경유')
        #            """
        # CAR = self.dbUtil.export_to_pandas(sql_stmt)
        CAR = self.CAR[(self.CAR['VHCL_ERSR_YN'] == 'N') & ((self.CAR['FUEL_CD_NM'].isin(['휘발유', '휘발유 하이브리드', 'LPG(액화석유가스)', '경유'])))].copy()


        # RSD수시점검 데이터 중에서 현재연도의 전월 기준 5년 데이터만 사용
        RSD = INSP[(INSP['INSP_MSRMT_MTHD'] == 'RSD') & (
                    INSP['INSP_YMD'] < pd.to_datetime(datetime.date.today().strftime('%Y%m'), format='%Y%m')) & (
                               INSP['INSP_YMD'] >= pd.to_datetime(datetime.date.today().strftime('%Y%m'),
                                                                  format='%Y%m') - relativedelta(years=5))]

        # 컬럼 명 변경
        RSD.rename(columns={'CO_1_ITM_MEVLU': 'RSD_CO_1_ITM_MEVLU', 'HC_1_ITM_MEVLU': 'RSD_HC_1_ITM_MEVLU',
                            'NOX_MEVLU': 'RSD_NOX_MEVLU', 'SMO_1_ITM_MEVLU': 'RSD_SMO_1_ITM_MEVLU',
                            'INSP_YMD': 'RSD_INSP_YMD'}, inplace=True)
        RSD = RSD.loc[:, ['VIN', 'VHRNO', 'RSD_INSP_YMD', 'RSD_CO_1_ITM_MEVLU', 'RSD_HC_1_ITM_MEVLU', 'RSD_NOX_MEVLU', 'RSD_SMO_1_ITM_MEVLU']]
        RSD.reset_index(inplace = True)

        # 경유 차량이 부하검사(LUG DOWN)을 안 받은 경우 제거
        ASM = INSP[(INSP['INSP_MSRMT_MTHD'] != 'RSD') & (INSP['INSP_PASAGE_YN'] == 'Y') & (
                    INSP['INSP_MSRMT_MTHD'] != '부하검사(LUG DOWN)')].merge(CAR[CAR['FUEL_CD_NM'] != '경유'], how='inner',
                                                                        on=['VIN', 'VHRNO'])

        # 경유 차량이 아닌데 부하검사(LUG DOWN)을 받은 경우 제거
        LUG = INSP[(INSP['INSP_MSRMT_MTHD'] != 'RSD') & (INSP['INSP_PASAGE_YN'] == 'Y') & (
                    INSP['INSP_MSRMT_MTHD'] == '부하검사(LUG DOWN)')].merge(CAR[CAR['FUEL_CD_NM'] == '경유'], how='inner',
                                                                        on=['VIN', 'VHRNO'])
        
        ASM = ASM.loc[:, ['VIN', 'VHRNO', 'YRIDNW', 'FUEL_CD_NM', 'VHCTY_CD_NM', 'INSP_YMD', 'DRVNG_DSTNC', 'CO_1_ITM_MEVLU', 'HC_1_ITM_MEVLU', 'NOX_MEVLU', 'SMO_1_ITM_MEVLU']]
        LUG = LUG.loc[:, ['VIN', 'VHRNO', 'YRIDNW', 'FUEL_CD_NM', 'VHCTY_CD_NM', 'INSP_YMD', 'DRVNG_DSTNC', 'CO_1_ITM_MEVLU', 'HC_1_ITM_MEVLU', 'NOX_MEVLU', 'SMO_1_ITM_MEVLU']]
        
        bu_insp = pd.concat([ASM, LUG], ignore_index = True)
        
        rsd = RSD.merge(bu_insp, how = 'inner', on = ['VIN', 'VHRNO'])

        # RSD수시점검 데이터 중에서 각 정기검사별 가장 가까운 검사만 추출
        rsd['DT_DY'] = abs((rsd['RSD_INSP_YMD'] - rsd['INSP_YMD']).dt.days)
        
        rsd_mean = rsd.groupby(['index', 'DT_DY', 'DRVNG_DSTNC'], dropna = False)['CO_1_ITM_MEVLU', 'HC_1_ITM_MEVLU', 'NOX_MEVLU', 'SMO_1_ITM_MEVLU'].mean().reset_index()

        rsd = rsd.drop(columns = ['CO_1_ITM_MEVLU', 'HC_1_ITM_MEVLU', 'NOX_MEVLU', 'SMO_1_ITM_MEVLU']).merge(rsd_mean, how = 'left', on = ['index', 'DT_DY', 'DRVNG_DSTNC'])
        
        rsd.sort_values(by=['index', 'DT_DY', 'DRVNG_DSTNC'], ascending=[True, True, False], na_position='last', inplace=True)

        rsd_u = rsd.drop_duplicates(['index'], keep = 'first')

        # 배출가스별 차량 수, 평균값, 중앙값 산출
        rsd_g_co = rsd_u[rsd_u['CO_1_ITM_MEVLU'].notnull()].groupby(['YRIDNW', 'FUEL_CD_NM', 'VHCTY_CD_NM']).agg(
            EXHST_GAS_VHCL_CNT=('CO_1_ITM_MEVLU', 'count'), RSD_AVRG_VAL=('RSD_CO_1_ITM_MEVLU', 'mean'),
            RSD_MED_VAL=('RSD_CO_1_ITM_MEVLU', 'median'),
            INSP_AVRG_VAL=('CO_1_ITM_MEVLU', 'mean'), INSP_MED_VAL=('CO_1_ITM_MEVLU', 'median')).reset_index()
        rsd_g_co['EXHST_GAS'] = 'CO'

        rsd_g_hc = rsd_u[rsd_u['HC_1_ITM_MEVLU'].notnull()].groupby(['YRIDNW', 'FUEL_CD_NM', 'VHCTY_CD_NM']).agg(
            EXHST_GAS_VHCL_CNT=('HC_1_ITM_MEVLU', 'count'), RSD_AVRG_VAL=('RSD_HC_1_ITM_MEVLU', 'mean'),
            RSD_MED_VAL=('RSD_HC_1_ITM_MEVLU', 'median'),
            INSP_AVRG_VAL=('HC_1_ITM_MEVLU', 'mean'), INSP_MED_VAL=('HC_1_ITM_MEVLU', 'median')).reset_index()
        rsd_g_hc['EXHST_GAS'] = 'HC'

        rsd_g_nox = rsd_u[rsd_u['NOX_MEVLU'].notnull()].groupby(['YRIDNW', 'FUEL_CD_NM', 'VHCTY_CD_NM']).agg(
            EXHST_GAS_VHCL_CNT=('NOX_MEVLU', 'count'), RSD_AVRG_VAL=('RSD_NOX_MEVLU', 'mean'),
            RSD_MED_VAL=('RSD_NOX_MEVLU', 'median'),
            INSP_AVRG_VAL=('NOX_MEVLU', 'mean'), INSP_MED_VAL=('NOX_MEVLU', 'median')).reset_index()
        rsd_g_nox['EXHST_GAS'] = 'NOX'

        rsd_g_smoke = rsd_u[rsd_u['SMO_1_ITM_MEVLU'].notnull()].groupby(['YRIDNW', 'FUEL_CD_NM', 'VHCTY_CD_NM']).agg(
            EXHST_GAS_VHCL_CNT=('SMO_1_ITM_MEVLU', 'count'), RSD_AVRG_VAL=('RSD_SMO_1_ITM_MEVLU', 'mean'),
            RSD_MED_VAL=('RSD_SMO_1_ITM_MEVLU', 'median'),
            INSP_AVRG_VAL=('SMO_1_ITM_MEVLU', 'mean'), INSP_MED_VAL=('SMO_1_ITM_MEVLU', 'median')).reset_index()
        rsd_g_smoke['EXHST_GAS'] = 'SMOKE'

        rsd_g = pd.concat([rsd_g_co, rsd_g_hc, rsd_g_nox, rsd_g_smoke])

        # 선형 회귀 식의 계수, Y절편, 결정 계수 산출 함수
        def lin_regress(data, yvar, xvar):
            Y = data[yvar]
            X = data[xvar]
            linreg = linear_model.LinearRegression()
            model = linreg.fit(X, Y)
            score = model.score(X, Y)
            intercept = model.intercept_
            coef = model.coef_
            coef = coef[0]
            return [intercept, coef, score]

        # 차량 수가 30개 이상인 것만 식 산출
        grouped = rsd_g[rsd_g['EXHST_GAS_VHCL_CNT'] >= 30].groupby(['FUEL_CD_NM', 'VHCTY_CD_NM', 'EXHST_GAS'])
        lin_regress_result = grouped.apply(lin_regress, 'RSD_AVRG_VAL', ['INSP_AVRG_VAL']).reset_index(name='REGRESS')

        lin_regress_result[['Y_INCE', 'CFFCNT', 'DCSN_CFFCNT']] = pd.DataFrame(lin_regress_result['REGRESS'].tolist(), index=lin_regress_result.index)
        lin_regress_result.drop('REGRESS', axis = 1, inplace = True)
        
        rsd_f = rsd_g.merge(lin_regress_result, how = 'left', on = ['FUEL_CD_NM', 'VHCTY_CD_NM', 'EXHST_GAS'])

        # 선형회귀식을 통해서 구한 예상 RSD 평균 값
        rsd_f['EXPC_RSD_AVRG_VAL'] = rsd_f['INSP_AVRG_VAL'] * rsd_f['CFFCNT'] + rsd_f['Y_INCE']

        rsd_f.drop(columns=['CFFCNT', 'Y_INCE'], inplace=True)
        
        rsd_f.sort_values(by = ['YRIDNW'], inplace = True)
    
        
        # 테이블 생성 연월일
        rsd_f['LOAD_DT'] = datetime.date.today()
        
        # lPG(액화석유가스) -> 가스
        rsd_ff = rsd_f['FUEL_CD_NM'] == 'LPG(액화석유가스)'
        rsd_f.loc[rsd_ff, ['FUEL_CD_NM']] = '가스'

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_rsd_insp'
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(rsd_f.columns):
            if 'float' in rsd_f[column].dtype.name:
                sql += column+' float'
            elif 'int' in rsd_f[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(rsd_f.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(rsd_f, table_nm)
        # 1783sec(30min)

    def proj05_analysis_02(self):
        """
        RSD분석-2(구현 분석가: 김강민)
        :return:
        """

        # STD_BD_CAR_INSP_HST에서 필요한 컬럼 추출
        # sql_stmt = """
        #             select
        #                 VIN, VHRNO, INSP_MSRMT_MTHD, INSP_YMD, DRVNG_DSTNC, CO_1_ITM_MEVLU, HC_1_ITM_MEVLU, NOX_MEVLU, SMO_1_ITM_MEVLU
        #             from
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 DRVNG_DSTNC >= 0 AND INSP_PASAGE_YN = 'Y'
        #            """
        # INSP = dbUtil.export_to_pandas(sql_stmt)
        INSP = self.INSP[(self.INSP['DRVNG_DSTNC'] >= 0) & (self.INSP['INSP_PASAGE_YN'] == 'Y')]

        # STD_BD_CAR에서 필요한 컬럼 추출
        # sql_stmt = """
        #             select
        #                 VIN, VHRNO, FUEL_UP_CD_NM, VHCTY_CD_NM
        #             from
        #                 "vsysb".STD_BD_CAR
        #             WHERE
        #                 VHCL_ERSR_YN = 'N'
        #            """
        # CAR = dbUtil.export_to_pandas(sql_stmt)
        CAR = self.CAR[self.CAR['VHCL_ERSR_YN'] == 'N']

        insp = INSP.copy()

        # 당월의 데이터는 제거
        insp = insp[(insp['INSP_YMD'] < pd.to_datetime(datetime.date.today().strftime('%Y%m'), format='%Y%m'))]

        # 주행거리를 10,000단위로 구분
        insp['DRVNG_DSTNC_UNIT_10000'] = insp['DRVNG_DSTNC'] // 10000

        insp = insp.merge(CAR, how='inner', on=['VIN', 'VHRNO'])

        # 배출가스별 측청지 합과 차량 수 게산
        insp_co = insp[insp['CO_1_ITM_MEVLU'].notnull()].groupby(
            ['INSP_MSRMT_MTHD', 'FUEL_UP_CD_NM', 'VHCTY_CD_NM', 'DRVNG_DSTNC_UNIT_10000']).agg(
            TOTL_MEVLU=('CO_1_ITM_MEVLU', 'sum'), VHCL_CNT=('CO_1_ITM_MEVLU', 'count')).reset_index()
        insp_hc = insp[insp['HC_1_ITM_MEVLU'].notnull()].groupby(
            ['INSP_MSRMT_MTHD', 'FUEL_UP_CD_NM', 'VHCTY_CD_NM', 'DRVNG_DSTNC_UNIT_10000']).agg(
            TOTL_MEVLU=('HC_1_ITM_MEVLU', 'sum'), VHCL_CNT=('HC_1_ITM_MEVLU', 'count')).reset_index()
        insp_nox = insp[insp['NOX_MEVLU'].notnull()].groupby(
            ['INSP_MSRMT_MTHD', 'FUEL_UP_CD_NM', 'VHCTY_CD_NM', 'DRVNG_DSTNC_UNIT_10000']).agg(
            TOTL_MEVLU=('NOX_MEVLU', 'sum'), VHCL_CNT=('NOX_MEVLU', 'count')).reset_index()
        insp_smoke = insp[insp['SMO_1_ITM_MEVLU'].notnull()].groupby(
            ['INSP_MSRMT_MTHD', 'FUEL_UP_CD_NM', 'VHCTY_CD_NM', 'DRVNG_DSTNC_UNIT_10000']).agg(
            TOTL_MEVLU=('SMO_1_ITM_MEVLU', 'sum'), VHCL_CNT=('SMO_1_ITM_MEVLU', 'count')).reset_index()

        insp_co['EXHST_GAS'] = 'CO'
        insp_hc['EXHST_GAS'] = 'HC'
        insp_nox['EXHST_GAS'] = 'NOX'
        insp_smoke['EXHST_GAS'] = 'SMOKE'

        insp_g = pd.concat([insp_co, insp_hc, insp_nox, insp_smoke])
        
        # 테이블 생성 연월일
        insp_g['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_drvng_dstnc_acto_mevlu'
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(insp_g.columns):
            if 'float' in insp_g[column].dtype.name:
                sql += column+' float'
            elif 'int' in insp_g[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(insp_g.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(insp_g, table_nm)
        # 3104sec


    def proj05_analysis_03(self):
        """
        RSD분석-03(구현 분석가: 김강민)
        :return:
        """
        # TB_RSD_FINAL_JGMT에서 필요한 컬럼 추출
        sql_stmt = """
                    SELECT /*+ PARALLEL(2) */
                        *
                    FROM
                        "vsysb".TB_RSD_FINAL_JGMT
                    WHERE
                        VLD_YN = 'Y'
                    """
        RSD = self.dbUtil.export_to_pandas(sql_stmt)

        # TB_RSD_EMS_STD에서 필요한 컬럼 추출
        sql_stmt = """
                    SELECT
                        STD_TAG, CO_EX, PPMHC_EX, PPMNO_EX
                    FROM
                        "vsysb".TMP_TB_RSD_EMS_STD
                    """
        STD = self.dbUtil.export_to_pandas(sql_stmt)

        # RSD 허용치 부여
        rsd = RSD.merge(STD, how='left', on=['STD_TAG'])

        rsd['CO'] = pd.to_numeric(rsd['CO'], errors='coerce')
        rsd['PPMHC'] = pd.to_numeric(rsd['PPMHC'], errors='coerce')
        rsd['PPMNO'] = pd.to_numeric(rsd['PPMNO'], errors='coerce')

        rsd['CO_EX'] = rsd['CO_EX'] * 3
        rsd['PPMHC_EX'] = rsd['PPMHC_EX'] * 3
        rsd['PPMNO_EX'] = rsd['PPMNO_EX'] * 3

        # 측정치가 음수인 경우 0으로 변경
        rsd_s = rsd['CO'] < 0
        rsd.loc[rsd_s, 'CO'] = 0
        rsd_s = rsd['PPMHC'] < 0
        rsd.loc[rsd_s, 'PPMHC'] = 0
        rsd_s = rsd['PPMNO'] < 0
        rsd.loc[rsd_s, 'PPMNO'] = 0

        rsd['SI_CO'] = rsd['CO'] / rsd['CO_EX']
        rsd['SI_HC'] = rsd['PPMHC'] / rsd['PPMHC_EX']
        rsd['SI_NOX'] = rsd['PPMNO'] / rsd['PPMNO_EX']

        # 차종 분류
        rsd['VHCTY_CD_NM'] = rsd['CAR_TYPE_NM'].str[:2]

        # 연료 분류
        rsd['FUEL_UP_CD_NM'] = '가스'
        rsd_s = (rsd['CAR_FUEL_NM'].isin(['휘발유', '휘발유(무연)', '휘발유(유연)', '하이브리드(휘발유+전기)']))
        rsd.loc[rsd_s, 'FUEL_UP_CD_NM'] = '휘발유'
        rsd_s = (rsd['CAR_FUEL_NM'].isin(['CNG', '하이브리드(CNG+전기)']))
        rsd.loc[rsd_s, 'FUEL_UP_CD_NM'] = '기타'

        # 연료, 차종, 연식, 초과여부 별 평균값 계산
        rsd_g = rsd.groupby(['FUEL_UP_CD_NM', 'VHCTY_CD_NM', 'CAR_YEAR', 'EXCS_YN']).agg(
            RSD_INSP_CNT=('SI_CO', 'count'), AVRG_SI_CO=('SI_CO', 'mean'), AVRG_SI_HC=('SI_HC', 'mean'),
            AVRG_SI_NOX=('SI_NOX', 'mean')).reset_index()

        rsd_g.rename(columns={'CAR_YEAR': 'YRIDNW'}, inplace=True)
        
        # 테이블 생성 연월일
        rsd_g['LOAD_DT'] = datetime.date.today()

        table_nm = 'std_bd_rsd_excs'.lower()
        
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(rsd_g.columns):
            if 'float' in rsd_g[column].dtype.name:
                sql += column+' float'
            elif 'int' in rsd_g[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(rsd_g.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        # 데이터프레임을 DB에 적재
        self.dbUtil2.import_from_pandas(rsd_g, table_nm)
        # 499sec(7min)


    def proj06_analysis_01(self):
        """
        (구현 분석가: 윤혜진)
        :return:
        """

        # ----------<BD_CAR>-------------
        # sql_stmt = """
        #            SELECT
        #                VHRNO, VIN, FRST_REG_YMD, ACQS_YMD, VHCTY_CD_NM, EXHST_GAS_GRD_CD_NM, FUEL_CD_NM, YRIDNW
        #            FROM
        #                "vsysb".STD_BD_CAR
        #            """
        # BD_CAR = self.dbUtil.export_to_pandas(sql_stmt)

        BD_CAR = self.CAR.copy()
        # ----------</BD_CAR>-------------

        # ----------<BD_CAR 조기폐차 컬럼 추가용>-------------
        BD_CAR_erase = BD_CAR[['VHRNO', 'VIN', 'VHCTY_CD_NM', 'EXHST_GAS_GRD_CD_NM', 'YRIDNW']]
        # ----------<BD_CAR 조기폐차 컬럼 추가용>-------------

        # ----------<BD_CAR 구매차량 컬럼 추가용>-------------
        BD_CAR_new = BD_CAR[
            ['VHRNO', 'VIN', 'FRST_REG_YMD', 'ACQS_YMD', 'VHCTY_CD_NM', 'EXHST_GAS_GRD_CD_NM', 'FUEL_CD_NM', 'YRIDNW']]
        # ----------<BD_CAR 구매차량 컬럼 추가용>-------------

        # ----------<저공해조치이력데이터>-------------
        # sql_stmt = """
        #            SELECT /*+  PARALLEL(2) */
        #                *
        #            FROM
        #                "vsysb".STD_BD_LWEM_ACTN_HST
        #             WHERE ACTN_SE = '조기폐차'
        #            """
        # BD_DLM_HIS = self.dbUtil.export_to_pandas(sql_stmt)

        BD_DLM_HIS = self.LWEM_ACTN_HST[self.LWEM_ACTN_HST['ACTN_SE'] == '조기폐차']
        # ----------</저공해조치이력데이터>-------------

        # ----------<조기폐차데이터>-------------
        early_erase = BD_DLM_HIS[BD_DLM_HIS['ACTN_SE'] == '조기폐차']

        early_erase_1 = early_erase[early_erase['PRGRS_STTS_CD'] == 'Y']
        # ----------</조기폐차데이터>-------------

        # ----------<조기폐차 수도권 여부 컬럼 추가>-------------
        early_erase_1['ELPDSRC_NCPITL_YN'] = 'N'
        early_erase_1.loc[early_erase_1['STDG_CTPV_NM'].isin(['경기도', '서울시', '인천시']), 'ELPDSRC_NCPITL_YN'] = 'Y'
        # ----------</조기폐차 수도권 여부 컬럼 추가>-------------

        # ----------<배출가스 등급 1~5 데이터만 추출>-------------
        early_erase_2 = pd.merge(early_erase_1, BD_CAR_erase, on=['VHRNO', 'VIN'], how='left')

        early_erase_3 = early_erase_2[early_erase_2['EXHST_GAS_GRD_CD_NM'].isin(['1', '2', '3', '4', '5'])]
        # ----------</ACTN_DE.notnull인 데이터만 추출>-------------

        # ----------<구매차량 BD_CAR 컬럼 추가>-------------
        BD_CAR_new.rename(columns={'VHRNO': 'PRCHS_VHCL_VHRNO', 'VIN': 'PRCHS_VHCL_VIN'}, inplace=True)

        early_erase_4 = pd.merge(early_erase_3, BD_CAR_new, on=['PRCHS_VHCL_VHRNO', 'PRCHS_VHCL_VIN'], how='left')
        # ----------</구매차량 BD_CAR 컬럼 추가>-------------

        # ----------<구매차량 신차 여부 컬럼 추가>-------------
        early_erase_4.loc[early_erase_4['PRCHS_VHCL_VHRNO'].notnull(), 'NCAR_YN'] = 'Y'

        early_erase_4['FRST_REG_YMD'] = pd.to_numeric(early_erase_4['FRST_REG_YMD'])
        early_erase_4['ACQS_YMD'] = pd.to_numeric(early_erase_4['ACQS_YMD'])

        condition = (early_erase_4['PRCHS_VHCL_VHRNO'].notnull()) & (
                early_erase_4['FRST_REG_YMD'] < early_erase_4['ACQS_YMD'])
        early_erase_4.loc[condition, 'NCAR_YN'] = 'N'
        # ----------</구매차량 신차 여부 컬럼 추가>-------------

        # ----------<컬럼명 표준화>-------------
        res = early_erase_4[
            ['VHRNO', 'VIN', 'ACTN_YMD', 'STDG_CTPV_NM', 'ELPDSRC_NCPITL_YN', 'VHCTY_CD_NM_x', 'YRIDNW_x',
             'PRCHS_VHCL_VHRNO', 'PRCHS_VHCL_VIN', 'ACQS_YMD', 'NCAR_YN', 'VHCTY_CD_NM_y', 'EXHST_GAS_GRD_CD_NM_y',
             'FUEL_CD_NM', 'YRIDNW_y']]

        res.rename(columns={'VHRNO': 'ELPDSRC_VHRNO', 'VIN': 'ELPDSRC_VIN', 'ACTN_YMD': 'ELPDSRC_YMD',
                            'STDG_CTPV_NM': 'ELPDSRC_CTPV', 'VHCTY_CD_NM_x': 'ELPDSRC_VHCTY',
                            'YRIDNW_x': 'ELPDSRC_VHCL_YRIDNW',
                            'ACQS_YMD': 'PRCHS_YMD', 'VHCTY_CD_NM_y': 'PRCHS_VHCL_VHCTY',
                            'EXHST_GAS_GRD_CD_NM_y': 'PRCHS_VHCL_EXHST_GAS_GRD', 'FUEL_CD_NM': 'PRCHS_VHCL_FUEL',
                            'YRIDNW_y': 'PRCHS_VHCL_YRIDNW'},
                   inplace=True)
        # ----------</컬럼명 표준화>-------------


        # ----------<데이터프레임을 DB에 저장>-------------
        table_nm = 'std_bd_elpdsrc_prchs_vhcl_anl'

        # 오늘 날짜 컬럼 추가
        res['LOAD_DT'] = datetime.date.today()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(res.columns):
            if 'float' in res[column].dtype.name:
                sql += column+' float'
            elif 'int' in res[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(res.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(res, table_nm)
        # ----------</데이터프레임을 DB에 저장>-------------

        # ----------<데이터프레임의 메모리해제>-------------
        del [[res]]
        # ----------<데이터프레임의 메모리해제>-------------

    def proj06_analysis_02(self):
        """
        (구현 분석가: 윤혜진)
        :return:
        """

        # ----------<BD_CAR>-------------
        # sql_stmt = """
        #            SELECT
        #                VHRNO, VIN, EXHST_GAS_GRD_CD_NM
        #            FROM
        #                "vsysb".STD_BD_CAR
        #            """
        # BD_CAR = self.dbUtil.export_to_pandas(sql_stmt)
        BD_CAR = self.CAR.copy()
        # ----------</BD_CAR>-------------

        # ----------<저공해조치이력데이터>-------------
        # sql_stmt = """
        #            SELECT /*+  PARALLEL(2) */
        #                *
        #            FROM
        #                "vsysb".STD_BD_LWEM_ACTN_HST
        #             WHERE ACTN_SE = '조기폐차'
        #            """
        # BD_DLM_HIS = self.dbUtil.export_to_pandas(sql_stmt)

        BD_DLM_HIS = self.LWEM_ACTN_HST.copy()
        # ----------</저공해조치이력데이터>-------------

        # ----------<조기폐차데이터>-------------
        early_erase = BD_DLM_HIS[BD_DLM_HIS['ACTN_SE'] == '조기폐차']

        early_erase_1 = early_erase[early_erase['PRGRS_STTS_CD'] == 'Y']
        # ----------</조기폐차데이터>-------------

        # ----------<조기폐차 수도권 데이터만 추출>-------------
        early_erase_2 = early_erase_1[early_erase_1['STDG_CTPV_NM'].isin(['경기도', '서울시', '인천시'])]
        # ----------</조기폐차 수도권 데이터만 추출>-------------

        # ----------<배출가스 등급 1~5 데이터만 추출>-------------
        early_erase_3 = pd.merge(early_erase_2, BD_CAR, on=['VHRNO', 'VIN'], how='left')

        early_erase_4 = early_erase_3[early_erase_3['EXHST_GAS_GRD_CD_NM'].isin(['1', '2', '3', '4', '5'])]
        # ----------</ACTN_DE.notnull인 데이터만 추출>-------------

        # ----------<연도 컬럼 생성>-------------
        early_erase_4['ACTN_YMD'] = early_erase_4['ACTN_YMD'].astype(int)
        early_erase_4['ACTN_YMD'] = early_erase_4['ACTN_YMD'].astype(str)

        early_erase_4['ACTN_YMD_year'] = early_erase_4['ACTN_YMD'].str.slice(start=0, stop=4)
        # ----------</연도 컬럼 생성>-------------

        # ----------<조기폐차 연도별 차량 수>-------------
        res_1 = pd.DataFrame(early_erase_4.groupby('ACTN_YMD_year').size(), columns={'count'}).reset_index()
        # ----------</조기폐차 연도별 차량 수>-------------

        # ----------<저감장치부착데이터>-------------
        att = BD_DLM_HIS[BD_DLM_HIS['ACTN_SE'] == '저감장치부착']
        # ----------</저감장치부착데이터>-------------

        # ----------<ACTN_YMD.notnull인 데이터만 추출>-------------
        att_1 = att[att['ACTN_YMD'].notnull()]
        # ----------</ACTN_YMD.notnull인 데이터만 추출>-------------

        # ----------<연도 컬럼 생성>-------------
        idx = att_1[att_1['ACTN_YMD'] == '010-7963'].index
        att_2 = att_1.drop(idx)

        idx = att_2[att_2['ACTN_YMD'] == '2021-04-'].index
        att_2.drop(idx, inplace=True)

        idx = att_2[att_2['ACTN_YMD'] == '2022-01-'].index
        att_2.drop(idx, inplace=True)

        att_2['ACTN_YMD'] = att_2['ACTN_YMD'].astype(int)
        att_2['ACTN_YMD'] = att_2['ACTN_YMD'].astype(str)

        att_2['ACTN_YMD_year'] = att_2['ACTN_YMD'].str.slice(start=0, stop=4)
        # ----------</연도 컬럼 생성>-------------

        # ----------<저감장치부착 연도별 차량 수>-------------
        res_2 = pd.DataFrame(att_2.groupby('ACTN_YMD_year').size(), columns={'count'}).reset_index()
        # ----------</저감장치부착 연도별 차량 수>-------------

        # ----------<최종테이블 생성>-------------
        res = pd.merge(res_2, res_1, on=['ACTN_YMD_year'], how='left')
        # ----------</최종테이블 생성>-------------

        # ----------<NULL 값 0으로 채우기>-------------
        res.loc[res['count_y'].isnull(), 'count_y'] = 0
        # ----------</NULL 값 0으로 채우기>-------------

        # ----------<저공해조치 합계 컬럼 생성>-------------
        res['sum'] = res['count_x'] + res['count_y']
        # ----------</저공해조치 합계 컬럼 생성>-------------

        # ----------<조기폐차 비율 컬럼 생성>-------------
        res['percent'] = round((res['count_y'] / res['sum']) * 100)
        # ----------</조기폐차 비율 컬럼 생성>-------------

        # ----------<컬럼명 표준화>-------------
        res.rename(
            columns={'ACTN_YMD_year': 'ACTN_YR', 'count_x': 'RDCDVC_EXTRNS_VHCL_CNT', 'count_y': 'ELPDSRC_VHCL_CNT',
                     'sum': 'LEM_VHCL_CNT', 'percent': 'ELPDSRC_RT'}, inplace=True)
        # ----------</컬럼명 표준화>-------------

        # ----------<데이터프레임을 DB에 저장>-------------
        table_nm = 'std_bd_elpdsrc_actn_yr_curstt'

        # 오늘 날짜 컬럼 추가
        res['LOAD_DT'] = datetime.date.today()
        
         # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(res.columns):
            if 'float' in res[column].dtype.name:
                sql += column+' float'
            elif 'int' in res[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(res.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)


        self.dbUtil2.import_from_pandas(res, table_nm)
        # ----------</데이터프레임을 DB에 저장>-------------

        # ----------<데이터프레임의 메모리해제>-------------
        del [[res]]
        # ----------<데이터프레임의 메모리해제>-------------

    def proj06_analysis_03(self):
        """
        (구현 분석가: 윤혜진)
        :return:
        """

        # ----------<BD_CAR>-------------
        # sql_stmt = """
        #             SELECT /*+  PARALLEL(2) */
        #                 VHRNO, VIN, VHCL_ERSR_YN, EXHST_GAS_GRD_CD_NM, VHCTY_CD_NM, FUEL_CD_NM
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             """
        # BD_CAR = self.dbUtil.export_to_pandas(sql_stmt)
        BD_CAR = self.CAR.copy()
        # ----------</BD_CAR>-------------

        # ----------<운행 중인 BD_CAR 5등급 경유 차량 추출>-------------
        BD_CAR_5_DS = BD_CAR[
            (BD_CAR['VHCL_ERSR_YN'] == 'N') & (BD_CAR['EXHST_GAS_GRD_CD_NM'] == '5') & (BD_CAR['FUEL_CD_NM'] == '경유')][
            ['VHRNO', 'VIN', 'EXHST_GAS_GRD_CD_NM', 'VHCTY_CD_NM', 'FUEL_CD_NM']]

        BD_CAR_1 = BD_CAR[['VHRNO', 'VIN', 'EXHST_GAS_GRD_CD_NM', 'VHCTY_CD_NM', 'FUEL_CD_NM']]
        # ----------</운행 중인 BD_CAR 5등급 경유 차량 추출>-------------

        # ----------<저공해조치이력데이터>-------------
        # sql_stmt = """
        #             SELECT /*+  PARALLEL(2) */
        #                 *
        #             FROM
        #                 "vsysb".STD_BD_LWEM_ACTN_HST
        #             WHERE
        #                 ACTN_SE = '저감장치부착'
        #             """
        # BD_DLM_HIS = self.dbUtil.export_to_pandas(sql_stmt)
        BD_DLM_HIS = self.LWEM_ACTN_HST[self.LWEM_ACTN_HST['ACTN_SE'] == '저감장치부착']
        # ----------</저공해조치이력데이터>-------------

        # ----------<저감장치 부착 데이터>-------------
        att = BD_DLM_HIS[BD_DLM_HIS['ACTN_SE'] == '저감장치부착']
        # ----------</저감장치 부착 데이터>-------------

        # ----------<저감장치 구분 + 종류 컬럼 생성>-------------
        att['rdcdvc_knd'] = att['LEM_KND_CD_NM'] + ' ' + att['ACTN_KND']
        # ----------</저감장치 구분 + 종류 컬럼 생성>-------------

        # ----------<가장 최근 저감장치 부착 데이터만 남기고 중복 제거>-------------
        idx = att[att['ACTN_YMD'] == '010-7963'].index
        att_1 = att.drop(idx)

        idx = att_1[att_1['ACTN_YMD'] == '2021-04-'].index
        att_1.drop(idx, inplace=True)

        idx = att_1[att_1['ACTN_YMD'] == '2022-01-'].index
        att_1.drop(idx, inplace=True)

        att_1['ACTN_YMD'] = pd.to_numeric(att_1['ACTN_YMD'])

        att_2 = att_1[['REDUC_REG_NO', 'VHRNO', 'VIN', 'ENTRPS_NM', 'ACTN_YMD', 'rdcdvc_knd']].sort_values('ACTN_YMD')
        att_2.drop_duplicates(['VHRNO', 'VIN'], keep='last', inplace=True)
        
        # ----------<가장 최근 저감장치 부착 데이터만 남기고 중복 제거>-------------
        
        # 2022-10-25
        # ----------<검사이력데이터>-------------
        # sql_stmt = """
        #             SELECT /*+  PARALLEL(2) */
        #                 VHRNO, VIN, INSP_SE, INSP_MSRMT_MTHD, INSP_YMD, INSP_PASAGE_YN, SMO_1_ITM_MEVLU, SMO_1_ITM_PRMT_VAL, SMO_1_ITM_JGMT_YN,
        #                 CO_1_ITM_MEVLU, CO_1_ITM_PRMT_VAL, CO_1_ITM_JGMT_YN, HC_1_ITM_MEVLU, HC_1_ITM_PRMT_VAL, HC_1_ITM_JGMT_YN,
        #                 NOX_MEVLU, NOX_PRMT_VAL, NOX_JGMT_YN
        #             FROM
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 INSP_SE IN ('정기검사', '정밀검사')
        #             """
        # BD_INSP_HIS = self.dbUtil.export_to_pandas(sql_stmt)
        BD_INSP_HIS = self.INSP[((self.INSP['INSP_SE'] == '정기검사') | (self.INSP['INSP_SE'] == '정밀검사'))].copy()
        # ----------</검사이력데이터>-------------

        # ----------<1. 측정값이 존재하는 데이터만 추출>-------------
        jgjm_att_1 = BD_INSP_HIS[BD_INSP_HIS['SMO_1_ITM_MEVLU'].notnull()]
        # ----------</1. 측정값이 존재하는 데이터만 추출>-------------

        # ----------<2. 판정이 모두 Y인 데이터만 추출>-------------
        jgjm_att_2 = jgjm_att_1[(jgjm_att_1['INSP_PASAGE_YN'] == 'Y') & (jgjm_att_1['SMO_1_ITM_JGMT_YN'] == 'Y')]
        # ----------</2. 판정이 모두 Y인 데이터만 추출>-------------

        # ----------<3. 측정값 < 허용치인 데이터만 추출>-------------
        jgjm_att_2['SMO_1_ITM_MEVLU'] = pd.to_numeric(jgjm_att_2['SMO_1_ITM_MEVLU'])
        jgjm_att_2['SMO_1_ITM_PRMT_VAL'] = pd.to_numeric(jgjm_att_2['SMO_1_ITM_PRMT_VAL'])

        jgjm_att_3 = jgjm_att_2[jgjm_att_2['SMO_1_ITM_MEVLU'] <= jgjm_att_2['SMO_1_ITM_PRMT_VAL']]
        # ----------</3. 측정값 < 허용치인 데이터만 추출>-------------

        # ----------<4. 같은 일자에 여러번 합격받은 중복 제거>-------------
        jgjm_att_4 = jgjm_att_3.sort_values('INSP_YMD')
        jgjm_att_4.drop_duplicates(['VHRNO', 'VIN', 'INSP_YMD'], keep='last', inplace=True)
        # ----------</4. 같은 일자에 여러번 합격받은 중복 제거>-------------

        # ----------<5등급 경유 저공해 미조치 차량 추출>-------------
        BD_CAR_5_DS_1 = pd.merge(BD_CAR_5_DS, att_2, on=['VHRNO', 'VIN'], how='left')
        BD_CAR_5_DS_not_att = BD_CAR_5_DS_1[BD_CAR_5_DS_1['REDUC_REG_NO'].isnull()]
        # ----------</5등급 경유 저공해 미조치 차량 추출>-------------

        # ----------<5등급 경유 저공해 미조치 차량 매연 평균 측정치>-------------
        BD_CAR_5_DS_not_att_1 = pd.merge(BD_CAR_5_DS_not_att, jgjm_att_4, on=['VHRNO', 'VIN'])

        BD_CAR_5_DS_not_att_2 = BD_CAR_5_DS_not_att_1.sort_values('INSP_YMD')
        BD_CAR_5_DS_not_att_2.drop_duplicates(['VHRNO', 'VIN'], keep='last', inplace=True)

        BD_CAR_5_DS_not_att_2['SMO_1_ITM_MEVLU'] = pd.to_numeric(BD_CAR_5_DS_not_att_2['SMO_1_ITM_MEVLU'])
        res_1 = pd.DataFrame(
            BD_CAR_5_DS_not_att_2[['VHCTY_CD_NM', 'SMO_1_ITM_MEVLU']].groupby('VHCTY_CD_NM')['SMO_1_ITM_MEVLU'].agg(
                ['mean'])).reset_index()
        # ----------</5등급 경유 저공해 미조치 차량 매연 평균 측정치>-------------

        # ----------<저감장치부착 차량 매연 평균 측정치>-------------
        att_3 = pd.merge(att_2, BD_CAR_1, on=['VHRNO', 'VIN'], how='left')

        BD_CAR_5_DS_att_1 = pd.merge(att_3, jgjm_att_4, on=['VHRNO', 'VIN'])
        BD_CAR_5_DS_att_2 = BD_CAR_5_DS_att_1[BD_CAR_5_DS_att_1['rdcdvc_knd'].notnull()]
        # ----------</저감장치부착 차량 매연 평균 측정치>-------------

        # ----------<장치부착 이전 데이터>-------------
        BD_CAR_5_DS_att_2['INSP_YMD'] = pd.to_numeric(BD_CAR_5_DS_att_2['INSP_YMD'])
        BD_CAR_5_DS_att_2['ACTN_YMD'] = pd.to_numeric(BD_CAR_5_DS_att_2['ACTN_YMD'])
        att_before = BD_CAR_5_DS_att_2[BD_CAR_5_DS_att_2['INSP_YMD'] < BD_CAR_5_DS_att_2['ACTN_YMD']]

        att_before_1 = att_before.sort_values('INSP_YMD')
        att_before_1.drop_duplicates(['VHRNO', 'VIN'], keep='last', inplace=True)
        # ----------</장치부착 이전 데이터>-------------

        # ----------<장치부착 이후 데이터>-------------
        att_after = BD_CAR_5_DS_att_2[BD_CAR_5_DS_att_2['INSP_YMD'] > BD_CAR_5_DS_att_2['ACTN_YMD']]

        att_after_1 = att_after.sort_values('INSP_YMD')
        att_after_1.drop_duplicates(['VHRNO', 'VIN'], keep='first', inplace=True)
        # ----------</장치부착 이후 데이터>-------------

        # ----------<부착 이후 가장 최근 데이터>-------------
        att_after_2 = att_after.sort_values('INSP_YMD')
        att_after_2.drop_duplicates(['VHRNO', 'VIN'], keep='last', inplace=True)
        # ----------</부착 이후 가장 최근 데이터>-------------

        # ----------<장치부착 이전 + 이후 데이터>-------------
        att_before_1_1 = att_before_1[['VHRNO', 'VIN']]
        att_after_1_1 = att_after_1[['VHRNO', 'VIN']]
        df_sel = pd.merge(att_before_1_1, att_after_1_1, on=['VHRNO', 'VIN'])
        # ----------</장치부착 이전 + 이후 데이터>-------------

        # ----------<장치부착 직전 데이터>-------------
        att_before_2 = pd.merge(att_before_1, df_sel, on=['VHRNO', 'VIN'])

        att_before_3 = att_before_2[
            ['VHRNO', 'VIN', 'VHCTY_CD_NM', 'ACTN_YMD', 'rdcdvc_knd', 'INSP_YMD', 'SMO_1_ITM_MEVLU']]
        att_before_3.rename(columns={'INSP_YMD': 'INT_CHK_DT_1', 'SMO_1_ITM_MEVLU': 'SMOKE_FST_VAL_1'}, inplace=True)
        # ----------</장치부착 직전 데이터>-------------

        # ----------<장치부착 직후 데이터>-------------
        att_after_3 = pd.merge(att_after_1, df_sel, on=['VHRNO', 'VIN'])

        att_after_5 = att_after_3[['VHRNO', 'VIN', 'INSP_YMD', 'SMO_1_ITM_MEVLU']]
        att_after_5.rename(columns={'INSP_YMD': 'INT_CHK_DT_2', 'SMO_1_ITM_MEVLU': 'SMOKE_FST_VAL_2'}, inplace=True)
        # ----------</장치부착 직후 데이터>-------------

        # ----------<장치부착 이후 가장 최근 데이터>-------------
        att_after_4 = pd.merge(att_after_2, df_sel, on=['VHRNO', 'VIN'])

        att_after_6 = att_after_4[['VHRNO', 'VIN', 'INSP_YMD', 'SMO_1_ITM_MEVLU']]
        att_after_6.rename(columns={'INSP_YMD': 'INT_CHK_DT_3', 'SMO_1_ITM_MEVLU': 'SMOKE_FST_VAL_3'}, inplace=True)
        # ----------</장치부착 이후 가장 최근 데이터>-------------

        # ----------<직전 + 직후 + 가장 최근>-------------
        att_insp_all = pd.merge(att_before_3, att_after_5, on=['VHRNO', 'VIN'], how='left')
        att_insp_all_1 = pd.merge(att_insp_all, att_after_6, on=['VHRNO', 'VIN'], how='left')
        # ----------</직전 + 직후 + 가장 최근>-------------

        # ----------<저감장치별>-------------
        att_insp_all_1['SMOKE_FST_VAL_1'] = pd.to_numeric(att_insp_all_1['SMOKE_FST_VAL_1'])
        att_insp_all_1['SMOKE_FST_VAL_2'] = pd.to_numeric(att_insp_all_1['SMOKE_FST_VAL_2'])
        att_insp_all_1['SMOKE_FST_VAL_3'] = pd.to_numeric(att_insp_all_1['SMOKE_FST_VAL_3'])

        rdcdvc_1 = pd.DataFrame(
            att_insp_all_1[['rdcdvc_knd', 'SMOKE_FST_VAL_1', 'SMOKE_FST_VAL_2', 'SMOKE_FST_VAL_3']].groupby(
                'rdcdvc_knd').mean()).reset_index()
        rdcdvc_2 = pd.DataFrame(att_insp_all_1.groupby('rdcdvc_knd').size(), columns={'count'}).reset_index()

        rdcdvc_res_1 = pd.merge(rdcdvc_1, rdcdvc_2, on=['rdcdvc_knd'], how='left')

        rdcdvc_res_1['RSLT_SE1'] = '저감장치별'

        rdcdvc_res_1.rename(columns={'rdcdvc_knd': 'RSLT_SE2', 'SMOKE_FST_VAL_1': 'ACTN_BF_SMO_AVRG_MEVLU',
                                     'SMOKE_FST_VAL_2': 'ACTN_AFTR_SMO_AVRG_MEVLU',
                                     'SMOKE_FST_VAL_3': 'RCNT_SMO_AVRG_MEVLU', 'count': 'VHCL_CNT'}, inplace=True)
        # ----------</저감장치별>-------------

        # ----------<차종별>-------------
        vhcty_1 = pd.DataFrame(
            att_insp_all_1[['VHCTY_CD_NM', 'SMOKE_FST_VAL_1', 'SMOKE_FST_VAL_2', 'SMOKE_FST_VAL_3']].groupby(
                'VHCTY_CD_NM').mean()).reset_index()
        vhcty_2 = pd.DataFrame(att_insp_all_1.groupby('VHCTY_CD_NM').size(), columns={'count'}).reset_index()

        vhcty_res_1 = pd.merge(res_1, vhcty_1, on=['VHCTY_CD_NM'], how='left')
        vhcty_res_2 = pd.merge(vhcty_res_1, vhcty_2, on=['VHCTY_CD_NM'], how='left')

        vhcty_res_2['RSLT_SE1'] = '차종별'

        vhcty_res_2.rename(columns={'VHCTY_CD_NM': 'RSLT_SE2', 'mean': 'UNAT_SMO_AVRG_MEVLU',
                                    'SMOKE_FST_VAL_1': 'ACTN_BF_SMO_AVRG_MEVLU',
                                    'SMOKE_FST_VAL_2': 'ACTN_AFTR_SMO_AVRG_MEVLU',
                                    'SMOKE_FST_VAL_3': 'RCNT_SMO_AVRG_MEVLU', 'count': 'VHCL_CNT'}, inplace=True)
        # ----------</차종별>-------------

        # ----------<차종별 증가>-------------
        att_after_0 = att_after[['VHRNO', 'VIN', 'VHCTY_CD_NM', 'ACTN_YMD', 'INSP_YMD', 'SMO_1_ITM_MEVLU']]
        att_after_sel = pd.merge(att_after_0, df_sel, on=['VHRNO', 'VIN'])

        att_before_3_1 = att_before_3[['VHRNO', 'VIN', 'INT_CHK_DT_1', 'SMOKE_FST_VAL_1']]
        att_after_sel_1 = pd.merge(att_after_sel, att_before_3_1, on=['VHRNO', 'VIN'])

        att_after_up = att_after_sel_1[att_after_sel_1['SMOKE_FST_VAL_1'] <= att_after_sel_1['SMO_1_ITM_MEVLU']]

        att_after_up_1 = att_after_up.sort_values('INSP_YMD')
        att_after_up_1.drop_duplicates(['VHRNO', 'VIN'], keep='first', inplace=True)

        att_after_up_1['SMO_1_ITM_MEVLU'] = pd.to_numeric(att_after_up_1['SMO_1_ITM_MEVLU'])

        vhcty_up_1 = pd.DataFrame(
            att_after_up_1[['VHCTY_CD_NM', 'SMO_1_ITM_MEVLU']].groupby('VHCTY_CD_NM').mean()).reset_index()

        vhcty_a = pd.DataFrame(att_insp_all_1[['VHCTY_CD_NM', 'SMOKE_FST_VAL_1', 'SMOKE_FST_VAL_2']].groupby(
            'VHCTY_CD_NM').mean()).reset_index()

        vhcty_up_2 = pd.merge(vhcty_a, vhcty_up_1, on=['VHCTY_CD_NM'], how='left')
        # ----------</차종별 증가>-------------

        # ----------<차종별 증가 일수 계산>-------------
        att_after_up_1['ACTN_YMD'] = att_after_up_1['ACTN_YMD'].astype(int)
        att_after_up_1['ACTN_YMD'] = att_after_up_1['ACTN_YMD'].astype(str)
        att_after_up_1['INSP_YMD'] = att_after_up_1['INSP_YMD'].astype(int)
        att_after_up_1['INSP_YMD'] = att_after_up_1['INSP_YMD'].astype(str)

        idx = att_after_up_1[att_after_up_1['ACTN_YMD'] == '20202018'].index
        att_after_up_2 = att_after_up_1.drop(idx)

        att_after_up_2['ACTN_YMD_00'] = pd.to_datetime(att_after_up_2['ACTN_YMD'])
        att_after_up_2['INSP_YMD_00'] = pd.to_datetime(att_after_up_2['INSP_YMD'])

        att_after_up_2['PERIOD'] = att_after_up_2['INSP_YMD_00'] - att_after_up_2['ACTN_YMD_00']
        att_after_up_2['PERIOD_00'] = att_after_up_2['PERIOD'].astype(str)
        att_after_up_2['PERIOD_day'] = att_after_up_2['PERIOD_00'].str.extract(r'(\d+)')
        att_after_up_2['PERIOD_day'] = att_after_up_2['PERIOD_day'].astype(int)

        vhcty_up_day_1 = pd.DataFrame(
            att_after_up_2[['VHCTY_CD_NM', 'PERIOD_day']].groupby('VHCTY_CD_NM').sum()).reset_index()
        vhcty_up_day_2 = pd.DataFrame(att_after_up_2.groupby('VHCTY_CD_NM').size(), columns={'count'}).reset_index()
        vhcty_up_day_3 = pd.DataFrame(
            att_after_up_2[['VHCTY_CD_NM', 'PERIOD_day']].groupby('VHCTY_CD_NM').mean()).reset_index()

        vhcty_up_res_1 = pd.merge(vhcty_up_2, vhcty_up_day_1, on=['VHCTY_CD_NM'], how='left')
        vhcty_up_res_2 = pd.merge(vhcty_up_res_1, vhcty_up_day_2, on=['VHCTY_CD_NM'], how='left')
        vhcty_up_res_3 = pd.merge(vhcty_up_res_2, vhcty_up_day_3, on=['VHCTY_CD_NM'], how='left')

        vhcty_up_res_3['RSLT_SE1'] = '차종별 증가'

        vhcty_up_res_3.rename(columns={'VHCTY_CD_NM': 'RSLT_SE2', 'SMOKE_FST_VAL_1': 'ACTN_BF_SMO_AVRG_MEVLU',
                                       'SMOKE_FST_VAL_2': 'ACTN_AFTR_SMO_AVRG_MEVLU',
                                       'SMO_1_ITM_MEVLU': 'RCNT_SMO_AVRG_MEVLU', 'PERIOD_day_x': 'DYCN_TOTL_SUM',
                                       'count': 'VHCL_CNT', 'PERIOD_day_y': 'DYCN_AVRG'}, inplace=True)
        # ----------</차종별 증가 일수 계산>-------------

        # ----------<차종별 감소>-------------
        att_after_down = att_after_sel_1[att_after_sel_1['SMOKE_FST_VAL_1'] > att_after_sel_1['SMO_1_ITM_MEVLU']]

        att_after_down_1 = att_after_down.sort_values('INSP_YMD')
        att_after_down_1.drop_duplicates(['VHRNO', 'VIN'], keep='first', inplace=True)

        att_after_up_1_1 = att_after_up_1[['VHRNO', 'VIN']]
        att_after_up_1_1['Y'] = 'Y'

        att_after_down_2 = pd.merge(att_after_down_1, att_after_up_1_1, on=['VHRNO', 'VIN'], how='left')
        att_after_down_3 = att_after_down_2[att_after_down_2['Y'].isnull()]

        att_after_down_3['SMO_1_ITM_MEVLU'] = pd.to_numeric(att_after_down_3['SMO_1_ITM_MEVLU'])
        vhcty_down_1 = pd.DataFrame(
            att_after_down_3[['VHCTY_CD_NM', 'SMO_1_ITM_MEVLU']].groupby('VHCTY_CD_NM').mean()).reset_index()

        vhcty_down_2 = pd.merge(vhcty_a, vhcty_down_1, on=['VHCTY_CD_NM'], how='left')
        # ----------</차종별 감소>-------------

        # ----------<차종별 감소 일수 계산>-------------
        att_after_down_3['ACTN_YMD'] = att_after_down_3['ACTN_YMD'].astype(int)
        att_after_down_3['ACTN_YMD'] = att_after_down_3['ACTN_YMD'].astype(str)
        att_after_down_3['INSP_YMD'] = att_after_down_3['INSP_YMD'].astype(int)
        att_after_down_3['INSP_YMD'] = att_after_down_3['INSP_YMD'].astype(str)

        att_after_down_3['ACTN_YMD_00'] = pd.to_datetime(att_after_down_3['ACTN_YMD'])
        att_after_down_3['INSP_YMD_00'] = pd.to_datetime(att_after_down_3['INSP_YMD'])

        att_after_down_3['PERIOD'] = att_after_down_3['INSP_YMD_00'] - att_after_down_3['ACTN_YMD_00']
        att_after_down_3['PERIOD_00'] = att_after_down_3['PERIOD'].astype(str)
        att_after_down_3['PERIOD_day'] = att_after_down_3['PERIOD_00'].str.extract(r'(\d+)')
        att_after_down_3['PERIOD_day'] = att_after_down_3['PERIOD_day'].astype(int)

        vhcty_down_day_1 = pd.DataFrame(
            att_after_down_3[['VHCTY_CD_NM', 'PERIOD_day']].groupby('VHCTY_CD_NM').sum()).reset_index()
        vhcty_down_day_2 = pd.DataFrame(att_after_down_3.groupby('VHCTY_CD_NM').size(), columns={'count'}).reset_index()
        vhcty_down_day_3 = pd.DataFrame(
            att_after_down_3[['VHCTY_CD_NM', 'PERIOD_day']].groupby('VHCTY_CD_NM').mean()).reset_index()

        vhcty_down_res_1 = pd.merge(vhcty_down_2, vhcty_down_day_1, on=['VHCTY_CD_NM'], how='left')
        vhcty_down_res_2 = pd.merge(vhcty_down_res_1, vhcty_down_day_2, on=['VHCTY_CD_NM'], how='left')
        vhcty_down_res_3 = pd.merge(vhcty_down_res_2, vhcty_down_day_3, on=['VHCTY_CD_NM'], how='left')

        vhcty_down_res_3['RSLT_SE1'] = '차종별 감소'

        vhcty_down_res_3.rename(columns={'VHCTY_CD_NM': 'RSLT_SE2', 'SMOKE_FST_VAL_1': 'ACTN_BF_SMO_AVRG_MEVLU',
                                         'SMOKE_FST_VAL_2': 'ACTN_AFTR_SMO_AVRG_MEVLU',
                                         'SMO_1_ITM_MEVLU': 'RCNT_SMO_AVRG_MEVLU', 'PERIOD_day_x': 'DYCN_TOTL_SUM',
                                         'count': 'VHCL_CNT', 'PERIOD_day_y': 'DYCN_AVRG'}, inplace=True)
        # ----------</차종별 감소 일수 계산>-------------

        # ----------<엔진개조 데이터>-------------
        engine = BD_DLM_HIS[(BD_DLM_HIS['ACTN_SE'] == '저감장치부착') & (BD_DLM_HIS['LEM_KND_CD_NM'] == '엔진개조')]

        engine_1 = pd.merge(engine, BD_CAR_1, on=['VHRNO', 'VIN'], how='left')

        engine_2 = engine_1[['VHRNO', 'VIN', 'ACTN_YMD', 'LEM_KND_CD_NM', 'EXHST_GAS_GRD_CD_NM', 'VHCTY_CD_NM',
                             'FUEL_CD_NM']].drop_duplicates(['VHRNO', 'VIN'], keep='first')
        # ----------</엔진개조 데이터>-------------

        # ----------<같은 일자에 여러번 검사받은 중복 데이터 제거>-------------
        jgjm_engine_1 = BD_INSP_HIS.sort_values('INSP_YMD')
        jgjm_engine_1.drop_duplicates(['VHRNO', 'VIN', 'INSP_YMD'], keep='last', inplace=True)
        # ----------<같은 일자에 여러번 검사받은 중복 데이터 제거>-------------

        # ----------<엔진개조 + 검사데이터>-------------
        engine_jgjm_1 = pd.merge(engine_2, jgjm_engine_1, on=['VHRNO', 'VIN'])
        # ----------</엔진개조 + 검사데이터>-------------

        # ----------<엔진개조 후 검사데이터>-------------
        engine_after = engine_jgjm_1[engine_jgjm_1['INSP_YMD'] > engine_jgjm_1['ACTN_YMD']]

        engine_after_1 = engine_after[
            (engine_after['SMO_1_ITM_MEVLU'].notnull()) & (engine_after['SMO_1_ITM_JGMT_YN'].notnull()) | (
                engine_after['CO_1_ITM_MEVLU'].notnull()) & (engine_after['CO_1_ITM_JGMT_YN'].notnull())
            | (engine_after['HC_1_ITM_MEVLU'].notnull()) & (engine_after['HC_1_ITM_JGMT_YN'].notnull()) | (
                engine_after['NOX_MEVLU'].notnull()) & (engine_after['NOX_JGMT_YN'].notnull())]
        # ----------</엔진개조 후 검사데이터>-------------

        # ----------<차량별 검사데이터 개수 파악>-------------
        engine_after_1['COUNT_ALL'] = engine_after_1.groupby(['VHRNO', 'VIN'])['INSP_YMD'].transform('count')

        engine_after_2 = engine_after_1.drop_duplicates(['VHRNO', 'VIN'])
        # ----------</차량별 검사데이터 개수 파악>-------------

        # ----------<불합격 횟수 파악>-------------
        engine_after_1_N = engine_after_1[
            (engine_after_1['SMO_1_ITM_JGMT_YN'] == 'N') | (engine_after_1['CO_1_ITM_JGMT_YN'] == 'N') | (
                    engine_after_1['HC_1_ITM_JGMT_YN'] == 'N') | (engine_after_1['NOX_JGMT_YN'] == 'N')]

        engine_after_1_N['COUNT_N'] = engine_after_1_N.groupby(['VHRNO', 'VIN'])['INSP_YMD'].transform('count')

        engine_after_1_N_1 = engine_after_1_N[['VHRNO', 'VIN', 'COUNT_N']].drop_duplicates(['VHRNO', 'VIN'])
        # ----------</불합격 횟수 파악>-------------

        # ----------<불합격 횟수 컬럼 추가>-------------
        engine_after_3 = pd.merge(engine_after_2, engine_after_1_N_1, on=['VHRNO', 'VIN'], how='left')

        engine_after_3['PASS_YN'] = 'Y'
        engine_after_3.loc[engine_after_3['COUNT_N'].notnull(), 'PASS_YN'] = 'N'

        engine_res_1 = pd.DataFrame(engine_after_3.groupby(['VHCTY_CD_NM', 'PASS_YN']).size(),
                                    columns={'count'}).reset_index()

        engine_res_1['RSLT_SE1'] = '엔진개조'
        engine_res_1.rename(columns={'VHCTY_CD_NM': 'RSLT_SE2', 'count': 'VHCL_CNT'}, inplace=True)
        # ----------</불합격 횟수 컬럼 추가>-------------

        # ----------<최종 테이블 생성>-------------
        final_res = pd.concat([rdcdvc_res_1, vhcty_res_2, vhcty_up_res_3, vhcty_down_res_3, engine_res_1])
        # ----------</최종 테이블 생성>-------------

        # ----------<데이터프레임을 DB에 저장>-------------
        table_nm = 'std_bd_rdcdvc_efcny_anl'

        # 오늘 날짜 컬럼 추가
        final_res['LOAD_DT'] = datetime.date.today()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(final_res.columns):
            if 'float' in final_res[column].dtype.name:
                sql += column+' float'
            elif 'int' in final_res[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(final_res.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(final_res, table_nm)
        # ----------</데이터프레임을 DB에 저장>-------------

        # ----------<데이터프레임의 메모리해제>-------------
        del [[final_res]]
        # ----------<데이터프레임의 메모리해제>-------------


    def proj06_analysis_04(self):
        """
        (구현 분석가: 윤혜진)
        :return:
        """

        # ----------<BD_CAR>-------------
        # sql_stmt = """
        #             SELECT /*+  PARALLEL(2) */
        #                 VHRNO, VIN, VHCTY_CD_NM
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             """
        # BD_CAR = self.dbUtil.export_to_pandas(sql_stmt)
        BD_CAR = self.CAR.copy()
        # ----------</BD_CAR>-------------

        # ----------<저공해조치이력데이터>-------------
        # sql_stmt = """
        #             SELECT /*+  PARALLEL(2) */
        #                 *
        #             FROM
        #                 "vsysb".STD_BD_LWEM_ACTN_HST
        #             WHERE
        #                 ACTN_SE = '저감장치클리닝'
        #             """
        # BD_DLM_HIS = self.dbUtil.export_to_pandas(sql_stmt)
        BD_DLM_HIS = self.LWEM_ACTN_HST[self.LWEM_ACTN_HST['ACTN_SE'] == '저감장치클리닝'].copy()
        # ----------</저공해조치이력데이터>-------------

        # ----------<클리닝데이터만 추출>-------------
        cleaning = BD_DLM_HIS[BD_DLM_HIS['ACTN_SE'] == '저감장치클리닝'][
            ['REDUC_REG_NO', 'VHRNO', 'VIN', 'ENTRPS_NM', 'ACTN_YMD', 'ACTN_KND', 'DRVNG_DSTNC', 'SBSIDY_TRGT_YN',
             'PRGRS_STTS_CD', 'VENTL_PSSR', 'ACTN_SE', 'BF_SMO_CCNTRA', 'PST_SMO_CCNTRA', 'BF_VENTL_PSSR',
             'CLNING_GENTE_WHL_SE_CD_NM']]
        # ----------</클리닝데이터만 추출>-------------

        # ----------<BD_CAR 컬럼 추가>-------------
        cleaning_1 = pd.merge(cleaning, BD_CAR, on=['VHRNO', 'VIN'], how='left')
        # ----------</BD_CAR 컬럼 추가>-------------

        # ----------<클리닝 전체 count>-------------
        cleaning_1['COUNT'] = cleaning_1.groupby(['VHRNO', 'VIN'])['REDUC_REG_NO'].transform('count')
        # ----------</클리닝 전체 count>-------------

        # ----------<보증기간내 count>-------------
        cleaning_1['COUNT_IN'] = \
            cleaning_1[cleaning_1['CLNING_GENTE_WHL_SE_CD_NM'] == '보증기간내'].groupby(['REDUC_REG_NO'])[
                'ACTN_YMD'].transform(
                'count')
        # ----------</보증기간내 count>-------------

        # ----------<보증기간경과 count>-------------
        cleaning_1['COUNT_OUT'] = \
            cleaning_1[cleaning_1['CLNING_GENTE_WHL_SE_CD_NM'] == '보증기간경과'].groupby(['REDUC_REG_NO'])[
                'ACTN_YMD'].transform(
                'count')
        # ----------</보증기간경과 count>-------------

        # ----------<보증기간내 데이터만 추출>-------------
        cleaning_grnty_in = cleaning_1[cleaning_1['CLNING_GENTE_WHL_SE_CD_NM'] == '보증기간내']

        cleaning_grnty_in['ACTN_YMD'] = pd.to_numeric(cleaning_grnty_in['ACTN_YMD'])
        cleaning_grnty_in_1 = cleaning_grnty_in.sort_values('ACTN_YMD')
        cleaning_grnty_in_1.drop_duplicates(['VHRNO', 'VIN'], keep='last', inplace=True)
        # ----------</보증기간내 데이터만 추출>-------------

        # ----------<보증기간경과 데이터 추출>-------------
        cleaning_grnty_out = cleaning_1[cleaning_1['CLNING_GENTE_WHL_SE_CD_NM'] == '보증기간경과'][
            ['VHRNO', 'VIN', 'COUNT_OUT']].drop_duplicates(['VHRNO', 'VIN'])
        # ----------</보증기간경과 데이터 추출>-------------

        # ----------<보증기간경과 여부 확인>-------------
        cleaning_grnty_in_2 = pd.merge(cleaning_grnty_in_1, cleaning_grnty_out, on=['VHRNO', 'VIN'], how='left')

        cleaning_grnty_in_2['GRNTE_WHL_AFTR_CLNING_YN'] = 'N'
        cleaning_grnty_in_2.loc[cleaning_grnty_in_2['COUNT_OUT_y'].notnull(), 'GRNTE_WHL_AFTR_CLNING_YN'] = 'Y'

        res_1 = pd.DataFrame(
            cleaning_grnty_in_2.groupby(['COUNT_IN', 'VHCTY_CD_NM', 'GRNTE_WHL_AFTR_CLNING_YN']).size(),
            columns={'count'}).reset_index()
        # ----------</보증기간경과 여부 확인>-------------

        # ----------<검사이력데이터>-------------
        # sql_stmt = """ /*+  PARALLEL(2) */
        #             SELECT
        #                 VHRNO, VIN, INSP_SE, INSP_MSRMT_MTHD, INSP_YMD, INSP_PASAGE_YN, SMO_1_ITM_MEVLU, SMO_1_ITM_PRMT_VAL, SMO_1_ITM_JGMT_YN
        #             FROM
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 INSP_SE IN ('정기검사', '정밀검사')
        #                 AND SMO_1_ITM_MEVLU IS NOT NULL
        #                 AND INSP_PASAGE_YN = 'Y'
        #                 AND SMO_1_ITM_JGMT_YN = 'Y'
        #             """
        # BD_INSP_HIS = self.dbUtil.export_to_pandas(sql_stmt)
        BD_INSP_HIS = self.INSP[((self.INSP['INSP_SE'] == '정기검사')|(self.INSP['INSP_SE'] == '정밀검사'))
                                & (self.INSP['SMO_1_ITM_MEVLU'].notnull())
                                & (self.INSP['INSP_PASAGE_YN'] == 'Y')
                                & (self.INSP['SMO_1_ITM_JGMT_YN'] == 'Y')]
        # ----------</검사이력데이터>-------------

        # ----------<1. 측정값이 존재하는 데이터만 추출>-------------
        jgjm_1 = BD_INSP_HIS[BD_INSP_HIS['SMO_1_ITM_MEVLU'].notnull()]
        # ----------</1. 측정값이 존재하는 데이터만 추출>-------------

        # ----------<2. 판정이 모두 Y인 데이터만 추출>-------------
        jgjm_2 = jgjm_1[(jgjm_1['INSP_PASAGE_YN'] == 'Y') & (jgjm_1['SMO_1_ITM_JGMT_YN'] == 'Y')]
        # ----------</2. 판정이 모두 Y인 데이터만 추출>-------------

        # ----------<3. 측정값 < 허용치인 데이터만 추출>-------------
        jgjm_2['SMO_1_ITM_MEVLU'] = pd.to_numeric(jgjm_2['SMO_1_ITM_MEVLU'])
        jgjm_2['SMO_1_ITM_PRMT_VAL'] = pd.to_numeric(jgjm_2['SMO_1_ITM_PRMT_VAL'])

        jgjm_3 = jgjm_2[jgjm_2['SMO_1_ITM_MEVLU'] <= jgjm_2['SMO_1_ITM_PRMT_VAL']]
        # ----------</3. 측정값 < 허용치인 데이터만 추출>-------------

        # ----------<4. 같은 일자에 여러번 합격받은 중복 제거>-------------
        jgjm_4 = jgjm_3.sort_values('INSP_YMD')
        jgjm_4.drop_duplicates(['VHRNO', 'VIN', 'INSP_YMD'], keep='last', inplace=True)
        # ----------</4. 같은 일자에 여러번 합격받은 중복 제거>-------------

        # ----------<클리닝 3회 + 검사이력데이터>-------------
        cleaning_grnty_in_3 = cleaning_grnty_in_2[cleaning_grnty_in_2['COUNT_IN'] == 3][
            ['VHRNO', 'VIN', 'ACTN_YMD', 'COUNT_IN', 'VHCTY_CD_NM', 'GRNTE_WHL_AFTR_CLNING_YN']]

        cleaning_3_jgjm = pd.merge(cleaning_grnty_in_3, jgjm_4, on=['VHRNO', 'VIN'])

        cleaning_3_jgjm['ACTN_YMD'] = pd.to_numeric(cleaning_3_jgjm['ACTN_YMD'])
        cleaning_3_jgjm['INSP_YMD'] = pd.to_numeric(cleaning_3_jgjm['INSP_YMD'])
        cleaning_3_jgjm_1 = cleaning_3_jgjm[cleaning_3_jgjm['ACTN_YMD'] <= cleaning_3_jgjm['INSP_YMD']]

        cleaning_3_jgjm_2 = cleaning_3_jgjm_1.sort_values('INSP_YMD')
        cleaning_3_jgjm_2['NUM'] = cleaning_3_jgjm_2.groupby(['VHRNO', 'VIN']).cumcount() + 1
        # ----------</클리닝 3회 + 검사이력데이터>-------------

        # ----------<검사횟수 1~4 데이터만 추출>-------------
        cleaning_3_jgjm_3 = cleaning_3_jgjm_2[(cleaning_3_jgjm_2['NUM'] >= 1) & (cleaning_3_jgjm_2['NUM'] <= 4)]

        cleaning_3_jgjm_3['SMO_1_ITM_MEVLU'] = pd.to_numeric(cleaning_3_jgjm_3['SMO_1_ITM_MEVLU'])
        res_2 = pd.DataFrame(cleaning_3_jgjm_3[['COUNT_IN', 'VHCTY_CD_NM', 'GRNTE_WHL_AFTR_CLNING_YN', 'NUM',
                                                'SMO_1_ITM_MEVLU']].groupby(
            ['COUNT_IN', 'VHCTY_CD_NM', 'GRNTE_WHL_AFTR_CLNING_YN', 'NUM']).mean()).reset_index()
        # ----------</검사횟수 1~4 데이터만 추출>-------------

        # ----------<최종 결과 테이블>-------------
        res = pd.merge(res_1, res_2, on=['COUNT_IN', 'VHCTY_CD_NM', 'GRNTE_WHL_AFTR_CLNING_YN'], how='outer')
        # ----------</최종 결과 테이블>-------------

        # ----------<컬럼명 표준화>-------------
        res.rename(columns={'COUNT_IN': 'GRNTE_WHL_ISE_CLNING_NCNT', 'VHCTY_CD_NM': 'VHCTY', 'count': 'VHCL_CNT',
                            'NUM': 'GRNTE_WHL_AFTR_INSP_NCNT', 'SMO_1_ITM_MEVLU': 'SMO_AVRG_MEVLU'}, inplace=True)
        # ----------</컬럼명 표준화>-------------

        # ----------<데이터프레임을 DB에 저장>-------------
        table_nm = 'std_bd_clning_3_aftr_mevlu_avg'

        # 오늘 날짜 컬럼 추가
        res['LOAD_DT'] = datetime.date.today()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(res.columns):
            if 'float' in res[column].dtype.name:
                sql += column+' float'
            elif 'int' in res[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(res.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(res, table_nm)
        # ----------</데이터프레임을 DB에 저장>-------------

        # ----------<데이터프레임의 메모리해제>-------------
        del [[res]]
        # ----------<데이터프레임의 메모리해제>-------------


    def proj06_analysis_05(self):
        """
        과제06-분석1 (구현 분석가: 김강민)
        :return:
        """

        # STD_BD_LWEM_ACTN_HST에서 필요한 컬럼 추출
        # sql_stmt = """
        #             SELECT
        #                 VIN, VHRNO, BF_SMO_CCNTRA, PST_SMO_CCNTRA
        #             FROM
        #                 "vsysb".STD_BD_LWEM_ACTN_HST
        #             WHERE
        #                 ACTN_SE = '저감장치클리닝'
        #             """
        # CL = self.dbUtil.export_to_pandas(sql_stmt)
        CL = self.LWEM_ACTN_HST[self.LWEM_ACTN_HST['ACTN_SE'] == '저감장치클리닝'].copy()

        # STD_BD_CAR에서 필요한 컬럼 추출
        # sql_stmt = """
        #             SELECT
        #                 VIN, VHRNO, VHCTY_CD_NM
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             """
        # CAR = self.dbUtil.export_to_pandas(sql_stmt)
        CAR = self.CAR.copy()

        cl = CL.merge(CAR, how='left', on=['VIN', 'VHRNO'])

        # 차량별 클리닝 회수 추가
        cl['CLNING_CNT'] = cl.groupby(['VIN', 'VHRNO']).cumcount() + 1


        cl['CLNING_CNT'] = cl['CLNING_CNT'].astype(str) + '회'

        # 4회 이상일 경우 '4회이상'으로 클리닝 회수 변경
        cl_s = (cl['CLNING_CNT'] != '1회') & (cl['CLNING_CNT'] != '2회') & (cl['CLNING_CNT'] != '3회')
        cl.loc[cl_s, 'CLNING_CNT'] = '4회이상'

        # 매연 배출량의 이상치 제거
        cl = cl[(cl['BF_SMO_CCNTRA'] < 1000) & (cl['PST_SMO_CCNTRA'] < 1000)]

        # 차종, 클리닝 회수별 집계
        def smo_ba(smo, ba):
            data = cl.groupby(['VHCTY_CD_NM', 'CLNING_CNT']).agg(AVRG_SMO_CCNTRA=(smo, 'mean'),
                                                                 VHCL_CNT=(smo, 'count')).reset_index()
            data_t = cl.groupby(['CLNING_CNT']).agg(AVRG_SMO_CCNTRA=(smo, 'mean'),
                                                    VHCL_CNT=(smo, 'count')).reset_index()
            data_t['VHCTY_CD_NM'] = '전체'
            data = pd.concat([data, data_t])
            data['SMO_SE'] = ba
            return data

        t = pd.concat([smo_ba('BF_SMO_CCNTRA', 'BEFORE'), smo_ba('PST_SMO_CCNTRA', 'AFTER')]).sort_values(
            by=['CLNING_CNT', 'SMO_SE'], ascending=[True, False])
        
        # 테이블 생성 연월일
        t['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_clning_smo_ccntra'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(t.columns):
            if 'float' in t[column].dtype.name:
                sql += column+' float'
            elif 'int' in t[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(t.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)


        self.dbUtil2.import_from_pandas(t, table_nm)
        # 322sec(6min)

    def proj06_analysis_06(self):
        """
        과제06-분석2 (구현 분석가: 김강민)
        :return:
        """

        # # STD_BD_LWEM_ACTN_HST에서 필요한 컬럼 추출
        # sql_stmt = """
        #             SELECT /*+ PARALLEL(2) */
        #                 VIN, VHRNO, ACTN_SE, ACTN_YMD, LEM_KND_CD_NM, ACTN_KND
        #             FROM
        #                 "vsysb".STD_BD_LWEM_ACTN_HST
        #             WHERE
        #                 ACTN_SE = '저감장치부착' OR ACTN_SE = '저감장치클리닝'
        #             """
        # LWEM = self.dbUtil.export_to_pandas(sql_stmt)
        LWEM = self.LWEM_ACTN_HST[(self.LWEM_ACTN_HST['ACTN_SE'].isin(['저감장치부착', '저감장치클리닝']))].copy()

        # STD_BD_CAR에서 필요한 컬럼 추출
        # sql_stmt = """
        #             SELECT /*+ PARALLEL(2) */
        #                 VIN, VHRNO
        #             FROM
        #                 "vsysb".STD_BD_CAR
        #             WHERE
        #                 EXHST_GAS_GRD_CD_NM IN ('T5', '5') AND FUEL_CD_NM = '경유'
        #             """
        # CAR = self.dbUtil.export_to_pandas(sql_stmt)
        CAR = self.CAR[(self.CAR['EXHST_GAS_GRD_CD_NM'].isin(['T5', '5'])) & (self.CAR['FUEL_CD_NM'] == '경유')].copy()

        # STD_BD_CAR_INSP_HST에서 필요한 컬럼 추출
        # sql_stmt = """
        #             SELECT /*+ PARALLEL(2) */
        #                 VIN, VHRNO, SMO_1_ITM_MEVLU, INSP_YMD
        #             FROM
        #                 "vsysb".STD_BD_CAR_INSP_HST
        #             WHERE
        #                 INSP_SE IN ('정기검사', '정밀검사') AND INSP_PASAGE_YN = 'Y' AND SMO_1_ITM_MEVLU IS NOT NULL
        #             """
        # INSP = self.dbUtil.export_to_pandas(sql_stmt)
        INSP = self.INSP[(self.INSP['INSP_SE'].isin(['정기검사', '정밀검사'])) & (self.INSP['INSP_PASAGE_YN'] == 'Y') & (self.INSP['SMO_1_ITM_MEVLU'].notnull())].copy()

        lwem = LWEM.merge(CAR, how='inner', on=['VIN', 'VHRNO'])

        rd = lwem[lwem['ACTN_SE'] == '저감장치부착']

        # LEM_KND_CD_NM의 엔진개조 및 삼원촉매 제거
        rd = rd[(rd['LEM_KND_CD_NM'] != '엔진개조') & (rd['LEM_KND_CD_NM'] != '삼원촉매')]

        # 최근 저감장치부착 내역만 출력
        rd.sort_values(by=['ACTN_YMD'], inplace=True)
        rd = rd.drop_duplicates(['VIN', 'VHRNO'], keep='last')

        rd['LEM_KND_NM'] = rd['LEM_KND_CD_NM'] + ' ' + rd['ACTN_KND']

        rd = rd[rd['LEM_KND_NM'].notnull()]

        rdinsp = rd.merge(INSP, how='left', on=['VIN', 'VHRNO'])

        rdinsp = rdinsp[rdinsp['ACTN_YMD'] <= rdinsp['INSP_YMD']]

        # 저감장치부착일자와 검사일자간의 연 구분
        rdinsp['DF_YR'] = (rdinsp['INSP_YMD'] - rdinsp['ACTN_YMD']).dt.days // 365

        rdinsp_s = (rdinsp['DF_YR'] < 2)
        rdinsp.loc[rdinsp_s, 'YR_SE'] = '2년이내'

        rdinsp_s = (rdinsp['DF_YR'] >= 2) & (rdinsp['DF_YR'] < 4)
        rdinsp.loc[rdinsp_s, 'YR_SE'] = '2년이후 4년이내'

        rdinsp_s = (rdinsp['DF_YR'] >= 4) & (rdinsp['DF_YR'] < 6)
        rdinsp.loc[rdinsp_s, 'YR_SE'] = '4년이후 6년이내'

        rdinsp_s = (rdinsp['DF_YR'] >= 6)
        rdinsp.loc[rdinsp_s, 'YR_SE'] = '6년이후'

        rdinsp_g = rdinsp.groupby(['LEM_KND_NM', 'YR_SE']).agg(INSP_CNT=('SMO_1_ITM_MEVLU', 'count'),
                                                               TOTL_SMO_MEVLU=('SMO_1_ITM_MEVLU', 'sum'),
                                                               AVRG_SMO_MEVLU=('SMO_1_ITM_MEVLU', 'mean')).reset_index()

        # 클리닝의 일자 없는 데이터 제거
        cl = lwem[(lwem['ACTN_SE'] == '저감장치클리닝') & (lwem['ACTN_YMD'].notnull())]
        cl = cl.loc[:, ['VIN', 'VHRNO', 'ACTN_YMD']]
        cl.rename(columns={'ACTN_YMD': 'CL_ACTN_YMD'}, inplace=True)

        rdcl = rd.merge(cl, how='inner', on=['VIN', 'VHRNO'])

        # 저감장치부착일자와 클리닝일자간의 연 구분
        rdcl['DF_YR'] = (rdcl['CL_ACTN_YMD'] - rdcl['ACTN_YMD']).dt.days // 365

        rdcl_s = (rdcl['DF_YR'] < 2)
        rdcl.loc[rdcl_s, 'YR_SE'] = '2년이내'

        rdcl_s = (rdcl['DF_YR'] >= 2) & (rdcl['DF_YR'] < 4)
        rdcl.loc[rdcl_s, 'YR_SE'] = '2년이후 4년이내'

        rdcl_s = (rdcl['DF_YR'] >= 4) & (rdcl['DF_YR'] < 6)
        rdcl.loc[rdcl_s, 'YR_SE'] = '4년이후 6년이내'

        rdcl_s = (rdcl['DF_YR'] >= 6)
        rdcl.loc[rdcl_s, 'YR_SE'] = '6년이후'

        rdcl_g = rdcl.groupby(['LEM_KND_NM', 'YR_SE']).size().reset_index(name='CLNING_CNT')

        rd_f = rdinsp_g.merge(rdcl_g, how='left', on=['LEM_KND_NM', 'YR_SE'])
        rd_f = rd_f.fillna(0)
        
        # 테이블 생성 연월일
        rd_f['LOAD_DT'] = datetime.date.today()

        # 데이터프레임을 DB에 적재
        table_nm = 'std_bd_rdcdvc_aftr_smo'.lower()
        
        # 테이블 생성
        sql = 'create table '+table_nm+'( \n'

        for idx,column in enumerate(rd_f.columns):
            if 'float' in rd_f[column].dtype.name:
                sql += column+' float'
            elif 'int' in rd_f[column].dtype.name:
                sql += column+' number'
            else:
                sql += column+' varchar(255)'

            if len(rd_f.columns)-1 != idx:
                sql += ','
            sql += '\n'
        sql += ')'    
        self.dbUtil2.execute(sql)

        self.dbUtil2.import_from_pandas(rd_f, table_nm)



if __name__ == '__main__':

    # 설정파일 경로
#     configFilePath = None
#     if os.path.isfile(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'analysisConf.ini')):
#         configFilePath = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'analysisConf.ini')
#     if configFilePath is None:
#         print("There is no DB configuration file.")
#         exit(1)



#     # 시스템 설정 정보(DB 및 SQL) 파일 로드
#     configUtil = ConfigUtil(configFilePath)

#     # 시스템 설정정보 딕셔너리 얻기
#     sys_conf_dict = configUtil.get_config_dict()

#     # DB 유틸 객체 생성
#     db_id = sys_conf_dict['db/id']
#     db_pwd = sys_conf_dict['db/pwd']
#     db_url = sys_conf_dict['db/url']
#     dbUtil = DBUtil(db_id, db_pwd, db_url)

    # 로깅 객체 생성 with DB 유틸 객체, 시스템 설정정보
#     logUtil = LogUtil(dbUtil)

    # 분석작업 객체 생성
    analysisTask = AnalysisTaskProj0306()

    # 분석쿼리 실행  with 시스템 설정정보 및 분석설정 정보
    try:
    
        print("<과제03-분석01 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj03_analysis_01()
        # 종료시간 저장
        end_time = time.time()
        print("</과제03-분석01 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)


    try:

        print("<과제03-분석02 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj03_analysis_02()
        # 종료시간 저장
        end_time = time.time()
        print("</과제03-분석02 실행 (수행시간: %d초)>" % (end_time - start_time))

    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)

    try:
    
        print("<과제04-분석01 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj04_analysis_01()
        # 종료시간 저장
        end_time = time.time()
        print("</과제04-분석01 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)

    try:
        print("<과제04-분석02 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj04_analysis_02()
        # 종료시간 저장
        end_time = time.time()
        print("</과제04-분석02 실행 (수행시간: %d초)>" % (end_time - start_time))
        
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)


    try:

        print("<과제04-분석03 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj04_analysis_03()
        # 종료시간 저장
        end_time = time.time()
        print("</과제04-분석03 실행 (수행시간: %d초)>" % (end_time - start_time))

    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)

    try:
        print("<과제05-분석01 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj05_analysis_01()
        # 종료시간 저장
        end_time = time.time()
        print("</과제05-분석01 실행 (수행시간: %d초)>" % (end_time - start_time))

    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)

    try:
        print("<과제05-분석02 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj05_analysis_02()
        # 종료시간 저장
        end_time = time.time()
        print("</과제05-분석02 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)


    try:
        print("<과제05-분석03 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj05_analysis_03()
        # 종료시간 저장
        end_time = time.time()
        print("</과제05-분석03 실행 (수행시간: %d초)>" % (end_time - start_time))
        
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)

    try:
        print("<과제06-분석01 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj06_analysis_01()
        # 종료시간 저장
        end_time = time.time()
        print("</과제06-분석01 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)



    try:
    
        print("<과제06-분석02 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj06_analysis_02()
        # 종료시간 저장
        end_time = time.time()
        print("</과제06-분석02 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)




    try:
        print("<과제06-분석03 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj06_analysis_03()
        # 종료시간 저장
        end_time = time.time()
        print("</과제06-분석03 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)

    try:
    
        print("<과제06-분석04 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj06_analysis_04()
        # 종료시간 저장
        end_time = time.time()
        print("</과제06-분석04 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)


    try:
    
        print("<과제06-분석05 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj06_analysis_05()
        # 종료시간 저장
        end_time = time.time()
        print("</과제06-분석05 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)

    try:
    
        print("<과제06-분석06 실행>")
        # 시작 시간 저장
        start_time = time.time()
        # 분석 실행
        analysisTask.proj06_analysis_06()
        # 종료시간 저장
        end_time = time.time()
        print("</과제06-분석06 실행 (수행시간: %d초)>" % (end_time - start_time))
    
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)
