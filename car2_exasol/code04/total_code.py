# Library

import pandas as pd
import numpy as np
from datetime import datetime
import pyexasol
# import psycopg2
import scipy.interpolate as intp
import time

start_time = time.time()

# 날짜 코드
## 기준연월 설정
## 연도 설정
## 날짜 설정

# server

# insider db
wd = pyexasol.connect(dsn='172.29.135.35/F99FAB2444F86051A9A467F6313FAAB48AF7C4760663430958E3B89A9DC53361:8563', user='sys', password='exasol', compression=True, schema='VSYSD')
we = pyexasol.connect(dsn='172.29.135.35/F99FAB2444F86051A9A467F6313FAAB48AF7C4760663430958E3B89A9DC53361:8563', user='sys', password='exasol', compression=True, schema='VSYSE')
# ws = pyexasol.connect(dsn='172.29.135.35/F99FAB2444F86051A9A467F6313FAAB48AF7C4760663430958E3B89A9DC53361:8563', user='sys', password='exasol', compression=True, schema='SYS')

# # exasol db
# wd = pyexasol.connect(dsn='dev.openankus.org:8563', user='sys', password='djslzja', compression=True, schema='VSYSD')
# we = pyexasol.connect(dsn='dev.openankus.org:8563', user='sys', password='djslzja', compression=True, schema='VSYSE')
# wbt = pyexasol.connect(dsn='dev.openankus.org:8563', user='sys', password='djslzja', compression=True, schema='wbt')

# Load ###################################################################################################################

## 등록정보(STD_CEG_CAR_MIG) 4등급만

# 8.6s
car = wd.export_to_pandas("SELECT VIN, BSPL_STDG_CD, EXHST_GAS_GRD_CD, EXHST_GAS_CERT_NO, VHCL_ERSR_YN, MANP_MNG_NO, YRIDNW, VHCTY_CD, PURPS_CD2, FRST_REG_YMD, VHCL_FBCTN_YMD, VHCL_MNG_NO, VHRNO, EXTGAS_INSP_VLD_YMD, VHCL_OWNR_CL_CD FROM STD_CEG_CAR_MIG WHERE EXHST_GAS_GRD_CD = 'A0504' OR EXHST_GAS_GRD_CD = 'A05T4';")
car_ch_col = {
    'VIN':'차대번호', 
    'BSPL_STDG_CD':'법정동코드', 
    'EXHST_GAS_GRD_CD':'배출가스등급', 
    'EXHST_GAS_CERT_NO':'배출가스인증번호',
    'VHCL_ERSR_YN':'차량말소YN',
    'MANP_MNG_NO':'제원관리번호', 
    'YRIDNW':'차량연식', 
    'VHCTY_CD':'차종', 
    'PURPS_CD2':'용도', 
    'FRST_REG_YMD':'최초등록일자',
    'VHCL_FBCTN_YMD':'제작일자', 
    'VHCL_MNG_NO':'차량관리번호', 
    'VHRNO':'자동차등록번호',
    'EXTGAS_INSP_VLD_YMD':'검사유효일',
    'VHCL_OWNR_CL_CD':'소유자구분',  
}
carr = car.rename(columns=car_ch_col)

print('data load : STD_CEG_CAR_MIG')

## 제원정보(STD_CEG_CAR_SRC_MIG)

# 3.8s
src = wd.export_to_pandas("SELECT MANP_MNG_NO, FUEL_CD, VHCTY_TY_CD2, MNFCTR_NM, VHCNM, VHCL_FRM, EGIN_TY, VHCTY_CL_CD, TOTL_WGHT, CRYNG_WGHT, DSPLVL, EGIN_OTPT FROM STD_CEG_CAR_SRC_MIG;")
src_ch_col = {
    'MANP_MNG_NO':'제원관리번호', 
    'FUEL_CD':'연료',
    'VHCTY_TY_CD2':'차종유형', 
    'MNFCTR_NM':'제작사명', 
    'VHCNM':'차명', 
    'VHCL_FRM':'자동차형식', 
    'EGIN_TY':'엔진형식', 
    'VHCTY_CL_CD':'차종분류',
    'TOTL_WGHT':'총중량',
    'CRYNG_WGHT':'적재중량',
    'DSPLVL':'배기량', 
    'EGIN_OTPT':'엔진출력',
}
srcr = src.rename(columns=src_ch_col)

print('data load : STD_CEG_CAR_SRC_MIG')

## 정기검사(STD_TB_JGT_HIS)

# 3m 34.9s
# jgt = wb.export_to_pandas("SELECT VIN, FDRM_INSP_INSP_MTHD_CD, FDRM_INSP_KND_CD, FDRM_INSP_JGMT, FDRM_NLOD_SMO_MSTVL1, FDRM_NLOD_SMO_MSTVL2, FDRM_NLOD_SMO_MSTVL3, FDRM_NLOD_SMO_JT_YN1, FDRM_INSP_YMD, FDRM_DRVNG_DSTNC, FDRM_NLOD_SMO_PRMVL1 FROM STD_TB_JGT_HIS WHERE ROWNUM <= 10000;") # 테스트용
jgt = wd.export_to_pandas("SELECT VIN, FDRM_INSP_INSP_MTHD_CD, FDRM_INSP_KND_CD, FDRM_INSP_JGMT, FDRM_NLOD_SMO_MSTVL1, FDRM_NLOD_SMO_MSTVL2, FDRM_NLOD_SMO_MSTVL3, FDRM_NLOD_SMO_JT_YN1, FDRM_INSP_YMD, FDRM_DRVNG_DSTNC, FDRM_NLOD_SMO_PRMVL1 FROM STD_TB_JGT_HIS;")
jgt_ch_col = {
    'VIN':'차대번호', 
    'FDRM_INSP_INSP_MTHD_CD':'검사방법', 
    'FDRM_INSP_KND_CD':'검사종류', 
    'FDRM_INSP_JGMT':'검사판정', 
    'FDRM_NLOD_SMO_MSTVL1':'무부하매연측정치1', 
    'FDRM_NLOD_SMO_MSTVL2':'무부하매연측정치2', 
    'FDRM_NLOD_SMO_MSTVL3':'무부하매연측정치3', 
    'FDRM_NLOD_SMO_JT_YN1':'무부하매연판정1', 
    'FDRM_INSP_YMD':'검사일자',
    'FDRM_DRVNG_DSTNC':'주행거리',
    'FDRM_NLOD_SMO_PRMVL1':'무부하매연허용치1', 
}
jgtr = jgt.rename(columns=jgt_ch_col)

print('data load : STD_CEG_CAR_SRC_MIG')

## 정밀검사(STD_TB_EET_HIS_ME)

# 6m 36.1s
eet = wd.export_to_pandas("SELECT VIN, PRCINSP_MSRMT_MTHD_CD, PRCINSP_KND_CD, PRCINSP_JGMT, PREC_NLOD_SMO_MSTVL1, PREC_NLOD_SMO_MSTVL2, PREC_NLOD_SMO_MSTVL3, PREC_NLOD_SMO_JT_YN1, PRCINSP_YMD, PRCINSP_DRVNG_DSTNC, PREC_NLOD_SMO_PRMVL1 FROM STD_TB_EET_HIS_ME;")
eet_ch_col = {
    'VIN':'차대번호', 
    'PRCINSP_MSRMT_MTHD_CD':'검사방법', 
    'PRCINSP_KND_CD':'검사종류', 
    'PRCINSP_JGMT':'검사판정', 
    'PREC_NLOD_SMO_MSTVL1':'무부하매연측정치1', 
    'PREC_NLOD_SMO_MSTVL2':'무부하매연측정치2', 
    'PREC_NLOD_SMO_MSTVL3':'무부하매연측정치3', 
    'PREC_NLOD_SMO_JT_YN1':'무부하매연판정1', 
    'PRCINSP_YMD':'검사일자',
    'PRCINSP_DRVNG_DSTNC':'주행거리', 
    'PREC_NLOD_SMO_PRMVL1':'무부하매연허용치1', 
}
eetr = eet.rename(columns=eet_ch_col)

print('data load : STD_TB_EET_HIS_ME')

## 법정동코드(STD_BJCD_INFO)

# 1.3s
code = wd.export_to_pandas("SELECT STDG_CD, STDG_CTPV_NM, STDG_SGG_NM, STDG_CTPV_CD, STDG_SGG_CD FROM STD_BJCD_INFO;")
code_ch_col = {
    'STDG_CD':'법정동코드', 
    'STDG_CTPV_NM':'시도', 
    'STDG_SGG_NM':'시군구', 
    'STDG_CTPV_CD':'시도코드',
    'STDG_SGG_CD':'시군구코드', # 오타 수정 요청 : SSG -> SGG(2023.07.13 어니컴 VSYSD에서 발견)
}
coder = code.rename(columns=code_ch_col)

print('data load : STD_BJCD_INFO')

## 노후차 조기폐차 관리정보(수도권)(STD_DLM_TB_ERP_EARLY_ERASE_AEA)

# 2.4s
aea = wd.export_to_pandas("SELECT VIN, ELPDSRC_STTS_CD, ELPDSRC_LST_APRV_YN, ERSR_YMD FROM STD_DLM_TB_ERP_EARLY_ERASE_AEA;")
aea_ch_col = {
    'VIN':'차대번호', 
    'ELPDSRC_STTS_CD':'조기폐차상태코드', 
    'ELPDSRC_LST_APRV_YN':'조기폐차최종승인YN', 
    'ERSR_YMD':'말소일자', 
}
aear = aea.rename(columns=aea_ch_col)

print('data load : STD_DLM_TB_ERP_EARLY_ERASE_AEA')

## 노후차 조기폐차 관리정보(수도권외)(STD_DLM_TB_ERP_EARLY_ERASE_LGV)

# 1.8s
lgv = wd.export_to_pandas("SELECT VIN, ELPDSRC_STTS_CD, ELPDSRC_LST_APRV_YN, ERSR_YMD FROM STD_DLM_TB_ERP_EARLY_ERASE_LGV;")
lgv_ch_col = {
    'VIN':'차대번호', 
    'ELPDSRC_STTS_CD':'조기폐차상태코드', 
    'ELPDSRC_LST_APRV_YN':'조기폐차최종승인YN', 
    'ERSR_YMD':'말소일자',  
}
lgvr = lgv.rename(columns=lgv_ch_col)

print('data load : STD_DLM_TB_ERP_EARLY_ERASE_LGV')

## 저감장치 부착이력(STD_DLM_TB_ERP_ATT_HIS)

# 3.0s
att = wd.export_to_pandas("SELECT VIN, RDCDVC_SE_CD, RDCDVC_KND_CD FROM STD_DLM_TB_ERP_ATT_HIS;")
att_ch_col = {
    'VIN':'차대번호', 
    'RDCDVC_SE_CD':'저감장치구분',
    'RDCDVC_KND_CD':'저감장치종류',
}
attr = att.rename(columns=att_ch_col)

print('data load : STD_DLM_TB_ERP_ATT_HIS')

## 등록이력(CEG_CAR_HISTORY_MIG)

# 1.8s
# edb_id = 'vsysd'
# edb_database = 'edb'
# edb_port = 5444
# edb_url = '172.29.135.50'
# edb_pwd = 'vsyswynn'
# conn = psycopg2.connect(dbname=edb_database, user=edb_id, password=edb_pwd, host=edb_url, port=edb_port)
# cur = conn.cursor()
# sql = 'select VHCL_ERSR_YN, CHNG_DE, VHMNO from vsysd.ceg_car_history_mig'
# cur.execute(sql)
# his = pd.DataFrame(cur.fetchall())
# his.columns = [desc[0].upper() for desc in cur.description]
# his_ch_col = {
#     'VHCL_ERSR_YN':'차량말소YN', 
#     'CHNG_DE':'변경일자',
#     'VHMNO':'차량관리번호'
# }
# hisr = his.rename(columns=his_ch_col)

his = wd.export_to_pandas("SELECT VHCL_ERSR_YN, CHNG_DE, VHMNO FROM CEG_CAR_HISTORY_MIG;")
his_ch_col = {
    'VHCL_ERSR_YN':'차량말소YN', 
    'CHNG_DE':'변경일자',
    'VHMNO':'차량관리번호'
}
hisr = his.rename(columns=his_ch_col)

print('data load : CEG_CAR_HISTORY_MIG')

## 비상시 및 계절제 단속발령(N_IS_ISSUE_DISCLOSURE)

# 1.8s
# sql = "select REGLT_NO, GNFD_NO, VIN, REG_SIDO_CD, REG_SIGNGU_CD, REGLT_AREA_CD from vsysd.n_is_issue_disclosure"
# cur.execute(sql)
# isdis = pd.DataFrame(cur.fetchall())

# isdis.columns = [desc[0].upper() for desc in cur.description]
# isdis_ch_col = {
#     'REGLT_NO':'적발번호', 
#     'GNFD_NO':'발령번호', 
#     'REG_SIDO_CD':'등록시도코드', 
#     'REG_SIGNGU_CD':'등록시군구코드', 
#     'VIN':'차대번호',
#     'REGLT_AREA_CD':'적발지역코드', 
# }
# isdisr = isdis.rename(columns=isdis_ch_col)

isdis = wd.export_to_pandas("SELECT REGLT_NO, GNFD_NO, VIN, REG_SIDO_CD, REG_SIGNGU_CD, REGLT_AREA_CD FROM N_IS_ISSUE_DISCLOSURE;")
isdis_ch_col = {
    'REGLT_NO':'적발번호', 
    'GNFD_NO':'발령번호', 
    'REG_SIDO_CD':'등록시도코드', 
    'REG_SIGNGU_CD':'등록시군구코드', 
    'VIN':'차대번호',
    'REGLT_AREA_CD':'적발지역코드', 
}
isdisr = isdis.rename(columns=isdis_ch_col)

print('data load : STD_N_IS_ISSUE_DISCLOSURE')

## 운행제한 발령정보(N_IS_ISSUE)

# sql = "select GNFD_NO, TY_STDR_ID, DNSTY_STDR_ID from vsysd.n_is_issue"
# cur.execute(sql)
# isis = pd.DataFrame(cur.fetchall())

# isis.columns = [desc[0].upper() for desc in cur.description]
# isis_ch_col = {
#     'GNFD_NO':'발령번호', 
#     'DNSTY_STDR_ID':'농도기준아이디', 
#     'TY_STDR_ID':'유형기준아이디', 
# }
# isisr = isis.rename(columns=isis_ch_col)

isis = wd.export_to_pandas("SELECT GNFD_NO, TY_STDR_ID, DNSTY_STDR_ID FROM N_IS_ISSUE;")
isis_ch_col = {
    'GNFD_NO':'발령번호', 
    'DNSTY_STDR_ID':'농도기준아이디', 
    'TY_STDR_ID':'유형기준아이디', 
}
isisr = isis.rename(columns=isis_ch_col)

print('data load : STD_N_IS_ISSUE')

## N_IS_PENALTY

# sql = "select REGLT_NO, REGLT_DE from vsysd.n_is_penalty"
# cur.execute(sql)
# ispe = pd.DataFrame(cur.fetchall())
# ispe.columns = [desc[0].upper() for desc in cur.description]
# ispe_ch_col = {
#     'REGLT_NO':'적발번호', 
#     'REGLT_DE':'단속일', 
# }
# isper = ispe.rename(columns=ispe_ch_col)

ispe = wd.export_to_pandas("SELECT REGLT_NO, REGLT_DE FROM N_IS_PENALTY;")
ispe_ch_col = {
    'REGLT_NO':'적발번호', 
    'REGLT_DE':'단속일', 
}
isper = ispe.rename(columns=ispe_ch_col)

print('data load : N_IS_PENALTY')

## 운행제한 단속정보(N_US_DISCLOSURE)

# sql = 'select "NO", VIN, DISCL_TY, REGLT_AREA_CD, REG_SIDO_CD, REG_SIGNGU_CD from vsysd.n_us_disclosure'
# cur.execute(sql)
# usdis = pd.DataFrame(cur.fetchall())
# usdis.columns = [desc[0].upper() for desc in cur.description]
# usdis_ch_dict = {
#     'NO':'번호', 
#     'VIN':'차대번호', 
#     'REG_SIDO_CD':'등록시도코드', 
#     'REG_SIGNGU_CD':'등록시군구코드', 
#     'DISCL_TY':'적발유형', 
#     'REGLT_AREA_CD':'단속지역코드', 
# }
# usdisr = usdis.rename(columns=usdis_ch_dict)

usdis = wd.export_to_pandas('SELECT "NO", VIN, DISCL_TY, REGLT_AREA_CD, REG_SIDO_CD, REG_SIGNGU_CD FROM N_US_DISCLOSURE;')
usdis_ch_dict = {
    'NO':'번호', 
    'VIN':'차대번호', 
    'REG_SIDO_CD':'등록시도코드', 
    'REG_SIGNGU_CD':'등록시군구코드', 
    'DISCL_TY':'적발유형', 
    'REGLT_AREA_CD':'단속지역코드', 
}
usdisr = usdis.rename(columns=usdis_ch_dict)

print('data load : N_US_DISCLOSURE')

## N_US_PENALTY

# sql = 'select "NO", REGLT_CNT, REGLT_YM FROM from vsysd.n_us_penalty'
# cur.execute(sql)
# uspe = pd.DataFrame(cur.fetchall())
# uspe.columns = [desc[0].upper() for desc in cur.description]
# uspe_ch_dict = {
#     'NO':'번호', 
#     'REGLT_CNT':'적발건수', 
#     'REGLT_YM':'적발년월'
# }
# usper = uspe.rename(columns=uspe_ch_dict)
# cur.close()
# conn.close()

uspe = wd.export_to_pandas('SELECT "NO", REGLT_CNT, REGLT_YM FROM N_US_PENALTY;')
uspe_ch_dict = {
    'NO':'번호', 
    'REGLT_CNT':'적발건수', 
    'REGLT_YM':'적발년월'
}
usper = uspe.rename(columns=uspe_ch_dict)

print('data load : STD_N_US_PENALTY')

## RH에서 제공한 법정동코드

rh = we.export_to_pandas("SELECT DONG_CODE, CTPRVN_NM, SIGNGU_NM FROM STD_BD_TB_MAPDATA;")
rh = rh.rename(columns={
    'DONG_CODE':'법정동코드_rh', 
    'CTPRVN_NM':'시도', 
    'SIGNGU_NM':'시군구'
    })

print('data load : STD_BD_TB_MAPDATA')

## 4등급 result(for DPF유무)

# 20s
rs = we.export_to_pandas("SELECT 차대번호, DPF유무_수정 FROM STD_BD_GRD4_RESULT;")

print('data load : STD_BD_GRD4_RESULT')

## STD_BD_KOSIS

kosis = we.export_to_pandas("SELECT CTPV, SGG, VHCTY_CD, DY_AVRG_DRVNG_DSTNC FROM STD_BD_KOSIS;")
kosis_ch_col = {
    'CTPV':'시도', 
    'SGG':'시군구', 
    'VHCTY_CD':'차종', 
    'DY_AVRG_DRVNG_DSTNC':'일일평균주행거리', 
}
kosisr = kosis.rename(columns=kosis_ch_col)

print('data load : STD_BD_KOSIS')

## 운행제한 건수 데이터

# # 3.0s
# lmt = we.export_to_pandas("SELECT * FROM STD_BD_GRD5_LMT_NOCS;")
# lmt['운행제한건수'] = lmt[['계절제_1차', '계절제_2차', '계절제_3차', '계절제_4차', '비상시', '상시']].sum(axis=1)

# print('data load : STD_BD_GRD5_LMT_NOCS')

# 전처리 #############################################################################################

## 등록정보

## 중복 차대번호 제거
carr = carr.sort_values('최초등록일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

## 배출가스등급 코드 변환
grd_dict = {
    'A0501':'1', 
    'A0502':'2', 
    'A0503':'3', 
    'A0504':'4', 
    'A0505':'5', 
    'A05T2':'2',
    'A05T3':'3', 
    'A05T4':'4', 
    'A05T5':'5', 
    'A05X':'X', 
}
carr['배출가스등급'] = carr['배출가스등급'].replace(grd_dict)

# carr['배출가스등급'].unique()

## 차종 코드 변환
cd_dict = {
    'A31M':'이륜', 
    'A31P':'승용', 
    'A31S':'특수', 
    'A31T':'화물', 
    'A31V':'승합'
}
carr['차종'] = carr['차종'].replace(cd_dict)

## 용도 코드 변환
purps_dict = {
    'A08P':'개인용', 
    'A08B':'영업용', 
    'A08O':'관용',
}
carr['용도'] = carr['용도'].replace(purps_dict)

## 소유자구분 코드 변환
ownr_dict = {
    'A27B':'사업자', 
    'A27F':'외국인', 
    'A27L':'법인', 
    'A27O':'기타', 
    'A27R':'주민', 
}
carr['소유자구분'] = carr['소유자구분'].replace(ownr_dict)

## 등록정보 말소 제거
carm = carr[carr['차량말소YN'] == 'N'].reset_index(drop=True)

## 제원정보

## 연료 코드 변환
fuel_dict = {
    'A90GS':'휘발유', 
    'A91DS':'경유',
    'A92LP':'LPG(액화석유가스)', 
    'A90GH':'휘발유 하이브리드', 
    'A93EV':'전기', 
    'A91DH':'경유 하이브리드', 
    'A92CN':'CNG(압축천연가스)', 
    'A93HD':'수소', 
    'A92LH':'LPG 하이브리드', 
    'A94OT':'기타연료', 
    'A92CH':'CNG 하이브리드',
    'A90AC':'알코올', 
    'A93SH':'태양열', 
    'A91KS':'등유', 
    'A92LN':'LNG(액화천연가스)', 
    'A90PH':'플러그인 하이브리드', 
}
srcr['연료'] = srcr['연료'].replace(fuel_dict)

## 차종유형 코드 변환
ty_dict = {
    'A30C':'경형', 
    'A30L':'대형', 
    'A30M':'중형', 
    'A30S':'소형',
}
srcr['차종유형'] = srcr['차종유형'].replace(ty_dict)

## 등록&제원 병합

# 0.7s
cs = carm.merge(srcr, on='제원관리번호', how='left')

## 정기&정밀 병합

# 3m 1.9s
ins = pd.concat([jgtr, eetr], ignore_index=True)

### 최근 검사만 활용
# 4m 14.3s
insm = ins.sort_values('검사일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

## 검사종류 코드 변환
# 27.5s
knd_dict = {
    'A21P01':'정밀검사', 
    'A1904':'정기검사', 
    'A21T06':'종합검사(경과)', 
    'A21T05':'종합검사', 
    'A21S01':'특정검사', 
    'A1902':'재검사(2부재검사)',
    'A21R02':'2부재검사(종합)', 
    'A21P02':'정밀검사(경과)', 
    'A1903':'정기(경과)검사', 
    'A21S04':'특정재검사(배출)', 
    'A21E01':'배출재검사', 
    'A21R01':'1부재검사(종합)',
    'A21C02':'구조변경검사', 
    'A21002':'관능재검사(임시)', 
    'A21S03':'특정재검사(관능)', 
    'A21S02':'특정검사(경과)', 
    'A1901':'재검사(1부재검사)', 
    'A21T02':'종합(정밀)',
    'A21E02':'배출재검사(구변)', 
    'A21T01':'종합(경과:정밀)', 
    'A21T04':'종합(정밀) 2부재검사', 
    'A21001':'관능재검사', 
    'A21T03':'종합(정밀) 1부재검사', 
    'A21TMP':'임시검사', 
}
insm['검사종류'] = insm['검사종류'].replace(knd_dict)

## 검사방법 코드 변환
# 14.9s
mth_dict = {
    'A18A':'무부하검사(TSI)', 
    'A18B':'무부하검사(급가속)', 
    'A18C':'무부하검사(정지가동)',
    'A2301':'무부하검사(급가속)', 
    'A2302':'무부하검사(정지가동)',
    'A2303':'무부하검사(TSI)', 
    'A2304':'부하검사(LUG DOWN)', 
    'A2305':'부하검사(ASM-Idling)', 
    'A2306':'부하검사(KD-147)', 
    'A2307':'부하검사(ASM2525)', 
}
insm['검사방법'] = insm['검사방법'].replace(mth_dict)

## 저감장치구분 코드 변환

rdcdvc_dict = {
    'A1001':'1종', 
    'A1002':'2종', 
    'A1003':'3종', 
    'A1004':'1종+SCR', 
    'A1005':'엔진개조', 
    'A1006':'엔진교체',
    'A1007':'삼원촉매',
}
attr['저감장치구분'] = attr['저감장치구분'].replace(rdcdvc_dict)

## 저감장치 부착 유무
attr.loc[(attr['저감장치구분'] == '1종') | (attr['저감장치구분'] == '1종+SCR'), 'DPF_YN'] = '유'

# 중복 차대번호 제거
attr = attr.sort_values('DPF_YN').drop_duplicates('차대번호').reset_index(drop=True)

# attr.columns

## 조기폐차(수도권, 수도권 외)

# aear['조기폐차상태코드'].unique()

# erase_dict = {
#     'A32E':'조기폐차상태코드(추가보조금신청대상)',
#     'A32G':'조기폐차상태코드(보조금청구)',
#     'A32I':'조기폐차상태코드(신청등록)',
#     'A32K':'조기폐차상태코드(추가보조금청구승인)',
#     'A32M':'조기폐차상태코드(보조금산정)',
#     'A32N':'조기폐차상태코드(보조금청구반려(제외))',
#     'A32P':'조기폐차상태코드(보조금대상)',
#     'A32T':'조기폐차상태코드(추가보조금청구)',
#     'A32X':'조기폐차상태코드(신청취소(제외))',
#     'A32Y':'조기폐차상태코드(보조금청구승인)',
#     'A32C':'조기폐차상태코드(성능확인검사등록)',
#     'A32D':'조기폐차상태코드(기간초과)',
#     'A32A':'조기폐차상태코드(성능확인검사신청)',
#     'A32B':'조기폐차상태코드(보조금미대상)',
# }
erase_dict = {
    'A32E':'E',
    'A32G':'G',
    'A32I':'I',
    'A32K':'K',
    'A32M':'M',
    'A32N':'N',
    'A32P':'P',
    'A32T':'T',
    'A32X':'X',
    'A32Y':'Y',
    'A32C':'C',
    'A32D':'D',
    'A32A':'A',
    'A32B':'B',
}
aear['조기폐차상태코드'] = aear['조기폐차상태코드'].replace(erase_dict)

# aear['조기폐차상태코드'].unique()

# lgvr['조기폐차상태코드'].unique()

lgvr['조기폐차상태코드'] = lgvr['조기폐차상태코드'].replace(erase_dict)

# lgvr['조기폐차상태코드'].unique()

### 조기폐차 신청 정보 추가

aear['조기폐차신청여부'] = 'Y'
lgvr['조기폐차신청여부'] = 'Y'

### 조기폐차 병합

elp = pd.concat([aear, lgvr], ignore_index=True)

# elp.shape

elp.shape, len(elp['차대번호'].unique())

elpm = elp.sort_values('조기폐차최종승인YN', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

# elpm.shape

elpm = elpm[elpm['조기폐차최종승인YN'] == 'Y'].reset_index(drop=True)

# elpm.shape

## kosis 시도명 수정

kosisr.loc[kosisr['시도'] == '강원도', '시도'] = '강원특별자치도'

## 등록&제원&정기&정밀 병합

# 2m 0.5s
csi = cs.merge(insm, on='차대번호', how='left')

## 등록&제원&정기&정밀&법정동코드 병합

# csi['법정동코드'].isnull().sum()

csi['법정동코드'] = csi['법정동코드'].astype('str')
csi['법정동코드'] = csi['법정동코드'].str[:5] + '00000'
csi['법정동코드'] = pd.to_numeric(csi['법정동코드'])

# csi['법정동코드'].isnull().sum()

df = csi.merge(coder, on='법정동코드', how='left')

# df['시도'].isnull().sum()

## 주소 수정

df['법정동코드_mod'] = df['법정동코드'].copy()

## 4등급 result 파일 참고하여 DPF유무 수정

rdf = df.copy()
rs = rs.drop_duplicates('차대번호').reset_index(drop=True)
rdf1 = rdf.merge(rs, on='차대번호', how='left')
df = rdf1.copy()

# df.shape

## 연료 컬럼 추가

df.loc[df['연료'] == '경유', 'fuel'] = '경유'
df.loc[(df['연료'] == '휘발유') | (df['연료'] == 'LPG(액화석유가스)'), 'fuel'] = '휘발유_가스'

# 분석

## 1-1 start

## EG 분류

grade_list = []
for f, y, cy, e in df[['fuel', '제작일자', '차량연식', 'DPF유무_수정']].values:
    if (f == '휘발유_가스') and ( (19980101 <= y <= 20001231) or (1998 <= cy <= 2000) ):
        grade_list.append('A')
    elif (f == '휘발유_가스') and ( (y <= 19971231) or (cy <= 1997) ):
        grade_list.append('B')
    elif (f == '경유') and ( (y >= 20080101) or (cy >= 2008) ) and (e == '유'):
        grade_list.append('A')
    elif (f == '경유') and ( (y <= 20071231) or (cy <= 2007) )and (e == '유'):
        grade_list.append('B')
    elif (f == '경유') and ( (y >= 20080101) or (cy >= 2008) ) and (e == '무'):
        grade_list.append('C')
    elif (f == '경유') and ( (y <= 20071231) or (cy <= 2007) ) and (e == '무'):
        grade_list.append('D')
    else:
        grade_list.append('X')
df['Grade'] = grade_list

# STD_BD_GRD4_CAR_CURSTT
## 4등급차만 추출
# 2.8s
df1 = df[df['배출가스등급'] == '4'].reset_index(drop=True)

# df1.shape

### 테이블생성일자 컬럼 추가
today_date = datetime.today().strftime("%Y%m%d")
df1['기준연월'] = '2022.12'
# df1['기준연월'] = today_date[:4] + '.' + today_date[4:6]
df1['테이블생성일자'] = today_date
# RH제공 법정동코드 타입 문자열로 수정
df1['법정동코드_mod'] = df1['법정동코드_mod'].astype('str')

# df1['법정동코드_mod'].head()

df1.loc[df1['시군구'] == '군위군', '시도'].unique()

STD_BD_GRD4_CAR_CURSTT = df1[[
    '테이블생성일자', 
    '기준연월',
    '차대번호', 
    '제원관리번호', 
    '차종', 
    '용도',
    '차량연식', 
    '차종유형', 
    '연료', 
    '법정동코드', 
    '시도',
    '시군구',
    '차명', 
    '제작사명', 
    '배출가스인증번호', 
    '배출가스등급', 
    'DPF유무_수정',
    '검사방법', 
    '검사종류', 
    '검사판정', 
    '무부하매연측정치1', 
    '무부하매연판정1', 
    '무부하매연측정치2', 
    '무부하매연측정치3', 
    '법정동코드_mod', 
    'Grade', 
    ]]
ch_col_dict = {
                '테이블생성일자':'LOAD_DT',
                '기준연월':'CRTR_YM', 
                '차대번호':'VIN', 
                '제원관리번호':'MANG_MNG_NO', 
                '차종':'VHCTY_CD', 
                '용도':'PURPS_CD2', 
                '차량연식':'YRIDNW', 
                '차종유형':'VHCTY_TY', 
                '연료':'FUEL_CD', 
                '법정동코드':'STDG_CD', 
                '시도':'CTPV_NM', 
                '시군구':'SGG_NM', 
                '차명':'VHCNM', 
                '제작사명':'MNFCTR_NM', 
                '배출가스인증번호':'EXHST_GAS_CERT_NO_MOD', 
                '배출가스등급':'EXHST_GAS_GRD_CD_MOD',
                'DPF유무_수정':'DPF_MNTNG_YN', 
                '검사방법':'INSP_MTHD', 
                '검사종류':'INSP_KND', 
                '검사판정':'INSP_JGMT', 
                '무부하매연측정치1':'NOLOD_SMO_MEVLU1', 
                '무부하매연판정1':'NOLOD_SMO_JGMT_YN1', 
                '무부하매연측정치2':'NOLOD_SMO_MEVLU2', 
                '무부하매연측정치3':'NOLOD_SMO_MEVLU3', 
                '법정동코드_mod':'STDG_CD_MOD',
                'Grade':'GRD4_MLSFC',

                '제작일자':'FBCTN_YMD', 
                '차종분류':'VHCTY_CL_CD',
                }
STD_BD_GRD4_CAR_CURSTT = STD_BD_GRD4_CAR_CURSTT.rename(columns=ch_col_dict)
 
# STD_BD_GRD4_CAR_CURSTT.columns

### [출력] STD_BD_GRD4_CAR_CURSTT

# expdf = STD_BD_GRD4_CAR_CURSTT
# table_nm = 'STD_BD_GRD4_CAR_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)
# # 데이터 추가
# # 9s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_CAR_CURSTT')




## !!! 수정 시작(2023.08.24) 
## 1\. 경유차만 추출
df2 = df1[df1['연료'] == '경유'].reset_index(drop=True)
### 차대번호 10자리 연식
df2['vin10'] = df2['차대번호'].str[9]
vin10_dict = {'J':1988, 'K':1989, 'L':1990, 'M':1991, 'N':1992, 'P':1993, 'R':1994, 'S':1995, 'T':1996, 'V':1997, 'W':1998, 'X':1999, 'Y':2000, '1':2001, '2':2002, '3':2003, '4':2004, '5':2005, '6':2006, '7':2007, '8':2008, '9':2009, 'A':2010, 'B':2011, 'C':2012, 'D':2013, 'E':2014, 'F':2015, 'G':2016, 'H':2017}
df2['vin10_year'] = df2['vin10'].map(vin10_dict, na_action='ignore')
### 배인번호_수정 문자 타입으로 변경
df2['배출가스인증번호'] = df2['배출가스인증번호'].astype('str')

## 2\. 차대번호 17자리 샘플
df2y = df2.loc[df2['차대번호'].str.len() == 17].reset_index(drop=True)
df2n = df2.loc[df2['차대번호'].str.len() != 17].reset_index(drop=True)

## 3\. 차대번호 연식과 연식 동일한 샘플
df3y = df2y.loc[df2y['vin10_year'] == df2y['차량연식']].reset_index(drop=True)
df3n = df2y.loc[df2y['vin10_year'] != df2y['차량연식']].reset_index(drop=True)

## 4\. 배번, 제번, 제작사명, 차명, 검사방법별 그룹화 and 100대 초과 추출
### 검사판정 Y만 활용
grp4 = df3y[df3y['검사판정'] == 'Y'].groupby(['배출가스인증번호', '제작사명', '차명', '검사방법', '제원관리번호']).agg({'차대번호':'count'}).reset_index()
grp4 = grp4.rename(columns={'차대번호':'차량대수'})
grp4 = grp4[grp4['배출가스인증번호'] != 'nan'].reset_index(drop=True)
### 100대 초과 샘플만 활용
df4 = grp4[grp4['차량대수'] > 100].reset_index(drop=True)

## 5\. 4번 조건에 해당되는 샘플만 추출
# 7m 43s
df5 = pd.DataFrame()
for one, two, three, four, five in df4[['배출가스인증번호', '제원관리번호', '제작사명', '차명', '검사방법']].values:
    temp = df3y[(df3y['검사판정'] == 'Y') & (df3y['배출가스인증번호'] == one) & (df3y['제원관리번호'] == two) & (df3y['제작사명'] == three) & (df3y['차명'] == four) & (df3y['검사방법'] == five)].reset_index(drop=True)
    df5 = pd.concat([df5, temp], ignore_index=True)

## 6\. 5번 데이터셋에서 KPI, 그리드(표), SI(산점도)용 테이블 생성
grp6 = df5.groupby(['배출가스인증번호', '제작사명', '차명', '검사방법', '제원관리번호']).agg({'무부하매연측정치1':[lambda x:x.describe()['25%'], lambda x:x.describe()['50%'], lambda x:x.describe()['75%']], '차대번호':'count'}).reset_index()
grp6.columns = ['배출가스인증번호', '제작사명', '차명', '검사방법', '제원관리번호', 'q1', 'q2', 'q3', '차량대수']

today_date = datetime.today().strftime("%Y%m%d")
grp6['테이블생성일자'] = today_date

STD_BD_GRD4_CAR_CURSTT_TOT = grp6[[
    '테이블생성일자',
    '차명',
    '제작사명', 
    '제원관리번호', 
    '배출가스인증번호', 
    '검사방법', 
    'q1', 
    'q2', 
    'q3',
    '차량대수',
    ]]
chc_dict = {
    '테이블생성일자':'LOAD_DT', 
    '차명':'VHCNM',
    '제작사명':'MNFCTR_NM', 
    '제원관리번호':'MANG_MNG_NO', 
    '배출가스인증번호':'EXHST_GAS_CERT_NO_MOD', 
    '검사방법':'INSP_MTHD', 
    '무부하매연측정치1':'NOLOD_SMO_MEVLU1', 
    'q1':'LOWR_QRT',
    'q2':'MID_QRT',
    'q3':'UP_QRT',
    '차량대수':'VHCL_MKCNT',

    # '차종':'VHCTY_CD', 
    # '용도':'PURPS_CD2', 
    # '차종유형':'CHCTY_TY', 
    # '법정동코드':'STDG_CD', 
    # '검사종류':'INSP_KND', 
    # '검사판정':'INSP_JGMT', 
    # '무부하매연판정1':'NOLOD_SMO_JGMT_YN1',
    # '차대번호':'VIN', 
    # '등급_수정':'EXHST_GAS_GRD_CD_MOD', 
    # 'DPF유무_수정':'DPF_MNTNG_YN', 
    # '시도명':'CTPV_NM', 
    # '시군구명':'SGG_NM', 
    # '차종분류':'VHCTY_CL_CD', 
    }
STD_BD_GRD4_CAR_CURSTT_TOT = STD_BD_GRD4_CAR_CURSTT_TOT.rename(columns=chc_dict)

### [출력] STD_BD_GRD4_CAR_CURSTT_TOT
# expdf = STD_BD_GRD4_CAR_CURSTT_TOT
# table_nm = 'STD_BD_GRD4_CAR_CURSTT_TOT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     if 'float' in expdf[column].dtype.name:
#         sql += column + ' float'
#     elif 'int' in expdf[column].dtype.name:
#         sql += column + ' number'
#     else:
#         sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 1s
# we.import_from_pandas(expdf, table_nm)

sidf = df5[[
    '차대번호', 
    '제원관리번호', 
    '차종', 
    '차량연식', 
    '차명', 
    '차종유형', 
    '제작사명', 
    '연료', 
    '법정동코드', 
    '배출가스인증번호', 
    '검사방법', 
    '검사종류', 
    '검사판정', 
    '주행거리', 
    '무부하매연판정1', 
    '무부하매연허용치1', 
    '무부하매연측정치1'
    ]]

current_yr = int(datetime.today().strftime("%Y"))
sidf['차령'] = current_yr - sidf['차량연식']
sidf['SI'] = sidf['무부하매연측정치1'] / sidf['무부하매연허용치1']

sidf['테이블생성일자'] = today_date
sidf1 = sidf[[
    '테이블생성일자', 
    '차대번호', 
    '제원관리번호', 
    '차명', 
    '제작사명', 
    '배출가스인증번호', 
    '검사방법',
    '주행거리', 
    '차령',
    'SI', 
    ]]
chc_dict = {
    '테이블생성일자':'LOAD_DT',
    '차대번호':'VIN', 
    '제원관리번호':'MANG_MNG_NO', 
    '차명':'VHCNM',
    '제작사명':'MNFCTR_NM', 
    '배출가스인증번호':'EXHST_GAS_CERT_NO_MOD', 
    '검사방법':'INSP_MTHD', 
    '주행거리':'DRVNG_DSTNC',
    '차령':'VHCAG',
    
    # '차종':'VHCTY_CD', 
    # '연식':'YRIDNW', 
    # '차종유형':'VHCTY_TY', 
    # '연료':'FUEL_CD',
    # '법정동코드':'STDG_CD', 
    # '검사종류':'INSP_KND', 
    # '검사판정':'INSP_JGMT', 
    # '무부하매연판정1':'NOLOD_SMO_JGMT_YN1',
    # '무부하매연허용치1':'NOLOD_SMO_PRMT_VAL1',
    # '무부하매연측정치1':'NOLOD_SMO_MEVLU1', 
    }
STD_BD_GRD4_SI = sidf1.rename(columns=chc_dict)

### [출력] STD_BD_GRD4_SI
# expdf = STD_BD_GRD4_SI
# table_nm = 'STD_BD_GRD4_SI'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 6s
# we.import_from_pandas(expdf, table_nm)

## 7\. 5번 데이터셋에서 DAT용 (검토구분 계산) 테이블 생성
### 검토구분(양호/주의 판정) 
# grp6 : df5.groupby(['배출가스인증번호', '제작사명', '차명', '검사방법', '제원관리번호'])
grp7 = grp6.copy()
grp7['q2_mean'] = grp7.groupby(['배출가스인증번호', '제작사명', '차명', '검사방법'])['q2'].transform('mean')
grp7.loc[(grp7['q2'] > grp7['q2_mean']*5) | (grp7['q2'] < grp7['q2_mean']/5), '검토구분'] = '주의'
grp7['검토구분'] = grp7['검토구분'].fillna('양호')

STD_BD_DAT_GRD4_CERT_NO_RVW = grp7[[
    '배출가스인증번호',
    '검사방법',
    '검토구분',
    '제작사명',
    '차명',
    '제원관리번호',
    'q1',
    'q2',
    'q3',
    '테이블생성일자',
]]
cdict = {
    '배출가스인증번호':'EXHST_GAS_CERT_NO',
    '검사방법':'INSP_MTHD',
    '검토구분':'RVW_SE',
    '제작사명':'RPRS_MNFCTR_NM',
    '차명':'RPRS_VHCNM', 
    '제원관리번호':'MANG_MNG_NO',
    'q1':'LOWR_QRT',
    'q2':'MID_QRT',
    'q3':'UP_QRT',
    '테이블생성일자':'LOAD_DT',
}
STD_BD_DAT_GRD4_CERT_NO_RVW = STD_BD_DAT_GRD4_CERT_NO_RVW.rename(columns=cdict)

### [출력] STD_BD_DAT_GRD4_CERT_NO_RVW
# expdf = STD_BD_DAT_GRD4_CERT_NO_RVW
# table_nm = 'STD_BD_DAT_GRD4_CERT_NO_RVW'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     if 'float' in expdf[column].dtype.name:
#         sql += column + ' float'
#     elif 'int' in expdf[column].dtype.name:
#         sql += column + ' number'
#     else:
#         sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 1s
# we.import_from_pandas(expdf, table_nm)

# 조건에 맞는 샘플 df에 '검토구분' 정보 추가
# sidf.merge(grp7, on=['배출가스인증번호', '제원관리번호', '제작사명', '차명', '검사방법'])
grp_sidf = sidf.groupby(['배출가스인증번호', '제작사명', '차명', '검사방법', '제원관리번호']).agg({'차종':lambda x:x.value_counts().index[0], '연료':lambda x:x.value_counts().index[0], '차량연식':lambda x : x.nsmallest(1), 'SI':'mean'}).reset_index()
grp_sidf = grp_sidf.rename(columns={'차량연식':'최초연식', 'SI':'열화도'})
df71 = grp_sidf.merge(grp7[['배출가스인증번호', '제원관리번호', '제작사명', '차명', '검사방법', '검토구분']], on=['배출가스인증번호', '제원관리번호', '제작사명', '차명', '검사방법'], how='left')
df71['검토구분'].value_counts(dropna=False)
df71['테이블생성일자'] = today_date
STD_BD_DAT_GRD4_SI = df71[[
    '배출가스인증번호',
    '검사방법', 
    '검토구분',
    '제작사명',
    '차명',
    '차종', 
    '연료',
    '최초연식',
    '열화도',
    '테이블생성일자'
]]
# cdict = {
#     '배출가스인증번호':'EXHST_GAS_CERT_NO', 
#     '검토구분':'RVW_SE', 
#     '대표제작사명':'RPRS_MNFCTR_NM', 
#     '대표차명':'RPRS_VHCNM', 
#     '대표차종':'RPRS_VHCTY_CD', 
#     '대표차연료':'RPRS_FUEL', 
#     '최초연식':'FRST_YRIDNW', 
#     '열화도':'SI', 
#     '테이블생성일자':'LOAD_DT', 
#     # '검사방법':'INSP_MTHD', 
# }
cdict = {
    '배출가스인증번호':'EXHST_GAS_CERT_NO', 
    '검사방법':'INSP_MTHD', 
    '검토구분':'RVW_SE', 
    '제작사명':'RPRS_MNFCTR_NM', 
    '차명':'RPRS_VHCNM', 
    '차종':'RPRS_VHCTY_CD', 
    '연료':'RPRS_FUEL', 
    '최초연식':'FRST_YRIDNW', 
    '열화도':'SI', 
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_GRD4_SI = STD_BD_DAT_GRD4_SI.rename(columns=cdict)

### [출력] STD_BD_DAT_GRD4_SI
# expdf = STD_BD_DAT_GRD4_SI
# table_nm = 'STD_BD_DAT_GRD4_SI'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     if 'float' in expdf[column].dtype.name:
#         sql += column + ' float'
#     elif 'int' in expdf[column].dtype.name:
#         sql += column + ' number'
#     else:
#         sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 1s
# we.import_from_pandas(expdf, table_nm)

## !!! 수정 끝(2023.08.24)



## 1-1 code end ##################################################################

## 1-2 start

# 전체 4등급 등록&제원 병합
cse = carr.merge(srcr, on='제원관리번호', how='left')

cse['법정동코드'] = cse['법정동코드'].astype('str')
cse['법정동코드'] = cse['법정동코드'].str[:5] + '00000'
cse['법정동코드'] = pd.to_numeric(cse['법정동코드'])

# 시도, 시군구 추가
csec = cse.merge(coder, on='법정동코드', how='left')

# csec['시도'].isnull().sum()

# ### 매칭 안되는 지역 처리
# # 주소 수정
# csec.loc[csec['법정동코드'] == 5172035031, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 강원특별자치도 홍천군
# csec.loc[csec['법정동코드'] == 5180031023, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csec.loc[csec['법정동코드'] == 5180031031, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csec.loc[csec['법정동코드'] == 5172035030, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csec.loc[csec['법정동코드'] == 5180031028, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 

# csec.loc[csec['법정동코드'] == 5172035021, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csec.loc[csec['법정동코드'] == 5180031025, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csec.loc[csec['법정동코드'] == 4165052000, ['시도', '시군구']] = ['경기도', '포천시'] # 경기도 포천시 선단동
# csec.loc[csec['법정동코드'] == 5172035023, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csec.loc[csec['법정동코드'] == 5180031027, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 

# csec.loc[csec['법정동코드'] == 5172035024, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csec.loc[csec['법정동코드'] == 5175037022, ['시도', '시군구']] = ['강원특별자치도', '영월군'] # 
# csec.loc[csec['법정동코드'] == 4163055000, ['시도', '시군구']] = ['경기도', '양주시'] # 경기도 양주시 회천3동
# csec.loc[csec['법정동코드'] == 5180031033, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 경기도 양주시 회천3동

# 조기폐차 추가
dfe = csec.merge(elpm, on='차대번호', how='left')
df1 = dfe[dfe['연료'] == '경유'].reset_index(drop=True)

# 기준연월 추가
df1['기준연월'] = '2022.12'
today_date = datetime.today().strftime("%Y%m%d")
# df1['기준연월'] = today_date[:4] + '.' + today_date[4:6]

STD_BD_GRD4_ELPDSRC_CURSTT = df1[[
    '기준연월',
    '차대번호', 
    '법정동코드', 
    '차종', 
    '용도', 
    '연료', 
    '차종유형', 
    '시도',
    '시군구', 
    '조기폐차상태코드', 
    '조기폐차최종승인YN',
]]
today_date = datetime.today().strftime("%Y%m%d")
STD_BD_GRD4_ELPDSRC_CURSTT['테이블생성일자'] = today_date
STD_BD_GRD4_ELPDSRC_CURSTT = STD_BD_GRD4_ELPDSRC_CURSTT[[
    '기준연월',
    '차대번호', 
    '법정동코드', 
    '차종', 
    '용도', 
    '연료', 
    '차종유형', 
    '시도', 
    '시군구', 
    '조기폐차상태코드',
    '조기폐차최종승인YN', 
    '테이블생성일자', 
]]
chc_dict = {
    '기준연월':'CRTR_YM', 
    '차대번호':'VIN', 
    '법정동코드':'STDG_CD', 
    '차종':'VHCTY_CD', 
    '용도':'PURPS_CD2', 
    '연료':'FUEL_CD', 
    '차종유형':'VHCTY_TY', 
    '시도':'CTPV', 
    '시군구':'SGG', 
    '조기폐차상태코드':'ELPDSRC_STTS_CD',
    '조기폐차최종승인YN':'ELPDSRC_LAST_APRV_YN', 
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_GRD4_ELPDSRC_CURSTT = STD_BD_GRD4_ELPDSRC_CURSTT.rename(columns=chc_dict)

# STD_BD_GRD4_ELPDSRC_CURSTT.columns

### [출력] STD_BD_GRD4_ELPDSRC_CURSTT

# expdf = STD_BD_GRD4_ELPDSRC_CURSTT
# table_nm = 'STD_BD_GRD4_ELPDSRC_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_ELPDSRC_CURSTT')

## 등록&제원&저감이력 병합
# 1.7s
csa = cs.merge(attr[['차대번호', 'DPF_YN']], on='차대번호', how='left')

csa['법정동코드'] = csa['법정동코드'].astype('str')
csa['법정동코드'] = csa['법정동코드'].str[:5] + '00000'
csa['법정동코드'] = pd.to_numeric(csa['법정동코드'])

## 지역정보 병합
df = csa.merge(coder, on='법정동코드', how='left')

# df['시도'].isnull().sum()

# ### 매칭 안되는 지역 처리
# # 경기도 양주시
# df.loc[df['법정동코드'] == 4163055000, ['시도', '시군구']] = ['경기도', '양주시']

# df.loc[df['시도'].isnull(), '법정동코드'].unique()

df['법정동코드_mod'] = df['법정동코드'].copy()

# ### rh 법정동코드 참고하여 법정동코드 수정
# rdf = df.copy()
# rdf['법정동코드'] = rdf['법정동코드'].astype('str')
# rdf['법정동코드_mod'] = rdf['법정동코드'].str[:5] + '00000'
# rdf['법정동코드_mod'] = pd.to_numeric(rdf['법정동코드_mod'])
# # 2817000000 인천광역시 남구 -> 인천광역시 미추홀구 2817700000
# # 4119500000 경기도 부천시 원미구 -> 경기도 부천시 4119000000
# # 4119700000 경기도 부천시 소사구 -> 경기도 부천시 4119000000
# # 4119900000 경기도 부천시 오정구 -> 경기도 부천시 4119000000
# # 4173000000 경기도 여주군 -> 경기도 여주시 4167000000
# # 4371000000 충청북도 청원군 -> 충청북도 충주시 4311000000
# rdf.loc[rdf['법정동코드_mod'] == 2817000000, '법정동코드_mod'] = 2817700000
# rdf.loc[rdf['법정동코드_mod'] == 4119500000, '법정동코드_mod'] = 4119000000
# rdf.loc[rdf['법정동코드_mod'] == 4119700000, '법정동코드_mod'] = 4119000000
# rdf.loc[rdf['법정동코드_mod'] == 4119900000, '법정동코드_mod'] = 4119000000
# rdf.loc[rdf['법정동코드_mod'] == 4173000000, '법정동코드_mod'] = 4167000000
# rdf.loc[rdf['법정동코드_mod'] == 4371000000, '법정동코드_mod'] = 4311000000
# df = rdf.copy()

## 4등급 result 파일 참고하여 DPF유무 수정
rdf = df.copy()
rs = rs.drop_duplicates('차대번호').reset_index(drop=True)
rdf1 = rdf.merge(rs, on='차대번호', how='left')
rdf1.loc[(rdf1['DPF_YN'] == '유') | (rdf1['DPF유무_수정'] == '유'), 'DPF_YN'] = '유'
rdf1.loc[(rdf1['DPF유무_수정'] == '무'), 'DPF_YN'] = '무'
rdf1.loc[(rdf1['DPF유무_수정'] == '확인불가'), 'DPF_YN'] = '확인불가'
df = rdf1.drop('DPF유무_수정', axis=1)

## 연료 컬럼 추가
df1 = df.copy()
df1.loc[df1['연료'] == '경유', 'fuel'] = '경유'
df1.loc[(df1['연료'] == '휘발유') | (df1['연료'] == 'LPG(액화석유가스)'), 'fuel'] = '휘발유_가스'

# 분석
## EG 분류
grade_list = []
for f, y, cy, e in df1[['fuel', '제작일자', '차량연식', 'DPF_YN']].values:
    if (f == '휘발유_가스') and ( (19980101 <= y <= 20001231) or (1998 <= cy <= 2000) ):
        grade_list.append('A')
    elif (f == '휘발유_가스') and ( (y <= 19971231) or (cy <= 1997) ):
        grade_list.append('B')
    elif (f == '경유') and ( (y >= 20080101) or (cy >= 2008) ) and (e == '유'):
        grade_list.append('A')
    elif (f == '경유') and ( (y <= 20071231) or (cy <= 2007) )and (e == '유'):
        grade_list.append('B')
    elif (f == '경유') and ( (y >= 20080101) or (cy >= 2008) ) and (e == '무'):
        grade_list.append('C')
    elif (f == '경유') and ( (y <= 20071231) or (cy <= 2007) ) and (e == '무'):
        grade_list.append('D')
    else:
        grade_list.append('X')
df1['Grade'] = grade_list

STD_BD_GRD4_MLSFC_RSLT = df1[[
    '차대번호', 
    '제원관리번호',
    '차종', 
    '용도', 
    '차량연식', 
    '차종유형', 
    '연료', 
    '법정동코드', 
    '시도', 
    '시군구', 
    'DPF_YN',
    'Grade',
    '법정동코드_mod',
    ]]

today_date = datetime.today().strftime("%Y%m%d")
STD_BD_GRD4_MLSFC_RSLT['테이블생성일자'] = today_date
STD_BD_GRD4_MLSFC_RSLT = STD_BD_GRD4_MLSFC_RSLT[[
    '테이블생성일자', 
    '차대번호', 
    '제원관리번호', 
    '차종', 
    '용도', 
    '차량연식', 
    '차종유형', 
    '연료', 
    '법정동코드', 
    '시도',
    '시군구', 
    'DPF_YN', 
    'Grade', 
    '법정동코드_mod',
    ]]

# RH법정동코드 문자형으로 변환
STD_BD_GRD4_MLSFC_RSLT['법정동코드_mod'] = STD_BD_GRD4_MLSFC_RSLT['법정동코드_mod'].astype('str')

ch_col_dict = {
                '테이블생성일자':'LOAD_DT',
                '차대번호':'VIN', 
                '제원관리번호':'MANG_MNG_NO',
                '차종':'VHCTY_CD', 
                '용도':'PURPS_CD2',
                '차량연식':'YRIDNW', 
                '차종유형':'VHCTY_TY', 
                '연료':'FUEL_CD', 
                '법정동코드':'STDG_CD', 
                '시도':'CTPV_NM',
                '시군구':'SGG_NM',
                'DPF_YN':'DPF_MNTNG_YN',
                'Grade':'GRD4_MLSFC', 
                '법정동코드_mod':'STDG_CD_MOD'
                }
STD_BD_GRD4_MLSFC_RSLT = STD_BD_GRD4_MLSFC_RSLT.rename(columns=ch_col_dict)

# STD_BD_GRD4_MLSFC_RSLT.columns

### [출력] STD_BD_GRD4_MLSFC_RSLT

# expdf = STD_BD_GRD4_MLSFC_RSLT
# table_nm = 'STD_BD_GRD4_MLSFC_RSLT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_MLSFC_RSLT')

## 4등급 등급세분류
dat_mlsfc = df1.copy()
dat_mlsfc['시군구_수정'] = dat_mlsfc['시군구'].str.split(' ').str[0]
grp1 = dat_mlsfc.groupby(['연료', '시도', '시군구_수정', '차종', '차종유형', '용도', 'Grade'])['차대번호'].count().unstack('Grade').reset_index()

# 연도 설정
grp1['연도'] = '2022'
today_date = datetime.today().strftime("%Y%m%d")
# grp1['연도'] = today_date[:4]
grp1['테이블생성일자'] = today_date

STD_BD_DAT_GRD4_MLSFC = grp1[[
    '연도', 
    '연료', 
    '시도', 
    '시군구_수정', 
    '차종', 
    '차종유형', 
    '용도', 
    'A', 
    'B', 
    'C', 
    'D', 
    'X',
    '테이블생성일자',
]]
cdict = {
    '연도':'YR', 
    '연료':'FUEL_CD', 
    '시도':'CTPV', 
    '시군구_수정':'SGG', 
    '차종':'VHCTY_CD', 
    '차종유형':'VHCTY_TY', 
    '용도':'PURPS_CD2', 
    'A':'A_MKCNT', 
    'B':'B_MKCNT', 
    'C':'C_MKCNT', 
    'D':'D_MKCNT', 
    'X':'X_MKCNT', 
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_GRD4_MLSFC = grp1.rename(columns=cdict)

### [출력] STD_BD_DAT_GRD4_MLSFC

# expdf = STD_BD_DAT_GRD4_MLSFC
# table_nm = 'STD_BD_DAT_GRD4_MLSFC'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_GRD4_MLSFC')

## 4등급차량 상세정보
cst = carr.merge(srcr, on='제원관리번호', how='left')
csat = cst.merge(attr, on='차대번호', how='left')

csat['법정동코드'] = csat['법정동코드'].astype('str')
csat['법정동코드'] = csat['법정동코드'].str[:5] + '00000'
csat['법정동코드'] = pd.to_numeric(csat['법정동코드'])

csact = csat.merge(coder, on='법정동코드', how='left')

# csact['시도'].isnull().sum()

# # 경기도 양주시
# csact.loc[csact['법정동코드'] == 4163055000, ['시도', '시군구']] = ['경기도', '양주시']

### 조기폐차 정보추가
dft = csact.merge(elpm, on='차대번호', how='left')

### 4등급 result 파일 참고하여 DPF유무 수정
rdf = dft.copy()
rs = rs.drop_duplicates('차대번호').reset_index(drop=True)
rdf1 = rdf.merge(rs, on='차대번호', how='left')
rdf1.loc[(rdf1['DPF_YN'] == '유') | (rdf1['DPF유무_수정'] == '유'), 'DPF_YN'] = '유'
rdf1.loc[(rdf1['DPF유무_수정'] == '무'), 'DPF_YN'] = '무'
rdf1.loc[(rdf1['DPF유무_수정'] == '확인불가'), 'DPF_YN'] = '확인불가'
dft = rdf1.drop('DPF유무_수정', axis=1)

# 4등급 연월, 시도, 시군구별 차량대수
## 등록 & 제원 정보 병합(말소 유지)
csersr = carr.merge(srcr, on='제원관리번호', how='left')

## 1\. 차량관리번호 기준 병합
# 58.3s
ersr = csersr.merge(hisr, on='차량관리번호', how='left')

## 2\. 차량말소YN 만 추출
errm = ersr[(ersr['차량말소YN_x'] == 'Y') & (ersr['차량말소YN_y'] == 'Y')].reset_index(drop=True)

## 3\. 변경일자 최신으로 차대번호 중복 제거
errm = errm.sort_values('변경일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

## 4\. 변경일자 2019.01.01 이상만 추출
errm = errm[errm['변경일자'] >= 20190101].reset_index(drop=True)

## 5\. 법정동 코드 정보 병합
errm['법정동코드'] = errm['법정동코드'].astype('str')
errm['법정동코드'] = errm['법정동코드'].str[:5] + '00000'
errm['법정동코드'] = pd.to_numeric(errm['법정동코드'])
errc = errm.merge(coder, on='법정동코드', how='left')

# errc['시도'].isnull().sum()

dfte = dft.merge(errc[['차대번호', '변경일자']], on='차대번호', how='left')
dftem = dfte.merge(df1[['차대번호', 'Grade']], on='차대번호', how='left')

# dftem.columns

today_date = datetime.today().strftime("%Y%m%d")
dftem['테이블생성일자'] = today_date
STD_BD_DAT_GRD4_DTL_INFO = dftem[[
    '자동차등록번호',
    '차대번호',
    'Grade',
    '차종',
    '차종유형',
    '용도',
    '연료',
    '시도',
    '시군구',
    '차량연식',
    'DPF_YN',
    '저감장치종류',
    '최초등록일자',
    '조기폐차신청여부',
    '조기폐차상태코드',
    '변경일자',
    '차량말소YN', 
    '테이블생성일자', 
    # '법정동코드',
    # '배출가스등급',
    # '배출가스인증번호',
    # '제원관리번호',
    # '제작일자',
    # '차량관리번호',
    # '제작사명',
    # '차명',
    # '자동차형식',
    # '엔진형식',
    # '저감장치구분',
    # '조기폐차최종승인YN',
]]

cdict = {
    '자동차등록번호':'VHRNO',
    '차대번호':'VIN',
    'Grade':'GRD4_MLSFC',
    '차종':'VHCTY_CD',
    '차종유형':'VHCTY_TY',
    '용도':'PURPS_CD2',
    '연료':'FUEL_CD',
    '시도':'CTPV',
    '시군구':'SGG',
    '차량연식':'YRIDNW',
    'DPF_YN':'DPF_MNTNG_YN',
    '저감장치종류':'RDCDVC_KND',
    '최초등록일자':'FRST_REG_YMD',
    '조기폐차신청여부':'ELPDSRC_APLY_YN',
    '조기폐차상태코드':'ELPDSRC_STTS_CD',
    '변경일자':'CHNG_DE',
    '차량말소YN':'VHCL_ERSR_YN',
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_GRD4_DTL_INFO = STD_BD_DAT_GRD4_DTL_INFO.rename(columns=cdict)

# STD_BD_DAT_GRD4_DTL_INFO.columns

### [출력] STD_BD_DAT_GRD4_DTL_INFO

# expdf = STD_BD_DAT_GRD4_DTL_INFO
# table_nm = 'STD_BD_DAT_GRD4_DTL_INFO'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_GRD4_DTL_INFO')

## 시도, 연도별 차량 대수
dfm = df.copy()
dfm['최초등록일자'] = dfm['최초등록일자'].astype('str')
dfm['최초등록일자_년'] = dfm['최초등록일자'].str[:4]
dfm['최초등록일자_월'] = dfm['최초등록일자'].str[4:6]
dfm['최초등록일자_일'] = dfm['최초등록일자'].str[6:8]
dfm['최초등록일자'] = dfm['최초등록일자_년'] + dfm['최초등록일자_월'] + dfm['최초등록일자_일']
dfm['최초등록일자'] = pd.to_numeric(dfm['최초등록일자'], errors='coerce')

### 시군구명 앞쪽 지역명만 남기기(dfm)
dfm['시군구_수정'] = dfm['시군구'].str.split(' ').str[0]

### 연료 지역별 차량대수
num_car_by_local1 = dfm.groupby(['연료', '시도', '시군구_수정'], dropna=False)['차대번호'].count().reset_index()
num_car_by_local1 = num_car_by_local1.rename(columns={'차대번호':'차량대수'})

# max_date = str(dfm['최초등록일자'].max())
# max_year = max_date[:4]
# max_month = max_date[4:6]

# date = '20220601'
# max_year = '2022'
# max_month = '06'
date = today_date # !!! 수정(2023.08.23)
max_year = today_date[:4] # !!! 수정(2023.08.23)
max_month = today_date[4:6] # !!! 수정(2023.08.23)

num_car_by_local1[['연도', '월']] = [max_year, max_month]

### 연료 지역별 등록차량대수
num_car_by_local2 = dfm.groupby(['연료', '시도', '시군구_수정', '최초등록일자_년', '최초등록일자_월'], as_index=False)['차대번호'].count()
num_car_by_local2 = num_car_by_local2.rename(columns={'차대번호':'등록차량대수', '최초등록일자_년':'연도', '최초등록일자_월':'월'})

### 연료 지역별 말소 대수
errc['변경일자'] = errc['변경일자'].astype('str')
errc['변경일자_년'] = errc['변경일자'].str[:4]
errc['변경일자_월'] = errc['변경일자'].str[4:6]
errc['변경일자_일'] = errc['변경일자'].str[6:8]

### 시군구명 앞쪽 지역명만 남기기(errc)
errc['시군구_수정'] = errc['시군구'].str.split(' ').str[0]
grp_erase = errc.groupby(['변경일자_년', '변경일자_월', '연료', '시도', '시군구_수정'], as_index=False)['차대번호'].count() # !!! 수정(2023.08.23)
grp_erase = grp_erase.rename(columns={'차대번호':'말소차량대수', '변경일자_년':'연도', '변경일자_월':'월'})
grp_erase = grp_erase.sort_values(['시도', '시군구_수정'])

periods = 12 # !!! 수정(2023.08.23)
y_plist = list(pd.date_range(end=date, periods=periods, freq="MS").year) # !!! 수정(2023.08.23)
mth_plist = list(pd.date_range(end=date, periods=periods, freq="MS").month) # !!! 수정(2023.08.23)

# y_plist, mth_plist

yr_list, mth_list, fuel_list, ctpv_list, sgg_list = [], [], [], [], [] # !!! 수정(2023.08.23)
sl = num_car_by_local1.drop_duplicates(['시도', '시군구_수정']).reset_index(drop=True)
for ctpv, sgg in sl[['시도', '시군구_수정']].values:
    for fuel in sl['연료'].unique():
        for yr, mth in zip(y_plist, mth_plist):
            mthm = f'{mth:0>2}'
            yr_list.append(str(yr))
            mth_list.append(mthm)
            fuel_list.append(fuel)
            ctpv_list.append(ctpv)
            sgg_list.append(sgg)
base = pd.DataFrame({'연도':yr_list, '월':mth_list, '연료':fuel_list, '시도':ctpv_list, '시군구_수정':sgg_list})

base1 = base.merge(num_car_by_local1, on=['연도', '월', '연료', '시도', '시군구_수정'], how='left')
base2 = base1.merge(num_car_by_local2, on=['연도', '월', '연료', '시도', '시군구_수정'], how='left')
base3 = base2.merge(grp_erase, on=['연도', '월', '연료', '시도', '시군구_수정'], how='left')
base3[['차량대수', '등록차량대수', '말소차량대수']] = base3[['차량대수', '등록차량대수', '말소차량대수']].fillna(0)

n = periods # !!! 수정(2023.08.23)
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소차량대수'] - base3.loc[(i+1)*n - (j-1), '등록차량대수']

today_date = datetime.today().strftime("%Y%m%d")
base3['테이블생성일자'] = today_date
base3['기준연월'] = base3['연도'] + '.' + base3['월']
base4 = base3[[
    '테이블생성일자', 
    '기준연월',
    '연도',
    '월', 
    '연료', 
    '시도', 
    '시군구_수정', 
    '차량대수',
]]
chc_col = {
    '테이블생성일자':'LOAD_DT', 
    '기준연월':'CRTR_YM',
    '연도':'YR', 
    '월':'MM', 
    '연료':'FUEL_CD', 
    '시도':'CTPV', 
    '시군구_수정':'SGG', 
    '차량대수':'VHCL_MKCNT', 
}
STD_BD_GRD4_RGN_CURSTT = base4.rename(columns=chc_col)

# STD_BD_GRD4_RGN_CURSTT.columns

### [출력] STD_BD_GRD4_RGN_CURSTT

# expdf = STD_BD_GRD4_RGN_CURSTT
# table_nm = 'STD_BD_GRD4_RGN_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_RGN_CURSTT')

## 4등급 연도, 시도, 차종별 차량 대수
### 현재 차량 대수
num_car_by_local1 = dfm.groupby(['시도', '차종'], dropna=False)['차대번호'].count().reset_index()
num_car_by_local1 = num_car_by_local1.rename(columns={'차대번호':'차량대수'})
num_car_by_local1['연도'] = max_year

### 등록 차량 대수
num_car_by_local2 = dfm.groupby(['시도', '차종', '최초등록일자_년'], as_index=False)['차대번호'].count()
num_car_by_local2 = num_car_by_local2.rename(columns={'차대번호':'등록차량대수', '최초등록일자_년':'연도'})

### 말소 차량 대수
grp_erase = errc.groupby(['변경일자_년', '시도', '차종'], as_index=False)['차대번호'].count()
grp_erase = grp_erase.rename(columns={'차대번호':'말소차량대수', '변경일자_년':'연도'})
grp_erase = grp_erase.sort_values(['시도'])

y_plist = list(pd.date_range(end=date, periods=4, freq="YS").year)

# y_plist

yr_list = []
fuel_list = []
ctpv_list = []
cd_list = []
for ctpv in num_car_by_local1['시도'].unique():
    for cd in ['승용', '승합', '화물', '특수']:
        for yrm in y_plist:
            yr_list.append(str(yrm))
            fuel_list.append(fuel)
            ctpv_list.append(ctpv)
            cd_list.append(cd)
base = pd.DataFrame({'연도':yr_list, '시도':ctpv_list, '차종':cd_list})

base1 = base.merge(num_car_by_local1, on=['연도', '시도', '차종'], how='left')
base2 = base1.merge(num_car_by_local2, on=['연도', '시도', '차종'], how='left')
base3 = base2.merge(grp_erase, on=['연도', '시도', '차종'], how='left')
base3[['차량대수', '등록차량대수', '말소차량대수']] = base3[['차량대수', '등록차량대수', '말소차량대수']].fillna(0)

n = len(base3['연도'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소차량대수'] - base3.loc[(i+1)*n - (j-1), '등록차량대수']

today_date = datetime.today().strftime("%Y%m%d")
base3['테이블생성일자'] = today_date
base4 = base3[[
    '테이블생성일자', 
    '연도', 
    '시도', 
    '차종', 
    '차량대수', 
]]
chc_col = {
    '테이블생성일자':'LOAD_DT',
    '연도':'CRTR_Y', 
    '시도':'CTPV', 
    '차종':'VHCTY_CD', 
    '차량대수':'VHCL_MKCNT', 
}
STD_BD_GRD4_RGN_CURSTT_MOD = base4.rename(columns=chc_col)

### [출력] STD_BD_GRD4_RGN_CURSTT_MOD

# expdf = STD_BD_GRD4_RGN_CURSTT_MOD
# table_nm = 'STD_BD_GRD4_RGN_CURSTT_MOD'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_RGN_CURSTT_MOD')

## 4등급 차량현황(그룹)
# - 연도, 월, 시도, 시군구, 연료, 차종, 차종유형, 용도
### 현재 차량 대수
# num_car_by_local1 = dfm.groupby(['시도', '시군구_수정', '연료', '차종', '차종유형', '용도'], dropna=False, as_index=False).agg({'차대번호':'count', 'DPF_YN':'count', '조기폐차최종승인YN':'count'})
# num_car_by_local1 = num_car_by_local1.rename(columns={'차대번호':'차량대수', 'DPF_YN':'저감장치부착대수', '조기폐차최종승인YN':'조기폐차대수'})
# num_car_by_local1['저감장치미부착대수'] = num_car_by_local1['차량대수'] - num_car_by_local1['저감장치부착대수']

# max_date = str(dfm['최초등록일자'].max())
# max_year = max_date[:4]
# max_month = max_date[4:6]
# num_car_by_local1[['연도', '월']] = [max_year, max_month]

# if len(num_car_by_local1['월'].unique()) != 1:
#     # 오름차순 정렬
#     num_car_by_local1 = num_car_by_local1.sort_values(['시도', '시군구_수정', '연료', '차종', '차종유형', '용도', '연도', '월']).reset_index(drop=True)
#     num_car_by_local1['차량대수_전월'] = num_car_by_local1['차량대수'].shift(1)
#     num_car_by_local1['감소율'] = (num_car_by_local1['차량대수_전월'] - num_car_by_local1['차량대수']) / num_car_by_local1['차량대수_전월']
#     for n in range(num_car_by_local1.shape[0] // len(num_car_by_local1['월'].unique())):
#         num_car_by_local1.loc[n*3, '감소율'] = np.nan
# else:
#     num_car_by_local1['감소율'] = np.nan

# today_date = datetime.today().strftime("%Y%m%d")
# num_car_by_local1['테이블생성일자'] = today_date

cse = carr.merge(srcr, on='제원관리번호', how='left')
ce = cse.merge(elpm, on='차대번호', how='left')
cea = ce.merge(attr, on='차대번호', how='left')

cea['법정동코드'] = cea['법정동코드'].astype('str')
cea['법정동코드'] = cea['법정동코드'].str[:5] + '00000'
cea['법정동코드'] = pd.to_numeric(cea['법정동코드'])

dfe = cea.merge(coder, on='법정동코드', how='left')

# dfe['시도'].isnull().sum()

# dfe.loc[dfe['법정동코드'] == 4163055000, ['시도', '시군구']] = ['경기도', '양주시']

dfe['시군구_수정'] = dfe['시군구'].str.split(' ').str[0]

ere = errc.merge(elpm, on='차대번호', how='left')
erea = ere.merge(attr, on='차대번호', how='left')

# erea.columns

rdf = dfe.copy()
rs = rs.drop_duplicates('차대번호').reset_index(drop=True)

rdf1 = rdf.merge(rs, on='차대번호', how='left')
rdf1.loc[(rdf1['DPF_YN'] == '유') | (rdf1['DPF유무_수정'] == '유'), 'DPF_YN'] = '유'
rdf1.loc[(rdf1['DPF유무_수정'] == '무'), 'DPF_YN'] = '무'
rdf1.loc[(rdf1['DPF유무_수정'] == '확인불가'), 'DPF_YN'] = '확인불가'
dfe = rdf1.drop('DPF유무_수정', axis=1)

dfe['연도'] = max_year
dfe['월'] = max_month

dfe['최초등록일자'] = dfe['최초등록일자'].astype('str')
dfe['최초등록일자_년'] = dfe['최초등록일자'].str[:4]
dfe['최초등록일자_월'] = dfe['최초등록일자'].str[4:6]
dfe['최초등록일자_일'] = dfe['최초등록일자'].str[6:8]

dfe.loc[dfe['DPF_YN'] == '유', '저감장치부착유무'] = 'Y'
erea.loc[erea['DPF_YN'] == '유', '저감장치부착유무'] = 'Y'

dfe['말소일자'] = dfe['말소일자'].astype('str')
dfe['말소일자_년'] = dfe['말소일자'].str[:4]
dfe['말소일자_월'] = dfe['말소일자'].str[4:6]
dfe['말소일자_일'] = dfe['말소일자'].str[6:8]

erea['말소일자'] = erea['말소일자'].astype('str')
erea['말소일자_년'] = erea['말소일자'].str[:4]
erea['말소일자_월'] = erea['말소일자'].str[4:6]
erea['말소일자_일'] = erea['말소일자'].str[6:8]

# 2022년 차량 대수
grp1 = dfe[dfe['차량말소YN'] == 'N'].groupby(['연도', '월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도']).agg({'차대번호':'count', '저감장치부착유무':'count'}).reset_index()
grp1 = grp1.rename(columns={'차대번호':'차량대수', '저감장치부착유무':'저감대수'})

# 연도별 등록대수
grp2 = dfe[dfe['차량말소YN'] == 'N'].groupby(['최초등록일자_년', '최초등록일자_월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도']).agg({'차대번호':'count', '저감장치부착유무':'count'}).reset_index()
grp2 = grp2.rename(columns={'차대번호':'등록대수', '저감장치부착유무':'등록저감대수', '최초등록일자_년':'연도', '최초등록일자_월':'월'})

# 연도별 말소대수
grp3 = erea.groupby(['변경일자_년', '변경일자_월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도']).agg({'차대번호':'count', '저감장치부착유무':'count'}).reset_index()
grp3 = grp3.rename(columns={'차대번호':'말소대수', '저감장치부착유무':'말소저감대수', '변경일자_년':'연도', '변경일자_월':'월'})

# 연도별 조기폐차 대수
grp4 = dfe.groupby(['말소일자_년', '말소일자_월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도']).agg({'조기폐차최종승인YN':'count'}).reset_index()
grp4 = grp4.rename(columns={'말소일자_년':'연도', '말소일자_월':'월', '조기폐차최종승인YN':'조기폐차'})

y_plist = list(pd.date_range(end=date, periods=4, freq="MS").year)
mth_plist = list(pd.date_range(end=date, periods=4, freq="MS").month)

# y_plist, mth_plist

# 
# 4개월 차량 통계 기본 데이터셋
ctpv_list, sgg_list, fuel_list, vhcty_list, ty_list, purps_list, yr_list, month_list = [], [], [], [], [], [], [], []
ctpv_sgg = grp1.drop_duplicates(['시도', '시군구_수정']).reset_index(drop=True)
for ctpv, sgg in ctpv_sgg[['시도', '시군구_수정']].values:
    for fuel in grp1['연료'].unique():
        for vhcty in grp1['차종'].unique():
            for ty in grp1['차종유형'].unique():
                for purps in grp1['용도'].unique():
                    for yr, month in zip(y_plist, mth_plist):
                        ctpv_list.append(ctpv)
                        sgg_list.append(sgg)
                        fuel_list.append(fuel)
                        vhcty_list.append(vhcty)
                        ty_list.append(ty)
                        purps_list.append(purps)
                        yr_list.append(str(yr))
                        month_list.append(f'{month:0>2}')
base = pd.DataFrame({'연도':yr_list, '월':month_list, '시도':ctpv_list, '시군구_수정':sgg_list, '연료':fuel_list, '차종':vhcty_list, '차종유형':ty_list, '용도':purps_list})

base1 = base.merge(grp1, on=['연도', '월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도'], how='left')
base2 = base1.merge(grp2, on=['연도', '월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도'], how='left')
base3 = base2.merge(grp3, on=['연도', '월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도'], how='left')
base4 = base3.merge(grp4, on=['연도', '월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도'], how='left')

base4[['차량대수', '조기폐차', '저감대수', '등록대수', '등록저감대수', '말소대수', '말소저감대수']] = base4[['차량대수', '조기폐차', '저감대수', '등록대수', '등록저감대수', '말소대수', '말소저감대수']].fillna(0)
base4[['차량대수_1', '등록대수_1', '말소대수_1', '저감대수_1', '등록저감대수_1', '말소저감대수_1']] = base4[['차량대수', '등록대수', '말소대수', '저감대수', '등록저감대수', '말소저감대수']].shift(-1)
base4.loc[[x for x in range(3, base4.shape[0], 4)], ['차량대수_1', '저감대수_1']]  = base4.loc[[x for x in range(3, base4.shape[0], 4)], ['차량대수', '저감대수']].values
base4.loc[[x for x in range(3, base4.shape[0], 4)], ['등록대수_1', '말소대수_1', '등록저감대수_1', '말소저감대수_1']] = 0
base4['차량대수'] = base4['차량대수_1'] - base4['등록대수_1'] + base4['말소대수_1']

base5 = base4[['연도', '월', '시도', '시군구_수정', '연료', '차종', '차종유형', '용도', '차량대수', '조기폐차', '저감대수']]
base5['감소대수'] = base5['차량대수'].shift() - base5['차량대수']
base5['감소율'] = base5['감소대수'] / base5['차량대수'].shift()
base5.loc[(base5['감소율'] == -np.inf) | (base5['감소율'] == np.inf), '감소율'] = 0
base5['감소율'] = base5['감소율'].fillna(0)
base5['저감장치미부착대수'] = base5['차량대수'] - base5['저감대수']

base5.loc[base5['차량대수'] < 0, '차량대수'] = 0
base5.loc[base5['저감장치미부착대수'] < 0, '저감장치미부착대수'] = 0
base5.loc[[x for x in range(0, base5.shape[0], 4)], '감소율'] = 0
base5 = base5.rename(columns={'조기폐차':'조기폐차대수', '저감대수':'저감장치부착대수'})

today_date = datetime.today().strftime("%Y%m%d")
base5['테이블생성일자'] = today_date

STD_BD_DAT_GRD4_CAR_CURSTT = base5[[
    '연도',
    '월',
    '시도',
    '시군구_수정',
    '연료',
    '차종',
    '차종유형', 
    '용도',
    '차량대수',
    '감소율',
    '저감장치부착대수',
    '저감장치미부착대수',
    '조기폐차대수',
    '테이블생성일자',
]]
chc_col = {
    '연도':'YR',
    '월':'MM',
    '시도':'CTPV',
    '시군구_수정':'SGG',
    '연료':'FUEL_CD',
    '차종':'VHCTY_CD',
    '차종유형':'VHCTY_TY', 
    '용도':'PURPS_CD2',
    '차량대수':'VHCL_MKCNT',
    '감소율':'DEC_RT',
    '저감장치부착대수':'RDCDVC_EXTRNS_MKCNT',
    '저감장치미부착대수':'RDCDVC_UNAT_MKCNT',
    '조기폐차대수':'ELPDSRC_MKCNT',
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_GRD4_CAR_CURSTT = STD_BD_DAT_GRD4_CAR_CURSTT.rename(columns=chc_col)

# STD_BD_DAT_GRD4_CAR_CURSTT.columns

### [출력] STD_BD_DAT_GRD4_CAR_CURSTT

# expdf = STD_BD_DAT_GRD4_CAR_CURSTT
# table_nm = 'STD_BD_DAT_GRD4_CAR_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_GRD4_CAR_CURSTT')

## 1-2 code end ##################################################################

## 1-3 start

## 지역정보 병합
df = csi.merge(coder, on='법정동코드', how='left')

# df['시도'].isnull().sum()

# df.loc[df['법정동코드'] == 4163055000, ['시도', '시군구']] = ['경기도', '양주시']

# ### rh 법정동코드 참고하여 법정동코드 수정
# rdf = df.copy()
# rdf['법정동코드'] = rdf['법정동코드'].astype('str')
# rdf['법정동코드_mod'] = rdf['법정동코드'].str[:5] + '00000'
# rdf['법정동코드_mod'] = pd.to_numeric(rdf['법정동코드_mod'])
# # 2817000000 인천광역시 남구 -> 인천광역시 미추홀구 2817700000
# # 4119500000 경기도 부천시 원미구 -> 경기도 부천시 4119000000
# # 4119700000 경기도 부천시 소사구 -> 경기도 부천시 4119000000
# # 4119900000 경기도 부천시 오정구 -> 경기도 부천시 4119000000
# # 4173000000 경기도 여주군 -> 경기도 여주시 4167000000
# # 4371000000 충청북도 청원군 -> 충청북도 충주시 4311000000
# rdf.loc[rdf['법정동코드_mod'] == 2817000000, '법정동코드_mod'] = 2817700000
# rdf.loc[rdf['법정동코드_mod'] == 4119500000, '법정동코드_mod'] = 4119000000
# rdf.loc[rdf['법정동코드_mod'] == 4119700000, '법정동코드_mod'] = 4119000000
# rdf.loc[rdf['법정동코드_mod'] == 4119900000, '법정동코드_mod'] = 4119000000
# rdf.loc[rdf['법정동코드_mod'] == 4173000000, '법정동코드_mod'] = 4167000000
# rdf.loc[rdf['법정동코드_mod'] == 4371000000, '법정동코드_mod'] = 4311000000
# df = rdf.copy()

df['법정동코드_mod'] = df['법정동코드'].copy()

# rdf = df.copy()
# rdf['법정동코드'] = rdf['법정동코드'].astype('str')
# rdf.loc[rdf['시도'].isnull() == True, '법정동코드'] = rdf.loc[rdf['시도'].isnull() == True, '법정동코드'].str[:5] + '00000'
# rdf['법정동코드'] = pd.to_numeric(rdf['법정동코드'])

# rdfy = rdf[rdf['시도'].isnull() == False]
# rdfn = rdf[rdf['시도'].isnull() == True]

# rdfn = rdfn.drop(['시도', '시군구'], axis=1)
# rdfnm = rdfn.merge(coder, on='법정동코드', how='left')

# df = pd.concat([rdfy, rdfnm], ignore_index=False)

## 일일평균주행거리 계산
df['검사일자'] = df['검사일자'].fillna(0)
df['검사일자'] = df['검사일자'].astype('str')
df['검사일자'] = df['검사일자'].str.split('.').str[0]
df['최초등록일자'] = pd.to_datetime(df['최초등록일자'], errors='coerce')
df['검사일자'] = pd.to_datetime(df['검사일자'], errors='coerce')
df['기간차이'] = df['검사일자'] - df['최초등록일자']

# 23.2s
df['기간차이'] = df['기간차이'].astype('str')
df['기간차이'] = df['기간차이'].str.split(' ').str[0]
df['기간차이'] = pd.to_numeric(df['기간차이'], errors='coerce')
df['일일평균주행거리'] = df['주행거리'] / (df['기간차이'])

### 빈 값 kosis로 대체
df1y = df[df['일일평균주행거리'].isnull() == False]
df1n = df[df['일일평균주행거리'].isnull() == True]

df1n = df1n.drop('일일평균주행거리', axis=1)
df1nm = df1n.merge(kosisr, on=['시도', '시군구', '차종'], how='left')

df2y = df1nm[df1nm['일일평균주행거리'].isnull() == False]
df2n = df1nm[df1nm['일일평균주행거리'].isnull() == True]
df2n = df2n.drop('일일평균주행거리', axis=1)
df2nm = df2n.merge(kosisr.drop_duplicates(['시도', '차종'])[['시도', '차종', '일일평균주행거리']], on=['시도', '차종'], how='left')

df3y = df2nm[df2nm['일일평균주행거리'].isnull() == False]
df3n = df2nm[df2nm['일일평균주행거리'].isnull() == True]

for ctpv, sgg, cd in df3n.loc[df3n['일일평균주행거리'].isnull() == True, ['시도', '시군구', '차종']].values:
    try:
        df3n.loc[(df3n['일일평균주행거리'].isnull() == True) & (df3n['시도'] == ctpv) & (df3n['시군구'] == sgg), '일일평균주행거리'] = kosisr.loc[(kosisr['시도'] == ctpv) & (kosisr['시군구'] == '소계') & (kosisr['차종'] == '합계'), '일일평균주행거리'].values[0]
    except:
        df3n.loc[(df3n['일일평균주행거리'].isnull() == True) & (df3n['시도'].isnull() == True) & (df3n['시군구'].isnull() == True), '일일평균주행거리'] = kosisr.loc[(kosisr['시도'] == '서울특별시') & (kosisr['시군구'] == '소계') & (kosisr['차종'] == '합계'), '일일평균주행거리'].values[0]

df2nm = pd.concat([df3y, df3n], ignore_index=True)
df1nm = pd.concat([df2y, df2nm], ignore_index=True)
df = pd.concat([df1y, df1nm], ignore_index=True)

### 저감장치 부착 유무 정보 병합
df1 = df.merge(attr[['차대번호', 'DPF_YN']], on='차대번호', how='left')

## 4등급 result 파일 참고하여 DPF유무 수정
rdf = df1.copy()
rs = rs.drop_duplicates('차대번호').reset_index(drop=True)
rdf1 = rdf.merge(rs, on='차대번호', how='left')
rdf1.loc[(rdf1['DPF_YN'] == '유') | (rdf1['DPF유무_수정'] == '유'), 'DPF_YN'] = '유'
rdf1.loc[(rdf1['DPF유무_수정'] == '무'), 'DPF_YN'] = '무'
rdf1.loc[(rdf1['DPF유무_수정'] == '확인불가'), 'DPF_YN'] = '확인불가'
df1 = rdf1.drop('DPF유무_수정', axis=1)

## 4등급 차량(휘발유, 경유, LPG) 추출
etc = df1[(df1['연료'] == '알코올') | (df1['연료'] == 'CNG(압축천연가스)') | (df1['연료'] == '기타연료')].reset_index(drop=True)
dgl = df1[(df1['연료'] == '휘발유') | (df1['연료'] == '경유') | (df1['연료'] == 'LPG(액화석유가스)')].reset_index(drop=True)

### 차령 정보 계산 및 데이터 추가
current_yr = int(datetime.today().strftime("%Y"))
dgl['차령'] = current_yr - dgl['차량연식']

## 시군구명 앞쪽만 사용
# - 시군구명 앞지역명만 추출
dgl['시군구_수정'] = dgl['시군구'].str.split(' ').str[0]

# 자동차-엔진 가열(Hot-start) 배출
## 배출량 식
# $$E_{i, j} = VKT \times {EF_i \over {1000}} \times DF \times (1 - {R \over {100}})$$
# $E_{i, j}$ : 자동차 $j$의 도로주행시 발생한 오염물질 $i$의 배출량($kg/yr$)
# $VKT$ : 주행거리($km/yr$)
# $EF_i$ : 차종별, 연료별, 연식별, 차속별 배출계수($g/km$)
# $DF$ : 열화계수
# $R$ : 저감장치 부착 효율(%)
df2 = dgl.copy()

## 주행거리(VKT)
# - [현재 설정] "용도별_차종별_시군구별_자동차주행거리" 자료(KOSIS)(2021)의 1일 평균주행거리 * 365 = 주행거리(km/yr)
df2['VKT'] = df2['일일평균주행거리'] * 365

## DF(열화계수)
# - 필요한 정보 : 연료, 차종, 차종유형, 연식, 차령
# - 조건 수정(2023.04.20, 최)
#     - 특수 소형 = 승용 소형
#     - 승합 대형 = 화물 대형

DF_col = ['연료', '차종', '차종유형', '차량연식', '차령']

### ❗ DF(열화계수) 코드
# - 연료, 차종, 차종유형 설정에 해당되지 않는 차량의 열화계수 값 설정
#     - 현재 설정값 : np.nan
#     - 고려 설정값 : 1

# about 5.0s
DF_CO_list = []
DF_HC_list = []
DF_PM_list = []
DF_NOx_list = []
# '연료', '차종', '차종유형', '차량연식', '차령'
for fuel, car_type, car_size, car_birth, car_age in df2[DF_col].values:    
    if fuel == '경유':
        if (car_type == '승용') or ( (car_type == '특수') and (car_size == '소형') ): # 최 확인(2023.04.20) : "특수 소형" -> "승용 소형" 조건으로 계산
            warranty = 5
            if car_age <= warranty:
                DF_CO = 1
                DF_HC = 1
                DF_PM = 1
                DF_NOx = 1
            else:
                DF_CO = 1 + (car_age - warranty)*0.05
                DF_HC = 1 + (car_age - warranty)*0.05
                DF_PM = 1 + (car_age - warranty)*0.05
                DF_NOx = 1 + (car_age - warranty)*0.02
                if DF_CO > 1.5:
                    DF_CO = 1.5
                if DF_HC > 1.5:
                    DF_HC = 1.5
                if DF_PM > 1.5:
                    DF_PM = 1.5
                if DF_NOx > 1.2:
                    DF_NOx = 1.2
        elif car_type == '승합':
            if car_size == '소형' or car_size == '중형':
                warranty = 5
                if car_age <= warranty:
                    DF_CO = 1
                    DF_HC = 1
                    DF_PM = 1
                    DF_NOx = 1
                else:
                    DF_CO = 1 + (car_age - warranty)*0.05
                    DF_HC = 1 + (car_age - warranty)*0.05
                    DF_PM = 1 + (car_age - warranty)*0.05
                    DF_NOx = 1 + (car_age - warranty)*0.02
                    if DF_CO > 1.5:
                        DF_CO = 1.5
                    if DF_HC > 1.5:
                        DF_HC = 1.5
                    if DF_PM > 1.5:
                        DF_PM = 1.5
                    if DF_NOx > 1.2:
                        DF_NOx = 1.2
            elif car_size == '대형':
                warranty = 3
                if car_age <= warranty:
                    DF_CO = 1
                    DF_HC = 1
                    DF_PM = 1
                    DF_NOx = 1
                else:
                    DF_CO = 1 + (car_age - warranty)*0.05
                    DF_HC = 1 + (car_age - warranty)*0.05
                    DF_PM = 1 + (car_age - warranty)*0.05
                    DF_NOx = 1 + (car_age - warranty)*0.02
                    if DF_CO > 1.5:
                        DF_CO = 1.5
                    if DF_HC > 1.5:
                        DF_HC = 1.5
                    if DF_PM > 1.5:
                        DF_PM = 1.5
                    if DF_NOx > 1.2:
                        DF_NOx = 1.2
        elif car_type == '화물':
            # car_size : 경, 소, 중, 대
            warranty = 5
            if car_age <= warranty:
                DF_CO = 1
                DF_HC = 1
                DF_PM = 1
                DF_NOx = 1
            else:
                DF_CO = 1 + (car_age - warranty)*0.05
                DF_HC = 1 + (car_age - warranty)*0.05
                DF_PM = 1 + (car_age - warranty)*0.05
                DF_NOx = 1 + (car_age - warranty)*0.02
                if DF_CO > 1.5:
                    DF_CO = 1.5
                if DF_HC > 1.5:
                    DF_HC = 1.5
                if DF_PM > 1.5:
                    DF_PM = 1.5
                if DF_NOx > 1.2:
                    DF_NOx = 1.2
        elif car_type == '특수':
            # car_size : 중, 대
            warranty = 5
            if car_age <= warranty:
                DF_CO = 1
                DF_HC = 1
                DF_PM = 1
                DF_NOx = 1
            else:
                DF_CO = 1 + (car_age - warranty)*0.05
                DF_HC = 1 + (car_age - warranty)*0.05
                DF_PM = 1 + (car_age - warranty)*0.05
                DF_NOx = 1 + (car_age - warranty)*0.02
                if DF_CO > 1.5:
                    DF_CO = 1.5
                if DF_HC > 1.5:
                    DF_HC = 1.5
                if DF_PM > 1.5:
                    DF_PM = 1.5
                if DF_NOx > 1.2:
                    DF_NOx = 1.2
    elif (fuel == '휘발유') or (fuel == 'LPG(액화석유가스)'):
        if car_type == '승용':
            # 경, 소, 중, 대
            if car_birth <= 2000:
                warranty = 5
                if car_age <= warranty:
                    DF_CO = 1
                    DF_HC = 1
                    DF_PM = 1
                    DF_NOx = 1
                else:
                    DF_CO = 1 + (car_age - warranty)*0.1
                    DF_HC = 1 + (car_age - warranty)*0.1
                    DF_PM = 1 + (car_age - warranty)*0.1
                    DF_NOx = 1 + (car_age - warranty)*0.1
                    if DF_CO > 2.0:
                        DF_CO = 2.0
                    if DF_HC > 2.0:
                        DF_HC = 2.0
                    if DF_PM > 2.0:
                        DF_PM = 2.0
                    if DF_NOx > 2.0:
                        DF_NOx = 2.0
            elif car_birth >= 2001:
                warranty = 10
                if car_age <= warranty:
                    DF_CO = 1
                    DF_HC = 1
                    DF_PM = 1
                    DF_NOx = 1
                else:
                    DF_CO = 1 + (car_age - warranty)*0.1
                    DF_HC = 1 + (car_age - warranty)*0.1
                    DF_PM = 1 + (car_age - warranty)*0.1
                    DF_NOx = 1 + (car_age - warranty)*0.1
                    if DF_CO > 2.0:
                        DF_CO = 2.0
                    if DF_HC > 2.0:
                        DF_HC = 2.0
                    if DF_PM > 2.0:
                        DF_PM = 2.0
                    if DF_NOx > 2.0:
                        DF_NOx = 2.0
        elif car_type == '승합':
            if car_size == '경형':
                warranty = 5
                if car_age <= warranty:
                    DF_CO = 1
                    DF_HC = 1
                    DF_PM = 1
                    DF_NOx = 1
                else:
                    DF_CO = 1 + (car_age - warranty)*0.1
                    DF_HC = 1 + (car_age - warranty)*0.1
                    DF_PM = 1 + (car_age - warranty)*0.1
                    DF_NOx = 1 + (car_age - warranty)*0.1
                    if DF_CO > 2.0:
                        DF_CO = 2.0
                    if DF_HC > 2.0:
                        DF_HC = 2.0
                    if DF_PM > 2.0:
                        DF_PM = 2.0
                    if DF_NOx > 2.0:
                        DF_NOx = 2.0
            elif (car_size == '소형') or (car_size == '중형'):
                if car_birth <= 2005:
                    warranty = 5
                    if car_age <= warranty:
                        DF_CO = 1
                        DF_HC = 1
                        DF_PM = 1
                        DF_NOx = 1
                    else:
                        DF_CO = 1 + (car_age - warranty)*0.1
                        DF_HC = 1 + (car_age - warranty)*0.1
                        DF_PM = 1 + (car_age - warranty)*0.1
                        DF_NOx = 1 + (car_age - warranty)*0.1
                        if DF_CO > 2.0:
                            DF_CO = 2.0
                        if DF_HC > 2.0:
                            DF_HC = 2.0
                        if DF_PM > 2.0:
                            DF_PM = 2.0
                        if DF_NOx > 2.0:
                            DF_NOx = 2.0
                elif car_birth >= 2006:
                    warranty = 10
                    if car_age <= warranty:
                        DF_CO = 1
                        DF_HC = 1
                        DF_PM = 1
                        DF_NOx = 1
                    else:
                        DF_CO = 1 + (car_age - warranty)*0.1
                        DF_HC = 1 + (car_age - warranty)*0.1
                        DF_PM = 1 + (car_age - warranty)*0.1
                        DF_NOx = 1 + (car_age - warranty)*0.1
                        if DF_CO > 2.0:
                            DF_CO = 2.0
                        if DF_HC > 2.0:
                            DF_HC = 2.0
                        if DF_PM > 2.0:
                            DF_PM = 2.0
                        if DF_NOx > 2.0:
                            DF_NOx = 2.0
        elif (car_type == '화물') or ( (car_type == '승합') and (car_size == '대형')): # 최 확인(2023.04.20) : 습합 대형 -> 화물 대형 조건으로 계산
            # 경, 소, 중, 대
            warranty = 5
            if car_age <= warranty:
                DF_CO = 1
                DF_HC = 1
                DF_PM = 1
                DF_NOx = 1
            else:
                DF_CO = 1 + (car_age - warranty)*0.1
                DF_HC = 1 + (car_age - warranty)*0.1
                DF_PM = 1 + (car_age - warranty)*0.1
                DF_NOx = 1 + (car_age - warranty)*0.1
                if DF_CO > 2.0:
                    DF_CO = 2.0
                if DF_HC > 2.0:
                    DF_HC = 2.0
                if DF_PM > 2.0:
                    DF_PM = 2.0
                if DF_NOx > 2.0:
                    DF_NOx = 2.0
        elif car_type == '특수':
            # 중, 대
            warranty = 5
            if car_age <= warranty:
                DF_CO = 1
                DF_HC = 1
                DF_PM = 1
                DF_NOx = 1
            else:
                DF_CO = 1 + (car_age - warranty)*0.1
                DF_HC = 1 + (car_age - warranty)*0.1
                DF_PM = 1 + (car_age - warranty)*0.1
                DF_NOx = 1 + (car_age - warranty)*0.1
                if DF_CO > 2.0:
                    DF_CO = 2.0
                if DF_HC > 2.0:
                    DF_HC = 2.0
                if DF_PM > 2.0:
                    DF_PM = 2.0
                if DF_NOx > 2.0:
                    DF_NOx = 2.0
    else:
        # 설정 고민
            # 고려 : 1
        # DF_CO, DF_HC, DF_PM, DF_NOx = np.nan, np.nan, np.nan, np.nan
        DF_CO, DF_HC, DF_PM, DF_NOx = 1, 1, 1, 1

    # 열화계수(DF) 리스트에 저장
    DF_CO_list.append(DF_CO)
    DF_HC_list.append(DF_HC)
    DF_PM_list.append(DF_PM)
    DF_NOx_list.append(DF_NOx)

df2['DF_CO'] = DF_CO_list
df2['DF_HC'] = DF_HC_list
df2['DF_PM'] = DF_PM_list
df2['DF_NOx'] = DF_NOx_list
check_DF_col = ['DF_CO', 'DF_HC', 'DF_PM', 'DF_NOx']

## 저감장치 부착 효율(R) 계산
# - DPF유무_수정 : 유 -> DPF로 가정하고 진행
# - 휘발성 유기 화합물(Volatile Organic Compounds:VOC) : 생활주변에서 흔히 사용하는 탄화수소류가 거의 해당됨.
# - 저감장치별 물질 제거 효율(%)

#     장치종류|CO|VOC|PM
#     :-:|:-:|:-:|:-:
#     DPF|99.5|90|83.6
#     pDPF|94.6|89.3|56
#     DOC|85.4|72|35

df2.loc[df2['DPF_YN'] == '유', ['R_CO', 'R_HC', 'R_PM']] = 99.5, 90, 83.6
check_R_col = ['R_CO', 'R_HC', 'R_PM']
df2[check_R_col] = df2[check_R_col].fillna(0)

## ❗ 배출계수(EFi)
# - 연료, 차종, 차종유형 설정에 해당되지 않는 차량의 배출계수 값 설정
#     - 현재 설정값 : np.nan
#     - 고려 설정값 : 1
# - 조건 수정(2023.04.20, 최)
#     - 특수 중형 = 승합 중형
#     - 화물 중형 = 승합 중형
#     - 화물 대형 = 승합 대형
#     - 특수 중형 = 승용 중형

EFi_col = ['차종', '차종유형', '연료', '차량연식']

# 참고 : KOSIS 차량속도(2017) 일반국도 평균 (https://kosis.kr/statHtml/statHtml.do?orgId=210&tblId=DT_21002_J008)
V = 54.1
# 국가 대기오염물질 배출량 산정방법 편람(V)(2022) 부록 참고(for PM-2.5)
k = 0.92

# about 20.1s
EFi_CO_list = []
EFi_HC_list = []
EFi_NOx_list = []
EFi_PM10_list = []
EFi_PM2_5_list = []
EFi_NH3_list = []

# 차종, 차종유형, 연료, 연식
for car_type, car_size, fuel, car_birth in df2[EFi_col].values:
    EFi_COm = 0
    EFi_HCm = 0
    EFi_NOxm = 0
    EFi_PM10m = 0
    EFi_PM2_5m = 0
    EFi_NH3m = 0
    for V in [35, 70, 100]:
        if fuel == '휘발유':
            if (car_type == '승용') and (car_size == '경형'):
                if car_birth <= 1996:
                    if V <= 65:
                        EFi_CO = 59.783 * (V**-1.0007)
                    else:
                        EFi_CO = 0.0874 * V - 3.5618
                    EFi_HC = 7.6244 * (V**-0.8364)
                    EFi_NOx = 2.6754 * (V**-0.3236)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 1997 <= car_birth <= 1999:
                    if V <= 65:
                        EFi_CO = 59.783 * (V**-1.0007)
                    else:
                        EFi_CO = 0.0874 * V - 3.5618
                    EFi_HC = 8.6275 * (V**-1.0722)
                    EFi_NOx = 3.2294 * (V**-0.5763)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 2000 <= car_birth <= 2002:
                    if V <= 65:
                        EFi_CO = 60.556 * (V**-1.2501)
                    else:
                        EFi_CO = -0.0006 * V + 0.5753
                    EFi_HC = 5.1835 * (V**-1.1889)
                    EFi_NOx = 1.7525 * (V**-0.6481)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V > 90:
                        EFi_NH3 = 0.084
                elif 2003 <= car_birth <= 2005:
                    if V <= 65:
                        EFi_CO = 60.556 * (V**-1.2501)
                    else:
                        EFi_CO = -0.0006 * V + 0.5753
                    EFi_HC = 0.7446 * (V**-0.9392)
                    EFi_NOx = 0.3403 * (V**-0.5455)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2006 <= car_birth <= 2008:
                    if V <= 45:
                        EFi_CO = 4.9952 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0001 * V**2 + 0.0229 * V - 0.5701
                    EFi_HC = 0.2958 * (V**-0.7830)
                    EFi_NOx = 0.4819 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2009 <= car_birth <= 2011:
                    if V <= 45:
                        EFi_CO = 4.5956 * (V**-0.8461)
                    else:
                        EFi_CO = -9.2000*(10**-5) * (V**2) + 2.1068*(10**-2) * V - 5.2449*(10**-1)
                    EFi_HC = 0.2662 * (V**-0.7830)
                    EFi_NOx = 0.4476 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif 2012 <= car_birth <= 2013:
                    if V <= 45:
                        EFi_CO = 4.4517 * (V**-0.8461)
                    else:
                        EFi_CO = -8.9120*(10**-5) * (V**2) + 2.0408*(10**-2)*V - 5.0807*(10**-1)
                    EFi_HC = 0.2556 * (V**-0.7830)
                    EFi_NOx = 0.4353 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif car_birth == 2014:
                    if V <= 45:
                        EFi_CO = 4.3079 * (V**-0.8461)
                    else:
                        EFi_CO = -8.6240*(10**-5) * (V**2) + 1.9749*(10**-2)*V - 4.9165*(10**-1)
                    EFi_HC = 0.2449 * (V**-0.7830)
                    EFi_NOx = 0.4230 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif 2015 <= car_birth:
                    if V <= 45:
                        EFi_CO = 4.164 * (V**-0.8461)
                    else:
                        EFi_CO = -8.3360*(10**-5) * (V**2) + 1.9089*(10**-2)*V - 4.7524*(10**-1)
                    EFi_HC = 0.2343 * (V**-0.7830)
                    EFi_NOx = 0.4106 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
            elif (car_type == '승용') and (car_size == '소형'):
                if car_birth <= 1986:
                    EFi_CO = 247.00 * (V**-0.6651)
                    EFi_HC = 15.953 * (V**-0.5059)
                    EFi_NOx = 3.1140 * (V**-0.2278)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 1987 <= car_birth <= 1990:
                    EFi_CO = 36.169 * (V**-0.7587)
                    EFi_HC = 15.607 * (V**-1.0423)
                    EFi_NOx = 6.2007 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 1991 <= car_birth <= 1999:
                    EFi_CO = 111.67 * (V**-1.1566)
                    EFi_HC = 32.017 * (V**-1.4171)
                    EFi_NOx = 7.5244 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 2000 <= car_birth <= 2002:
                    EFi_CO = 22.356 * (V**-0.9068)
                    EFi_HC = 0.8428 * (V**-0.8829)
                    EFi_NOx = 1.2613 * (V**-0.3873)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V > 90:
                        EFi_NH3 = 0.084
                elif 2003 <= car_birth <= 2005:
                    EFi_CO = 1.4898 * (V**-0.3837)
                    EFi_HC = 0.1738 * (V**-0.7268)
                    EFi_NOx = 0.1563 * (V**-0.2671)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2006 <= car_birth <= 2008:
                    EFi_CO = 1.0000*(10**-4)*(V**2) - 7.1000*(10**-3)*V + 2.2450*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0633 * (V**-1.0484)
                    else:
                        EFi_HC = 1.3200*(10**-6)*(V**2) - 1.8800*(10**-4)*V + 7.7000*(10**-3)
                    EFi_NOx = -3.5000*(10**-6)*(V**2) + 3.3000*(10**-4)*V + 1.1200*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2009 <= car_birth <= 2011:
                    EFi_CO = 9.2000*(10**-5)*(V**2) - 6.5320*(10**-3)*V + 2.0654*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0570 * (V**-1.0484)
                    else:
                        EFi_HC = 1.1880*(10**-6)*(V**2) - 1.6920*(10**-4)*V + 6.9300*(10**-3)
                    EFi_NOx = -3.2511*(10**-6)*(V**2) + 3.0653*(10**-4)*V + 1.0404*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif 2012 <= car_birth <= 2013:
                    EFi_CO = 8.9120*(10**-5)*(V**2) - 6.3275*(10**-3)*V + 2.0007*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0547 * (V**-1.0484)
                    else:
                        EFi_HC = 1.1405*(10**-6)*(V**2) - 1.6243*(10**-4)*V + 6.6528*(10**-3)
                    EFi_NOx = -3.1615*(10**-6)*(V**2) + 2.9809*(10**-4)*V + 1.0117*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif car_birth == 2014:
                    EFi_CO = 8.6240*(10**-5)*(V**2) - 6.1230*(10**-3)*V + 1.9361*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0524 * (V**-1.0484)
                    else:
                        EFi_HC = 1.0930*(10**-6)*(V**2) - 1.5566*(10**-4)*V + 6.3756*(10**-3)
                    EFi_NOx = -3.0719*(10**-6)*(V**2) + 2.8964*(10**-4)*V + 9.8301*(10**-3)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif 2015 <= car_birth:
                    EFi_CO = 8.3360*(10**-5)*(V**2) - 5.9186*(10**-3)*V + 1.8714*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0501 * (V**-1.0484)
                    else:
                        EFi_HC = 1.0500*(10**-6)*(V**2) - 1.4890*(10**-4)*V + 6.09840*(10**-3)
                    EFi_NOx = -2.9823*(10**-6)*(V**2) + 2.8119*(10**-4)*V + 9.5434*(10**-3)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
            elif ( (car_type == '승용') and (car_size == '중형') ) or ( (car_type == '특수') and (car_size == '중형') ): # 최이사님 확인(2023.04.20) : 특수 중형 -> 승용 중형 조건으로 계산
                if car_birth <= 1986:
                    EFi_CO = 247.00 * (V**-0.6651)
                    EFi_HC = 15.953 * (V**-0.5059)
                    EFi_NOx = 3.1140 * (V**-0.2278)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 1987 <= car_birth <= 1990:
                    EFi_CO = 36.169 * (V**-0.7587)
                    EFi_HC = 15.607 * (V**-1.0423)
                    EFi_NOx = 6.2007 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 1991 <= car_birth <= 1999:
                    EFi_CO = 51.555 * (V**-0.9531)
                    EFi_HC = 31.816 * (V**-1.4804)
                    EFi_NOx = 7.5244 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 2000 <= car_birth <= 2002:
                    EFi_CO = 29.921 * (V**-0.8868)
                    EFi_HC = 7.9374 * (V**-1.3041)
                    EFi_NOx = 1.8525 * (V**-0.4192)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V > 90:
                        EFi_NH3 = 0.084
                elif 2003 <= car_birth <= 2005:
                    EFi_CO = 2.4938 * (V**-0.6106)
                    EFi_HC = 0.4262 * (V**-1.0122)
                    EFi_NOx = 0.1818 * (V**-0.4316)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2006 <= car_birth <= 2008:
                    EFi_CO = 2.2900*(10**-5)*(V**2) - 1.6300*(10**-3)*V + 5.8300*(10**-2)
                    if V <= 65.4:
                        EFi_HC = 0.0633 * (V**-1.0484)
                    else:
                        EFi_HC = 1.3200*(10**-6)*(V**2) - 1.8800*(10**-4)*V + 7.7000*(10**-3)
                    EFi_NOx = -3.5000*(10**-6)*(V**2) + 3.3000*(10**-4)*V + 1.1200*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2009 <= car_birth <= 2011:
                    EFi_CO = 2.1068*(10**-5)*(V**2) - 1.4996*(10**-3)*V + 5.3636*(10**-2)
                    if V <= 65.4:
                        EFi_HC = 0.0570 * (V**-1.0484)
                    else:
                        EFi_HC = 1.1880*(10**-6)*(V**2) - 1.6920*(10**-4)*V + 6.9300*(10**-3)
                    EFi_NOx = -3.2511*(10**-6)*(V**2) + 3.0653*(10**-4)*V + 1.0404*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif 2012 <= car_birth <= 2013:
                    EFi_CO = 2.0408*(10**-5)*(V**2) - 1.4527*(10**-3)*V + 5.1957*(10**-2)
                    if V <= 65.4:
                        EFi_HC = 0.0547 * (V**-1.0484)
                    else:
                        EFi_HC = 1.1405*(10**-6)*(V**2) - 1.6243*(10**-4)*V + 6.6528*(10**-3)
                    EFi_NOx = -3.1615*(10**-6)*(V**2) + 2.9809*(10**-4)*V + 1.0117*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif car_birth == 2014:
                    EFi_CO = 1.9749*(10**-5)*(V**2) - 1.4057*(10**-3)*V + 5.0278*(10**-2)
                    if V <= 65.4:
                        EFi_HC = 0.0524 * (V**-1.0484)
                    else:
                        EFi_HC = 1.0930*(10**-6)*(V**2) - 1.5566*(10**-4)*V + 6.3756*(10**-3)
                    EFi_NOx = -3.0719*(10**-6)*(V**2) + 2.8964*(10**-4)*V + 9.8301*(10**-3)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif 2015 <= car_birth:
                    EFi_CO = 1.9089*(10**-5)*(V**2) - 1.3588*(10**-3)*V + 4.8599*(10**-2)
                    if V <= 65.4:
                        EFi_HC = 0.0501 * (V**-1.0484)
                    else:
                        EFi_HC = 1.0500*(10**-6)*(V**2) - 1.4890*(10**-4)*V + 6.0984*(10**-3)
                    EFi_NOx = -2.9823*(10**-6)*(V**2) + 2.8119*(10**-4)*V + 9.5434*(10**-3)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
            elif (car_type == '승용') and (car_size == '대형'):
                if car_birth <= 1986:
                    EFi_CO = 247.00 * (V**-0.6651)
                    EFi_HC = 15.953 * (V**-0.5059)
                    EFi_NOx = 3.1140 * (V**-0.2278)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 1987 <= car_birth <= 1990:
                    EFi_CO = 36.169 * (V**-0.7587)
                    EFi_HC = 15.607 * (V**-1.0423)
                    EFi_NOx = 6.2007 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 1991 <= car_birth <= 1999:
                    EFi_CO = 51.555 * (V**-0.9531)
                    EFi_HC = 31.816 * (V**-1.4804)
                    EFi_NOx = 7.5244 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 2000 <= car_birth <= 2002:
                    EFi_CO = 29.921 * (V**-0.8868)
                    EFi_HC = 7.9374 * (V**-1.3041)
                    EFi_NOx = 1.8525 * (V**-0.4192)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V > 90:
                        EFi_NH3 = 0.084
                elif 2003 <= car_birth <= 2005:
                    EFi_CO = 2.4938 * (V**-0.6106)
                    EFi_HC = 0.4262 * (V**-1.0122)
                    EFi_NOx = 0.1818 * (V**-0.4316)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2006 <= car_birth <= 2008:
                    if V <= 65.4:
                        EFi_CO = 1.4082 * (V**-0.7728)
                    else:
                        EFi_CO = 8.0000*(10**-5)*(V**2) - 1.2700*(10**-2)*V + 5.7510*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0633 * (V**-1.0484)
                    else:
                        EFi_HC = 1.3200*(10**-6)*(V**2) - 1.8800*(10**-4)*V + 7.7000*(10**-3)
                    EFi_NOx = -3.5000*(10**-6)*(V**2) + 3.3000*(10**-4)*V + 1.1200*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2009 <= car_birth <= 2011:
                    if V <= 65.4:
                        EFi_CO = 1.2955 * (V**-0.7728)
                    else:
                        EFi_CO = 7.3600*(10**-5)*(V**2) - 1.1684*(10**-2)*V + 5.2909*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0570 * (V**-1.0484)
                    else:
                        EFi_HC = 1.1880*(10**-6)*(V**2) - 1.6920*(10**-4)*V + 6.9300*(10**-3)
                    EFi_NOx = -3.2511*(10**-6)*(V**2) + 3.0653*(10**-4)*V + 1.0404*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif 2012 <= car_birth <= 2013:
                    if V <= 65.4:
                        EFi_CO = 1.2550 * (V**-0.7728)
                    else:
                        EFi_CO = 7.1296*(10**-5)*(V**2) - 1.1318*(10**-2)*V + 5.1253*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0547 * (V**-1.0484)
                    else:
                        EFi_HC = 1.1405*(10**-6)*(V**2) - 1.6243*(10**-4)*V + 6.6528*(10**-3)
                    EFi_NOx = -3.1615*(10**-6)*(V**2) + 2.9809*(10**-4)*V + 1.0117*(10**-2)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif car_birth == 2014:
                    if V <= 65.4:
                        EFi_CO = 1.2144 * (V**-0.7728)
                    else:
                        EFi_CO = 6.8992*(10**-5)*(V**2) - 1.0952*(10**-2)*V + 4.9597*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0524 * (V**-1.0484)
                    else:
                        EFi_HC = 1.0930*(10**-6)*(V**2) - 1.5566*(10**-4)*V + 6.3756*(10**-3)
                    EFi_NOx = -3.0719*(10**-6)*(V**2) + 2.8964*(10**-4)*V + 9.8301*(10**-3)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
                elif 2015 <= car_birth:
                    if V <= 65.4:
                        EFi_CO = 1.1739 * (V**-0.7728)
                    else:
                        EFi_CO = 6.6688*(10**-5)*(V**2) - 1.0587*(10**-2)*V + 4.7940*(10**-1)
                    if V <= 65.4:
                        EFi_HC = 0.0501 * (V**-1.0484)
                    else:
                        EFi_HC = 1.0500*(10**-6)*(V**2) - 1.4890*(10**-4)*V + 6.0984*(10**-3)
                    EFi_NOx = -2.9823*(10**-6)*(V**2) + 2.8119*(10**-4)*V + 9.5434*(10**-3)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
            elif ( (car_type == '승합') and (car_size == '경형') ) or ( (car_type == '화물') and (car_size == '경형') ):
                if car_birth <= 1996:
                    if V <= 45:
                        EFi_CO = 11.249 * (V**-0.6579)
                    else:
                        EFi_CO = 0.0003 * (V**2) + 0.0002 * V + 0.4136
                    EFi_HC = 7.6244 * (V**-0.8364)
                    EFi_NOx = 2.6754 * (V**-0.3236)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 1997 <= car_birth <= 1999:
                    if V <= 45:
                        EFi_CO = 16.965 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0003 * (V**2) + 0.0777 * V - 1.9363
                    EFi_HC = 3.0285 * (V**-0.7830)
                    EFi_NOx = 1.9923 * (V**-0.3889)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 65:
                        EFi_NH3 = 0.1
                    else:
                        EFi_NH3 = 0.07
                elif 2000 <= car_birth <= 2002:
                    if V <= 45:
                        EFi_CO = 9.9433 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0002 * (V**2) + 0.0455 * V - 1.1349
                    EFi_HC = 1.8928 * (V**-0.7830)
                    EFi_NOx = 1.2352 * (V**-0.3889)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V >= 90:
                        EFi_NH3 = 0.084
                elif 2003 <= car_birth <= 2005:
                    if V <= 45:
                        EFi_CO = 9.9433 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0002 * (V**2) + 0.0455 * V - 1.1349
                    EFi_HC = 0.9227 * (V**-0.7830)
                    EFi_NOx = 3.8859 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V >= 90:
                        EFi_NH3 = 0.065
                elif 2006 <= car_birth <= 2008:
                    if V <= 45:
                        EFi_CO = 4.4952 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0001 * (V**2) + 0.0229 * V - 0.5701
                    EFi_HC = 0.2958 * (V**-0.7830)
                    EFi_NOx = 0.4819 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V >= 90:
                        EFi_NH3 = 0.065
                elif 2009 <= car_birth:
                    if V <= 45:
                        EFi_CO = 4.4952 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0001 * (V**2) + 0.0229 * V - 0.5701
                    EFi_HC = 0.2958 * (V**-0.7830)
                    EFi_NOx = 0.4819 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V >= 90:
                        EFi_NH3 = 0.022
            elif (car_type == '승합') and (car_size == '소형'):
                if car_birth <= 1990:
                    EFi_CO = 36.169 * (V**-0.7587)
                    EFi_HC = 15.607 * (V**-1.0423)
                    EFi_NOx = 6.2007 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    EFi_NH3 = 0.002
                elif 1991 <= car_birth <= 1996:
                    EFi_CO = 39.402 * (V**-0.8879)
                    EFi_HC = 23.400 * (V**-1.4041)
                    EFi_NOx = 7.5244 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    EFi_NH3 = 0.002
                elif 1997 <= car_birth <= 1999:
                    EFi_CO = 77.088 * (V**-1.2078)
                    EFi_HC = 18.731 * (V**-1.5356)
                    EFi_NOx = 4.4260 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    EFi_NH3 = 0.002
                elif car_birth == 2000:
                    EFi_CO = 41.669 * (V**-1.2078)
                    EFi_HC = 14.190 * (V**-1.5356)
                    EFi_NOx = 3.4578 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V >= 90:
                        EFi_NH3 = 0.084
                elif 2001 <= car_birth <= 2002:
                    EFi_CO = 41.669 * (V**-1.2078)
                    EFi_HC = 11.920 * (V**-1.5356)
                    EFi_NOx = 3.0649 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V >= 90:
                        EFi_NH3 = 0.084
                elif 2003 <= car_birth <= 2005:
                    EFi_CO = 37.573 * (V**-1.2078)
                    EFi_HC = 3.1786 * (V**-1.5356)
                    EFi_NOx = 1.4931 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V >= 90:
                        EFi_NH3 = 0.065
                elif 2006 <= car_birth <= 2008:
                    EFi_CO = 32.899 * (V**-1.2078)
                    EFi_HC = 2.7387 * (V**-1.5356)
                    EFi_NOx = 1.1808 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V >= 90:
                        EFi_NH3 = 0.065
                elif 2009 <= car_birth:
                    EFi_CO = 32.899 * (V**-1.2078)
                    EFi_HC = 2.7387 * (V**-1.5356)
                    EFi_NOx = 1.1808 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.08
                    elif V >= 90:
                        EFi_NH3 = 0.022
            elif ( (car_type == '승합') and (car_size == '중형') or (car_size == '대형') ) or ( (car_type == '화물') and (car_size == '소형') ):
                if car_birth <= 1990:
                    EFi_CO = 36.169 * (V**-0.7587)
                    EFi_HC = 15.607 * (V**-1.0423)
                    EFi_NOx = 6.2007 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    EFi_NH3 = 0.002
                elif 1991 <= car_birth <= 1996:
                    EFi_CO = 39.402 * (V**-0.8879)
                    EFi_HC = 23.400 * (V**-1.4041)
                    EFi_NOx = 7.5244 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    EFi_NH3 = 0.002
                elif 1997 <= car_birth <= 1999:
                    EFi_CO = 77.088 * (V**-1.2078)
                    EFi_HC = 18.731 * (V**-1.5356)
                    EFi_NOx = 4.4260 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    EFi_NH3 = 0.002
                elif 2000 <= car_birth <= 2002:
                    EFi_CO = 41.669 * (V**-1.2078)
                    EFi_HC = 14.190 * (V**-1.5356)
                    EFi_NOx = 3.4578 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V >= 90:
                        EFi_NH3 = 0.084
                elif 2003 <= car_birth <= 2005:
                    EFi_CO = 36.578 * (V**-1.2078)
                    EFi_HC = 3.0337 * (V**-1.5356)
                    EFi_NOx = 2.0104 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V >= 90:
                        EFi_NH3 = 0.065
                elif 2006 <= car_birth <= 2008:
                    EFi_CO = 14.202 * (V**-1.2078)
                    EFi_HC = 1.2233 * (V**-1.5356)
                    EFi_NOx = 0.2493 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V >= 90:
                        EFi_NH3 = 0.065
                elif 2009 <= car_birth:
                    EFi_CO = 14.202 * (V**-1.2078)
                    EFi_HC = 1.2233 * (V**-1.5356)
                    EFi_NOx = 0.2493 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V >= 90:
                        EFi_NH3 = 0.022
            elif (car_type == '화물') and (car_size == '중형'):
                if 1993 <= car_birth <= 1998:
                    if V <= 65:
                        EFi_CO = 70
                        EFi_HC = 7
                        EFi_NOx = 4.5
                    else:
                        EFi_CO = 55
                        EFi_HC = 3.5
                        EFi_NOx = 7.5
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    EFi_NH3 = 0.002
                elif car_birth == 1999:
                    if V <= 65:
                        EFi_CO = 70
                        EFi_HC = 7
                        EFi_NOx = 4.5
                    else:
                        EFi_CO = 55
                        EFi_HC = 3.5
                        EFi_NOx = 7.5
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    EFi_NH3 = 0.002
                elif 2000 <= car_birth <= 2002: # CO, HC, NOx 배출계수 누락(⭕)
                    if V <= 65:
                        EFi_CO = 70
                        EFi_HC = 7
                        EFi_NOx = 4.5
                    else:
                        EFi_CO = 55
                        EFi_HC = 3.5
                        EFi_NOx = 7.5
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.169
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.149
                    elif V > 90:
                        EFi_NH3 = 0.084
                elif 2003 <= car_birth <= 2008: # CO, HC, NOx 배출계수 누락(⭕)
                    if V <= 65:
                        EFi_CO = 70
                        EFi_HC = 7
                        EFi_NOx = 4.5
                    else:
                        EFi_CO = 55
                        EFi_HC = 3.5
                        EFi_NOx = 7.5
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                    if V <= 60:
                        EFi_NH3 = 0.002
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.03
                    elif V > 90:
                        EFi_NH3 = 0.065
                elif 2009 <= car_birth:
                    if V <= 60:
                        EFi_NH3 = 0.004
                    elif 60 < V <= 90:
                        EFi_NH3 = 0.008
                    elif V > 90:
                        EFi_NH3 = 0.022
            elif (car_type == '화물') and (car_size == '대형'):
                if 1993 <= car_birth:
                    if V <= 65:
                        EFi_CO = 70
                        EFi_HC = 7
                        EFi_NOx = 4.5
                    else:
                        EFi_CO = 55
                        EFi_HC = 3.5
                        EFi_NOx = 7.5
                    if V < 85:
                        EFi_PM10 = 0.00030
                        EFi_PM2_5 = k * 0.00030
                    else:
                        EFi_PM10 = 0.00075
                        EFi_PM2_5 = k * 0.00075
                else:
                    EFi_CO = 1
                    EFi_HC = 1
                    EFi_NOx = 1
                    EFi_PM10 = 1
                    EFi_PM2_5 = 1
                EFi_NH3 = 0.002
        elif fuel == '경유':
            if (car_type == '승용') and (car_size == '경형'):
                if car_birth <= 2005:
                    EFi_CO = 0.7392 * (V**-0.7524)
                    EFi_HC = 0.0989 * (V**-0.6848)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.0839 * (V**-0.3420)
                    EFi_PM2_5 = k * 0.0839 * (V**-0.3420)
                    EFi_NH3 = 0.001
                elif 2006 <= car_birth <= 2010:
                    EFi_CO = 0.5775 * (V**-0.7524)
                    EFi_HC = 0.0825 * (V**-0.6848)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.0420 * (V**-0.3420)
                    EFi_PM2_5 = k * 0.0420 * (V**-0.3420)
                    EFi_NH3 = 0.001
                elif 2011 <= car_birth <= 2015:
                    EFi_CO = 0.5141 * (V**-0.6792)
                    EFi_HC = 0.3713 * (V**-0.7513)
                    EFi_NOx = 0.0003 * (V**2) - 0.0324 * V + 1.4773
                    if V <= 65.4:
                        EFi_PM10 = 0.0225 * (V**-0.7264)
                        EFi_PM2_5 = k * 0.0225 * (V**-0.7264)
                    else:
                        EFi_PM10 = 0.0009 * (V**0.0416)      # EFi_PM10 = 0.0009 * (V**0.0416) (⭕)
                        EFi_PM2_5 = k * 0.0009 * (V**0.0416) # EFi_PM2_5 = k * 0.0009 * (V**0.0416) (⭕)
                    EFi_NH3 = 0.0019
                elif 2016 <= car_birth:
                    EFi_CO = 0.4574 * (V**-0.5215)
                    EFi_HC = 0.1300 * (V**-0.7265)
                    EFi_NOx = 2.7702 * (V**-0.3869)
                    if V <= 65.4:
                        EFi_PM10 = 0.0225 * (V**-0.7264)
                        EFi_PM2_5 = k * 0.0225 * (V**-0.7264)
                    else:
                        EFi_PM10 = 0.0009 * (V**0.0416)     # EFi_PM10 = 0.0009 * (V**0.0416) (⭕)
                        EFi_PM2_5 = k * 0.0009 * (V**0.0416) # EFi_PM2_5 = k * 0.0009 * (V**0.0416) (⭕)
                    EFi_NH3 = 0.0019
            elif (car_type == '승용') and (car_size == '소형'):
                if car_birth <= 2004:
                    EFi_CO = 5.9672 * (V**-0.9534)
                    EFi_HC = 0.6523 * (V**-1.0167)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    if V <= 65:
                        EFi_PM10 = 0.3861 * (V**-0.5093)
                        EFi_PM2_5 = k * 0.3861 * (V**-0.5093)
                    else:
                        EFi_PM10 = -0.00001 * (V**2) + 0.0026 * V - 0.0618
                        EFi_PM2_5 = k * -0.00001 * (V**2) + 0.0026 * V - 0.0618
                    EFi_NH3 = 0.001
                elif car_birth == 2005:
                    EFi_CO = 0.7392 * (V**-0.7524)
                    EFi_HC = 0.0989 * (V**-0.6848)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.0839 * (V**-0.3420)
                    EFi_PM2_5 = k * 0.0839 * (V**-0.3420)
                    EFi_NH3 = 0.001
                elif 2006 <= car_birth <= 2010:
                    EFi_CO = 0.5775 * (V**-0.7524)
                    EFi_HC = 0.0825 * (V**-0.6848)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.0420 * (V**-0.3420)
                    EFi_PM2_5 = k * 0.0420 * (V**-0.3420)
                    EFi_NH3 = 0.001
                elif 2011 <= car_birth <= 2015:
                    EFi_CO = 0.5141 * (V**-0.6792)
                    EFi_HC = 0.3713 * (V**-0.7513)
                    EFi_NOx = 0.0003 * (V**2) - 0.0324 * V + 1.4773
                    if V <= 65.4:
                        EFi_PM10 = 0.0225 * (V**-0.7264)
                        EFi_PM2_5 = k * 0.0225 * (V**-0.7264)
                    else:
                        EFi_PM10 = 0.0009 * (V**0.0416)        # EFi_PM10 = 0.0009 * (V**0.0416) (⭕)
                        EFi_PM2_5 = k * 0.0009 * (V**0.0416)   # EFi_PM2_5 = k * 0.0009 * (V**0.0416) (⭕)
                    EFi_NH3 = 0.0019
                elif 2016 <= car_birth:
                    EFi_CO = 0.4574 * (V**-0.5215)
                    EFi_HC = 0.1300 * (V**-0.7265)
                    EFi_NOx = 2.7702 * (V**-0.3869)
                    if V <= 65.4:
                        EFi_PM10 = 0.0225 * (V**-0.7264)
                        EFi_PM2_5 = k * 0.0225 * (V**-0.7264)
                    else:
                        EFi_PM10 = 0.0009 * (V**0.0416)           # EFi_PM10 = 0.0009 * (V**0.0416) (⭕)
                        EFi_PM2_5 = k * 0.0009 * (V**0.0416)      # EFi_PM2_5 = k * 0.0009 * (V**0.0416) (⭕)
                    EFi_NH3 = 0.0019
            elif (car_type == '승용') and (car_size == '중형'):
                if car_birth <= 2004:
                    EFi_CO = 5.9672 * (V**-0.9534)
                    EFi_HC = 0.6523 * (V**-1.0167)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    if V <= 65:
                        EFi_PM10 = 0.3861 * (V**-0.5093)
                        EFi_PM2_5 = k * 0.3861 * (V**-0.5093)
                    else:
                        EFi_PM10 = -0.00001 * (V**2) + 0.0026 * V - 0.0618
                        EFi_PM2_5 = k * -0.00001 * (V**2) + 0.0026 * V - 0.0618
                    EFi_NH3 = 0.001
                elif car_birth == 2005:
                    EFi_CO = 0.6930 * (V**-0.7524)
                    EFi_HC = 0.1865 * (V**-0.6848)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.0723 * (V**-0.3420)
                    EFi_PM2_5 = k * 0.0723 * (V**-0.3420)
                    EFi_NH3 = 0.001
                elif 2006 <= car_birth <= 2010:
                    EFi_CO = 0.5414 * (V**-0.7524)
                    EFi_HC = 0.0927 * (V**-0.6848)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.0396 * (V**-0.3420)
                    EFi_PM2_5 = k * 0.0396 * (V**-0.3420)
                    EFi_NH3 = 0.001
                elif 2011 <= car_birth <= 2015:
                    EFi_CO = 0.5141 * (V**-0.6792)
                    EFi_HC = 0.3713 * (V**-0.7513)
                    EFi_NOx = 0.0003 * (V**2) - 0.0324 * V + 1.4773
                    if V <= 65.4:
                        EFi_PM10 = 0.0225 * (V**-0.7264)
                        EFi_PM2_5 = k * 0.0225 * (V**-0.7264)
                    else:
                        EFi_PM10 = 0.0009 * (V**0.0416)            # EFi_PM10 = 0.0009 * (V**0.0416) (⭕)
                        EFi_PM2_5 = k * 0.0009 * (V**0.0416)      # EFi_PM2_5 = k * 0.0009 * (V**0.0416) (⭕)
                    EFi_NH3 = 0.0019
                elif 2016 <= car_birth:
                    EFi_CO = 0.4574 * (V**-0.5215)
                    EFi_HC = 0.1300 * (V**-0.7265)
                    EFi_NOx = 2.7702 * (V**-0.3869)
                    if V <= 65.4:
                        EFi_PM10 = 0.0225 * (V**-0.7264)
                        EFi_PM2_5 = k * 0.0225 * (V**-0.7264)
                    else:
                        EFi_PM10 = 0.0009 * (V**0.0416)              # EFi_PM10 = 0.0009 * (V**0.0416) (⭕)
                        EFi_PM2_5 = k * 0.0009 * (V**0.0416)         # EFi_PM2_5 = k * 0.0009 * (V**0.0416) (⭕)
                    EFi_NH3 = 0.0019
            elif (car_type == '승용') and (car_size == '대형'):
                if car_birth <= 2004:
                    EFi_CO = 5.9672 * (V**-0.9534)
                    EFi_HC = 0.6523 * (V**-1.0167)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    if V <= 65:
                        EFi_PM10 = 0.3861 * (V**-0.5093)
                        EFi_PM2_5 = k * 0.3861 * (V**-0.5093)
                    else:
                        EFi_PM10 = -0.00001 * (V**2) + 0.0026 * V - 0.0618
                        EFi_PM2_5 = k * -0.00001 * (V**2) + 0.0026 * V - 0.0618
                    EFi_NH3 = 0.001
                elif car_birth == 2005:
                    EFi_CO = 0.9609 * (V**-0.7524)
                    EFi_HC = 0.1865 * (V**-0.6848)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.0723 * (V**-0.3420)
                    EFi_PM2_5 = k * 0.0723 * (V**-0.3420)
                    EFi_NH3 = 0.001
                elif 2006 <= car_birth <= 2010:
                    EFi_CO = 0.7507 * (V**-0.7524)
                    EFi_HC = 0.1554 * (V**-0.6848)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.0361 * (V**-0.3420)
                    EFi_PM2_5 = k * 0.0361 * (V**-0.3420)
                    EFi_NH3 = 0.001
                elif 2011 <= car_birth <= 2015:
                    EFi_CO = 0.5141 * (V**-0.6792)
                    EFi_HC = 0.3713 * (V**-0.7513)
                    EFi_NOx = 0.0003 * (V**2) - 0.0324 * V + 1.4773
                    if V <= 65.4:
                        EFi_PM10 = 0.0225 * (V**-0.7264)
                        EFi_PM2_5 = k * 0.0225 * (V**-0.7264)
                    else:
                        EFi_PM10 = 0.0009 * (V**0.0416)              # EFi_PM10 = 0.0009 * (V**0.0416) (⭕)
                        EFi_PM2_5 = k * 0.0009 * (V**0.0416)         # EFi_PM2_5 = k * 0.0009 * (V**0.0416) (⭕)
                    EFi_NH3 = 0.0019
                elif 2016 <= car_birth:
                    EFi_CO = 0.4574 * (V**-0.5215)
                    EFi_HC = 0.1300 * (V**-0.7265)
                    EFi_NOx = 2.7702 * (V**-0.3869)
                    if V <= 65.4:
                        EFi_PM10 = 0.0225 * (V**-0.7264)
                        EFi_PM2_5 = k * 0.0225 * (V**-0.7264)
                    else:
                        EFi_PM10 = 0.0009 * (V**0.0416)              # EFi_PM10 = 0.0009 * (V**0.0416) (⭕)
                        EFi_PM2_5 = k * 0.0009 * (V**0.0416)         # EFi_PM2_5 = k * 0.0009 * (V**0.0416) (⭕)
                    EFi_NH3 = 0.0019
            elif (car_type == '승합') and (car_size == '소형'):
                if car_birth <= 1990:
                    if V <= 65.4:
                        EFi_CO = 3.4539 * (V**-0.4266)
                    else:
                        EFi_CO = 0.0051 * V + 0.2212
                    EFi_HC = 0.9835 * (V**-0.5096)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 1.1412 * (V**-0.4324)
                    EFi_PM2_5 = k * 1.1412 * (V**-0.4324)
                elif 1991 <= car_birth <= 1995:
                    if V <= 65.4:
                        EFi_CO = 3.4539 * (V**-0.4266)
                    else:
                        EFi_CO = 0.0051 * V + 0.2212
                    EFi_HC = 1.6313 * (V**-0.7298)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.5999 * (V**-0.3294)
                    EFi_PM2_5 = k * 0.5999 * (V**-0.3294)
                elif 1996 <= car_birth <= 1997:
                    if V <= 65.4:
                        EFi_CO = 3.4539 * (V**-0.4266)
                    else:
                        EFi_CO = 0.0051 * V + 0.2212
                    EFi_HC = 1.1293 * (V**-0.6588)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.6408 * (V**-0.3596)
                    EFi_PM2_5 = k * 0.6408 * (V**-0.3596)
                elif 1998 <= car_birth <= 1999:
                    EFi_CO = 3.7564 * (V**-0.5175)
                    EFi_HC = 1.1293 * (V**-0.6588)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.5168 * (V**-0.3596)
                    EFi_PM2_5 = k * 0.5168 * (V**-0.3596)
                elif 2000 <= car_birth <= 2003:
                    EFi_CO = 3.7564 * (V**-0.5175)
                    EFi_HC = 1.1293 * (V**-0.6588)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.2894 * (V**-0.3596)
                    EFi_PM2_5 = k * 0.2894 * (V**-0.3596)
                elif 2004 <= car_birth <= 2007:
                    EFi_CO = 3.2797 * (V**-0.8887)
                    EFi_HC = 0.1807 * (V**-0.6588)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.2067 * (V**-0.3596)
                    EFi_PM2_5 = k * 0.2067 * (V**-0.3596)
                elif 2008 <= car_birth <= 2011:
                    if V <= 65.4:
                        EFi_CO = 4.222 * (V**-1.4035)
                    else:
                        EFi_CO = 0.01166 * (V**0.09222)         # EFi_CO = 0.01166 * (V**0.09222) (⭕)
                    if V <= 97.3:                               # 97.3 초과에 대한 내용 없음.(편람 p.371)
                        EFi_HC = 0.829 * (V**-1.0961)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.3111 * (V**-0.5125)
                    EFi_PM2_5 = k * 0.3111 * (V**-0.5125)
                elif 2012 <= car_birth:
                    if V <= 65.4:
                        EFi_CO = 4.222 * (V**-1.4035)
                    else:
                        EFi_CO = 0.01166 * (V**0.09222)         # EFi_CO = 0.01166 * (V**0.09222) (⭕)
                    if V <= 97.3:                               # 97.3 초과에 대한 내용 없음.(편람 p.371)
                        EFi_HC = 0.829 * (V**-1.0961)
                    if V <= 65.4:
                        EFi_NOx = 2.0217 * (V**-0.2645)
                    else:
                        EFi_NOx = 0.0271 * (V**0.7596)
                    EFi_PM10 = 0.1119 * (V**-0.5125)
                    EFi_PM2_5 = k * 0.1119 * (V**-0.5125)
                EFi_NH3 = 0.001
            elif (car_type == '승합') and (car_size == '중형'):
                if car_birth <= 1995:
                    EFi_CO = 32.550 * (V**-0.4944)
                    EFi_HC = 15.753 * (V**-0.5912)
                    if V < 80:
                        EFi_NOx = 40.692 * (V**-0.5590)
                    else:
                        EFi_NOx = -0.0023 * (V**2) + 0.5381 * V - 23.590
                    EFi_PM10 = 5.4886 * (V**-0.5911)
                    EFi_PM2_5 = k * 5.4886 * (V**-0.5911)
                elif 1996 <= car_birth <= 1997:
                    EFi_CO = 16.410 * (V**-0.3790)
                    EFi_HC = 4.2324 * (V**-0.3926)
                    if V < 80:
                        EFi_NOx = 22.804 * (V**-0.4660)
                    else:
                        EFi_NOx = -0.0021 * (V**2) + 0.4430 * V - 18.730
                    EFi_PM10 = 1.6593 * (V**-0.3935)
                    EFi_PM2_5 = k * 1.6593 * (V**-0.3935)
                elif 1998 <= car_birth <= 2000:
                    EFi_CO = 16.410 * (V**-0.3790)
                    EFi_HC = 4.2324 * (V**-0.3926)
                    if V < 80:
                        EFi_NOx = 25.708 * (V**-0.4772)
                    else:
                        EFi_NOx = 0.0019 * (V**2) - 0.2628 * V + 12.145
                    EFi_PM10 = 1.6593 * (V**-0.3935)
                    EFi_PM2_5 = k * 1.6593 * (V**-0.3935)
                elif 2001 <= car_birth <= 2004:
                    EFi_CO = 16.378 * (V**-0.5340)
                    EFi_HC = 5.8477 * (V**-0.5466)
                    if V < 80:
                        EFi_NOx = 25.436 * (V**-0.4656)
                    else:
                        EFi_NOx = 0.0008 * (V**2) - 0.0482 * V + 1.8424
                    EFi_PM10 = 1.2848 * (V**-0.4715)
                    EFi_PM2_5 = k * 1.2848 * (V**-0.4715)
                elif 2005 <= car_birth <= 2007:
                    EFi_CO = 15.256 * (V**-0.7448)
                    EFi_HC = 2.0502 * (V**-0.6504)
                    EFi_NOx = 15.001 * (V**-0.4528)
                    EFi_PM10 = 0.2979 * (V**-0.4008)
                    EFi_PM2_5 = k * 1.0457 * (V**-0.4527)
                elif 2008 <= car_birth <= 2010:
                    EFi_CO = 8.1771 * (V**-0.7725)
                    EFi_HC = 1.2991 * (V**-0.6538)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                        EFi_PM10 = 0.0539 * (V**-0.5182)
                        EFi_PM2_5 = k * 0.0539 * (V**-0.5182)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                        EFi_PM10 = 2.0000*(10**-4) * V - 7.5600*(10**-3)
                        EFi_PM2_5 = k * 2.0000*(10**-4) * V - 7.5600*(10**-3)
                elif 2011 <= car_birth <= 2014:
                    EFi_CO = 4.5201 * (V**-0.7279)
                    EFi_HC = 1.6826 * (V**-0.8045)
                    if V <= 64.7:
                        EFi_NOx = 17.2485 * (V**-0.4040)
                        EFi_PM10 = 0.0469 * (V**-0.4674)
                        EFi_PM2_5 = k * 0.0469 * (V**-0.4674)
                    else:
                        EFi_NOx = 1.1797 * (V**0.2308)
                        EFi_PM10 = 1.6800*(10**-4) * V - 5.1600*(10**-3)
                        EFi_PM2_5 = k * 1.6800*(10**-4) * V - 5.1600*(10**-3)
                elif 2015 <= car_birth:
                    EFi_CO = 7.4065 * (V**-0.5995)
                    EFi_HC = 2.4562 * (V**-1.3145)
                    EFi_NOx = 42.7393 * (V**-1.2949)
                    if V <= 64.7:
                        EFi_PM10 = 0.0081 * (V**-0.5182)
                        EFi_PM2_5 = k * 0.0081 * (V**-0.5182)
                    else:
                        EFi_PM10 = 3.0000*(10**-5) * V - 0.0011
                        EFi_PM2_5 = k * 3.0000*(10**-5) * V - 0.0011
                EFi_NH3 = 0.001
            elif (car_type == '승합') and (car_size == '대형'):
                if car_birth <= 1995:
                    EFi_CO = 28.205 * (V**-0.5337)
                    EFi_HC = 6.1146 * (V**-0.4979)
                    EFi_NOx = 41.346 * (V**-0.3645)
                    EFi_PM10 = 5.2158 * (V**-0.5048)
                    EFi_PM2_5 = k * 5.2158 * (V**-0.5048)
                    EFi_NH3 = 0.003
                elif 1996 <= car_birth <= 1997:
                    EFi_CO = 23.205 * (V**-0.5425)
                    EFi_HC = 6.5657 * (V**-0.5431)
                    EFi_NOx = 42.1379 * (V**-0.3786)
                    EFi_PM10 = 2.4911 * (V**-0.4149)
                    EFi_PM2_5 = k * 2.4911 * (V**-0.4149)
                    EFi_NH3 = 0.003
                elif 1998 <= car_birth <= 2000:
                    EFi_CO = 23.205 * (V**-0.5425)
                    EFi_HC = 6.5657 * (V**-0.5431)
                    EFi_NOx = 42.1379 * (V**-0.3786)
                    EFi_PM10 = 1.4432 * (V**-0.3870)
                    EFi_PM2_5 = k * 1.4432 * (V**-0.3870)
                    EFi_NH3 = 0.003
                elif car_birth == 2001:
                    EFi_CO = 21.348 * (V**-0.5806)
                    EFi_HC = 6.6390 * (V**-0.5760)
                    EFi_NOx = 36.7191 * (V**-0.3548)
                    EFi_PM10 = 0.9375 * (V**-0.3910)
                    EFi_PM2_5 = k * 0.9375 * (V**-0.3910)
                    EFi_NH3 = 0.003
                elif 2002 <= car_birth <= 2004:
                    EFi_CO = 21.348 * (V**-0.5806)
                    EFi_HC = 6.6390 * (V**-0.5760)
                    EFi_NOx = 36.7191 * (V**-0.3548)
                    EFi_PM10 = 1.1507 * (V**-0.4804)
                    EFi_PM2_5 = k * 1.1507 * (V**-0.4804)
                    EFi_NH3 = 0.003
                elif 2005 <= car_birth <= 2007:
                    EFi_CO = 9.6452 * (V**-0.5291)
                    EFi_HC = 3.2339 * (V**-0.7436)
                    EFi_NOx = 30.5870 * (V**-0.3548)
                    if V <= 80:
                        EFi_PM10 = 0.4657 * (V**-0.5634)
                        EFi_PM2_5 = k * 0.4657 * (V**-0.5634)
                    else:
                        EFi_PM10 = 0.0014 * (V**0.7970)
                        EFi_PM2_5 = k * 0.0014 * (V**0.7970)
                    EFi_NH3 = 0.003
                elif 2008 <= car_birth <= 2010:
                    EFi_CO = 6.8493 * (V**-0.6506)
                    EFi_HC = 1.7177 * (V**-0.6781)
                    EFi_NOx = 40.7564 * (V**-0.4757)
                    EFi_PM10 = 0.2418 * (V**-0.4727)
                    EFi_PM2_5 = k * 0.2418 * (V**-0.4727)
                    EFi_NH3 = 0.003
                elif 2011 <= car_birth <= 2014:
                    EFi_CO = 5.4607 * (V**-0.2990)
                    EFi_HC = 0.8863 * (V**-0.6933)
                    EFi_NOx = 40.3729 * (V**-0.5386)
                    EFi_PM10 = 0.2125 * (V**-0.4650)
                    EFi_PM2_5 = k * 0.2125 * (V**-0.4650)
                    EFi_NH3 = 0.003
                elif 2015 <= car_birth:
                    EFi_CO = 11.4415 * (V**-0.8036)
                    EFi_HC = 0.6774 * (V**-0.8321)
                    EFi_NOx = 112.1229 * (V**-1.6393)
                    EFi_PM10 = 0.0363 * (V**-0.4727)
                    EFi_PM2_5 = k * 0.0363 * (V**-0.4727)
                    EFi_NH3 = 0.007
            elif (car_type == '화물') and (car_size == '소형'):
                if car_birth <= 1990:
                    EFi_CO = 4.5854 * (V**-0.3613)
                    EFi_HC = 0.4840 * (V**-0.2756)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.8117 * (V**-0.4071)
                    EFi_PM2_5 = k * 0.8117 * (V**-0.4071)
                elif 1991 <= car_birth <= 1995:
                    EFi_CO = 3.4774 * (V**-0.3483)
                    EFi_HC = 0.4844 * (V**-0.3288)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.6188 * (V**-0.4540)
                    EFi_PM2_5 = k * 0.6188 * (V**-0.4540)
                elif 1996 <= car_birth <= 1997:
                    EFi_CO = 3.3934 * (V**-0.3837)
                    EFi_HC = 0.4955 * (V**-0.3393)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.7037 * (V**-0.5357)
                    EFi_PM2_5 = k * 0.7037 * (V**-0.5357)
                elif 1998 <= car_birth <= 1999:
                    EFi_CO = 4.0896 * (V**-0.6083)
                    EFi_HC = 0.6122 * (V**-0.5684)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.6157 * (V**-0.5357)
                    EFi_PM2_5 = k * 0.6157 * (V**-0.5357)
                elif 2000 <= car_birth <= 2003:
                    EFi_CO = 4.0896 * (V**-0.6083)
                    EFi_HC = 0.6122 * (V**-0.5684)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.4838 * (V**-0.5357)
                    EFi_PM2_5 = k * 0.4838 * (V**-0.5357)
                elif 2004 <= car_birth <= 2007:
                    EFi_CO = 3.2797 * (V**-0.8887)
                    EFi_HC = 0.1807 * (V**-0.6588)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.2067 * (V**-0.3596)
                    EFi_PM2_5 = k * 0.2067 * (V**-0.3596)
                elif 2008 <= car_birth <= 2011:
                    if V <= 65.4:
                        EFi_CO = 4.222 * (V**-1.4035)
                    else:
                        EFi_CO = 0.01166 * (V**0.09222)
                    if V <= 97.3:                               # 97.3 초과에 대한 내용 없음.(편람 p.374)
                        EFi_HC = 0.829 * (V**-1.0961)
                    EFi_NOx = 24.3491 * (V**-0.7277)
                    EFi_PM10 = 0.3111 * (V**-0.5125)
                    EFi_PM2_5 = k * 0.3111 * (V**-0.5125)
                elif 2012 <= car_birth <= 2016:
                    if V <= 65.4:
                        EFi_CO = 4.222 * (V**-1.4035)
                        EFi_NOx = 2.0217 * (V**-0.2645)
                    else:
                        EFi_CO = 0.01166 * (V**0.09222)
                        EFi_NOx = 0.0271 * (V**0.7596)
                    if V <= 97.3:                               # 97.3 초과에 대한 내용 없음.(편람 p.374)
                        EFi_HC = 0.829 * (V**-1.0961)
                    EFi_PM10 = 0.1119 * (V**-0.5125)
                    EFi_PM2_5 = k * 0.1119 * (V**-0.5125)
                EFi_NH3 = 0.001
            elif (car_type == '화물') and (car_size == '중형'):
                if car_birth <= 1995:
                    EFi_CO = 16.769 * (V**-0.3772)
                    EFi_HC = 6.7755 * (V**-0.5003)
                    EFi_NOx = 24.915 * (V**-0.3942)
                    EFi_PM10 = 3.6772 * (V**-0.5514)
                    EFi_PM2_5 = k * 3.6772 * (V**-0.5514)
                elif 1996 <= car_birth <= 2000:
                    EFi_CO = 21.057 * (V**-0.4958)
                    EFi_HC = 6.7532 * (V**-0.5711)
                    EFi_NOx = 25.022 * (V**-0.4240)
                    EFi_PM10 = 3.5285 * (V**-0.5962)
                    EFi_PM2_5 = k * 3.5285 * (V**-0.5962)
                elif car_birth == 2001:
                    EFi_CO = 23.501 * (V**-0.6100)        # EFi_CO = 23.501 * (V**-0.6100) (⭕)     
                    EFi_HC = 6.8738 * (V**-0.5913)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                    EFi_PM10 = 1.4444 * (V**-0.4824)
                    EFi_PM2_5 = k * 1.4444 * (V**-0.4824)
                elif 2002 <= car_birth <= 2004:
                    EFi_CO = 23.501 * (V**-0.6100)        # EFi_CO = 23.501 * (V**-0.6100) (⭕)
                    EFi_HC = 6.8738 * (V**-0.5913)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                    EFi_PM10 = 1.0432 * (V**-0.4992)
                    EFi_PM2_5 = k * 1.0432 * (V**-0.4992)
                elif 2005 <= car_birth <= 2007:
                    EFi_CO = 15.256 * (V**-0.7448)
                    EFi_HC = 2.0502 * (V**-0.6504)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                    EFi_PM10 = 0.2979 * (V**-0.4008)
                    EFi_PM2_5 = k * 0.2979 * (V**-0.4008)
                elif 2008 <= car_birth <= 2010:
                    EFi_CO = 8.1771 * (V**-0.7725)
                    EFi_HC = 1.2991 * (V**-0.6538)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                        EFi_PM10 = 0.0539 * (V**-0.5182)
                        EFi_PM2_5 = k * 0.0539 * (V**-0.5182)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                        EFi_PM10 = 0.0002 * V - 0.00756
                        EFi_PM2_5 = k * 0.0002 * V - 0.00756
                elif 2011 <= car_birth <= 2014:
                    EFi_CO = 4.5201 * (V**-0.7279)
                    EFi_HC = 1.6826 * (V**-0.8045)
                    if V <= 64.7:
                        EFi_NOx = 17.2485 * (V**-0.4040)
                        EFi_PM10 = 0.0469 * (V**-0.4674)
                        EFi_PM2_5 = k * 0.0469 * (V**-0.4674)
                    else:
                        EFi_NOx = 1.1797 * (V**0.2308)
                        EFi_PM10 = 0.000168 * V - 0.00516
                        EFi_PM2_5 = k * 0.000168 * V - 0.00516
                elif 2015 <= car_birth:
                    EFi_CO = 7.4065 * (V**-0.5995)
                    EFi_HC = 2.4562 * (V**-1.3145)
                    EFi_NOx = 42.7393 * (V**-1.2949)
                    if V <= 64.7:
                        EFi_PM10 = 0.0081 * (V**-0.5182)
                        EFi_PM2_5 = k * 0.0081 * (V**-0.5182)
                    else:
                        EFi_PM10 = 3.0000 * (10**-5) * V - 0.0011
                        EFi_PM2_5 = k * 3.0000 * (10**-5) * V - 0.0011
                EFi_NH3 = 0.001
            elif car_type == '특수':
                if car_birth <= 1995:
                    EFi_CO = 16.769 * (V**-0.3772)
                    EFi_HC = 6.7755 * (V**-0.5003)
                    EFi_NOx = 24.915 * (V**-0.3942)
                    EFi_PM10 = 3.6772 * (V**-0.5514)
                    EFi_PM2_5 = k * 3.6772 * (V**-0.5514)
                    EFi_NH3 = 0.003
                elif 1996 <= car_birth <= 2000:
                    EFi_CO = 21.057 * (V**-0.4958)
                    EFi_HC = 6.7532 * (V**-0.5711)
                    EFi_NOx = 25.022 * (V**-0.4240)
                    EFi_PM10 = 3.5285 * (V**-0.5962)
                    EFi_PM2_5 = k * 3.5285 * (V**-0.5962)
                    EFi_NH3 = 0.003
                elif car_birth == 2001:
                    EFi_CO = 23.501 * (V**-0.6100)             # EFi_CO = 23.501 * (V**-0.6100) (⭕)
                    EFi_HC = 6.8738 * (V**-0.5913)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                    EFi_PM10 = 1.4444 * (V**-0.4824)
                    EFi_PM2_5 = k * 1.4444 * (V**-0.4824)
                    EFi_NH3 = 0.003
                elif 2002 <= car_birth <= 2004:
                    EFi_CO = 23.501 * (V**-0.6100)             # EFi_CO = 23.501 * (V**-0.6100) (⭕)
                    EFi_HC = 6.8738 * (V**-0.5913)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                    EFi_PM10 = 1.0432 * (V**-0.4992)
                    EFi_PM2_5 = k * 1.0432 * (V**-0.4992)
                    EFi_NH3 = 0.003
                elif 2005 <= car_birth <= 2007:
                    EFi_CO = 15.256 * (V**-0.7448)
                    EFi_HC = 2.0502 * (V**-0.6504)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                    EFi_PM10 = 0.2979 * (V**-0.4008)
                    EFi_PM2_5 = k * 0.2979 * (V**-0.4008)
                    EFi_NH3 = 0.003
                elif 2008 <= car_birth <= 2009:
                    EFi_CO = 8.1771 * (V**-0.7725)
                    EFi_HC = 1.2991 * (V**-0.6538)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                        EFi_PM10 = 0.0539 * (V**-0.5182)
                        EFi_PM2_5 = k * 0.0539 * (V**-0.5182)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                        EFi_PM10 = 0.0002 * V - 0.00756
                        EFi_PM2_5 = k * 0.0002 * V - 0.00756
                    EFi_NH3 = 0.003
                elif car_birth == 2010:
                    EFi_CO = 8.1771 * (V**-0.7725)
                    EFi_HC = 1.2991 * (V**-0.6538)
                    if V <= 64.7:
                        EFi_NOx = 17.3032 * (V**-0.3660)
                        EFi_PM10 = 0.0539 * (V**-0.5182)
                        EFi_PM2_5 = k * 0.0539 * (V**-0.5182)
                    else:
                        EFi_NOx = 0.3259 * (V**0.5773)
                        EFi_PM10 = 0.0002 * V - 0.00756
                        EFi_PM2_5 = k * 0.0002 * V - 0.00756
                    EFi_NH3 = 0.011
                elif 2011 <= car_birth <= 2014:
                    EFi_CO = 4.5201 * (V**-0.7279)
                    EFi_HC = 1.6826 * (V**-0.8045)
                    if V <= 64.7:
                        EFi_NOx = 17.2485 * (V**-0.4040)
                        EFi_PM10 = 0.0469 * (V**-0.4674)
                        EFi_PM2_5 = k * 0.0469 * (V**-0.4674)
                    else:
                        EFi_NOx = 1.1797 * (V**0.2308)
                        EFi_PM10 = 0.000168 * V - 0.00516
                        EFi_PM2_5 = k * 0.000168 * V - 0.00516
                    EFi_NH3 = 0.011
                elif 2015 <= car_birth:
                    EFi_CO = 7.4065 * (V**-0.5995)
                    EFi_HC = 2.4562 * (V**-1.3145)
                    EFi_NOx = 42.7393 * (V**-1.2949)
                    if V <= 64.7:
                        EFi_PM10 = 0.0081 * (V**-0.5182)
                        EFi_PM2_5 = k * 0.0081 * (V**-0.5182)
                    else:
                        EFi_PM10 = 3.0000 * (10**-5) * V - 0.0011
                        EFi_PM2_5 = k * 3.0000 * (10**-5) * V - 0.0011
                    EFi_NH3 = 0.007
            elif (car_type == '화물') and (car_size == '대형'):
                if car_birth <= 1995:
                    EFi_CO = 30.402 * (V**-0.4685)
                    EFi_HC = 15.75 * (V**-0.582)
                    EFi_NOx = 117.49 * (V**-0.365)
                    EFi_PM10 = 7.6212 * (V**-0.4183)
                    EFi_PM2_5 = k * 7.6212 * (V**-0.4183)
                    EFi_NH3 = 0.003
                elif 1996 <= car_birth <= 1997:
                    EFi_CO = 18.101 * (V**-0.3454)
                    EFi_HC = 10.301 * (V**-0.5856)
                    EFi_NOx = 94.319 * (V**-0.4061)
                    EFi_PM10 = 6.0264 * (V**-0.4627)
                    EFi_PM2_5 = k * 6.0264 * (V**-0.4627)
                    EFi_NH3 = 0.003
                elif 1998 <= car_birth <= 2000:
                    EFi_CO = 18.101 * (V**-0.3454)
                    EFi_HC = 10.301 * (V**-0.5856)
                    EFi_NOx = 94.319 * (V**-0.4061)
                    EFi_PM10 = 4.873 * (V**-0.4382)
                    EFi_PM2_5 = k * 4.873 * (V**-0.4382)
                    EFi_NH3 = 0.003
                elif 2001 <= car_birth <= 2004:
                    EFi_CO = 28.399 * (V**-0.5999)
                    EFi_HC = 10.031 * (V**-0.5828)
                    EFi_NOx = 85.301 * (V**-0.4023)
                    EFi_PM10 = 3.7541 * (V**-0.4055)
                    EFi_PM2_5 = k * 3.7541 * (V**-0.4055)
                    EFi_NH3 = 0.003
                elif 2005 <= car_birth <= 2007:
                    EFi_CO = 52.136 * (V**-0.8618)
                    EFi_HC = 3.7878 * (V**-0.5425)
                    EFi_NOx = 107.5 * (V**-0.5679)
                    EFi_PM10 = 2.6847 * (V**-0.6112)
                    EFi_PM2_5 = k * 2.6847 * (V**-0.6112)
                    EFi_NH3 = 0.003
                elif car_birth == 2008:                    # 2008 <= car_birth <= 2009 -> 2008 == car_birth: 변경 (⭕)
                    EFi_CO = 6.8493 * (V**-0.6506)
                    EFi_HC = 1.7177 * (V**-0.6781)
                    EFi_NOx = 40.7564 * (V**-0.4757)
                    EFi_PM10 = 0.2418 * (V**-0.4727)
                    EFi_PM2_5 = k * 0.2418 * (V**-0.4727)
                    EFi_NH3 = 0.003
                elif 2009 <= car_birth <= 2010:
                    EFi_CO = 6.8493 * (V**-0.6506)
                    EFi_HC = 1.7177 * (V**-0.6781)
                    EFi_NOx = 40.7564 * (V**-0.4757)
                    EFi_PM10 = 0.2418 * (V**-0.4727)
                    EFi_PM2_5 = k * 0.2418 * (V**-0.4727)
                    EFi_NH3 = 0.011
                elif 2011 <= car_birth <= 2014:
                    EFi_CO = 5.4607 * (V**-0.2990)
                    EFi_HC = 0.8863 * (V**-0.6933)
                    EFi_NOx = 40.3729 * (V**-0.5386)
                    EFi_PM10 = 0.2125 * (V**-0.4650)
                    EFi_PM2_5 = k * 0.2125 * (V**-0.4650)
                    EFi_NH3 = 0.011
                elif 2015 <= car_birth:
                    EFi_CO = 4.3762 * (V**-0.4550)
                    EFi_HC = 0.3627 * (V**-0.7071)
                    EFi_NOx = 18.0405 * (V**-1.0986)
                    EFi_PM10 = 0.0363 * (V**-0.4727)
                    EFi_PM2_5 = k * 0.0363 * (V**-0.4727)
                    EFi_NH3 = 0.007
        elif fuel == 'LPG':
            if (car_type == '승용') and (car_size == '경형'):
                if car_birth <= 1996:
                    if V <= 45:
                        EFi_CO = 22.498 * (V**-0.6579)
                    else:
                        EFi_CO = 0.0006 * (V**2) + 0.0004 * V + 0.8272
                    EFi_HC = 12.961 * (V**-0.8364)
                    EFi_NOx = 4.0131 * (V**-0.3236)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1997 <= car_birth <= 1999:
                    if V <= 45:
                        EFi_CO = 19.887 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0004 * (V**2) + 0.0911 * V - 2.2698
                    EFi_HC = 2.2714 * (V**-0.7830)
                    EFi_NOx = 1.8528 * (V**-0.3889)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2000 <= car_birth <= 2002:
                    if V <= 45:
                        EFi_CO = 19.887 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0004 * (V**2) + 0.0911 * V - 2.2698
                    EFi_HC = 2.2714 * (V**-0.7830)
                    EFi_NOx = 5.8289 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2003 <= car_birth <= 2005:
                    if V <= 45:
                        EFi_CO = 19.887 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0004 * (V**2) + 0.0911 * V - 2.2698
                    EFi_HC = 1.1073 * (V**-0.7830)
                    EFi_NOx = 5.8289 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2006 <= car_birth <= 2007:
                    if V <= 45:
                        EFi_CO = 8.9904 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0002 * (V**2) + 0.0457 * V - 1.1403
                    EFi_HC = 0.3549 * (V**-0.7830)
                    EFi_NOx = 0.7228 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif car_birth == 2008:
                    if V <= 79.6:
                        EFi_CO = 0.7693 * (V**-0.7666)
                        EFi_HC = 0.1063 * (V**-1.0745)
                    else:
                        EFi_CO = 5.0000 * (10**-16) * (V**7.2766)
                        EFi_HC = 1.0000 * (10**-15) * (V**6.2696)
                    EFi_NOx = -4.0000 * (10**-6) * (V**2) + 6.0000 * (10**-4) * V + 5.5000 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2009 <= car_birth <= 2011:
                    if V <= 79.6:
                        EFi_CO = 0.7059 * (V**-0.7666)
                        EFi_HC = 0.0974 * (V**-1.0745)
                    else:
                        EFi_CO = 4.5878 * (10**-16) * (V**7.2766)
                        EFi_HC = 9.1667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.7333 * (10**-6) * (V**2) + 5.6000 * (10**-4) * V + 5.1333 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2012 <= car_birth <= 2013:
                    if V <= 79.6:
                        EFi_CO = 0.6830 * (V**-0.7666)
                        EFi_HC = 0.0943 * (V**-1.0745)
                    else:
                        EFi_CO = 4.4393 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.8667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.6373 * (10**-6) * (V**2) + 5.4560 * (10**-4) * V + 5.0013 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif car_birth == 2014:
                    if V <= 79.6:
                        EFi_CO = 0.6602 * (V**-0.7666)
                        EFi_HC = 0.0911 * (V**-1.0745)
                    else:
                        EFi_CO = 4.2909 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.5667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.5413 * (10**-6) * (V**2) + 5.3120 * (10**-4) * V + 4.8693 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2015 <= car_birth:
                    if V <= 79.6:
                        EFi_CO = 0.6374 * (V**-0.7666)
                        EFi_HC = 0.0879 * (V**-1.0745)
                    else:
                        EFi_CO = 4.1425 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.2667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.4453 * (10**-6) * (V**2) + 5.1680 * (10**-4) * V + 4.7373 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
            elif (car_type == '승용') and (car_size == '소형'):
                if car_birth <= 1990:
                    EFi_CO = 72.338 * (V**-0.7587)
                    EFi_HC = 26.532 * (V**-1.0423)
                    EFi_NOx = 9.3011 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1991 <= car_birth == 1996:
                    EFi_CO = 72.338 * (V**-0.7587)
                    EFi_HC = 101.79 * (V**-1.6823)
                    EFi_NOx = 11.287 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1997 <= car_birth <= 1999:
                    EFi_CO = 44.956 * (V**-1.0085)
                    EFi_HC = 11.173 * (V**-1.3927)
                    EFi_NOx = 7.5371 * (V**-0.7864)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2000 <= car_birth <= 2002:
                    EFi_CO = 44.956 * (V**-1.0085)
                    EFi_HC = 11.173 * (V**-1.3927)
                    EFi_NOx = 4.7108 * (V**-0.7864)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2003 <= car_birth <= 2005:
                    EFi_CO = 44.956 * (V**-1.0085)
                    EFi_HC = 3.2821 * (V**-1.3927)
                    EFi_NOx = 4.7108 * (V**-0.7864)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2006 <= car_birth <= 2007:
                    EFi_CO = 39.362 * (V**-1.0085)
                    EFi_HC = 2.8981 * (V**-1.3927)
                    EFi_NOx = 1.8419 * (V**-0.7864)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif car_birth == 2008:
                    if V <= 79.6:
                        EFi_CO = 0.7693 * (V**-0.7666)
                        EFi_HC = 0.1063 * (V**-1.0745)
                    else:
                        EFi_CO = 5.0000 * (10**-16) * (V**7.2766)
                        EFi_HC = 1.0000 * (10**-15) * (V**6.2696)
                    EFi_NOx = -4.0000 * (10**-6) * (V**2) + 6.0000 * (10**-4) * V + 5.5000 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2009 <= car_birth <= 2011:
                    if V <= 79.6:
                        EFi_CO = 0.7059 * (V**-0.7666)
                        EFi_HC = 0.0974 * (V**-1.0745)
                    else:
                        EFi_CO = 4.5878 * (10**-16) * (V**7.2766)
                        EFi_HC = 9.1667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.7333 * (10**-6) * (V**2) + 5.6000 * (10**-4) * V + 5.1333 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2012 <= car_birth <= 2013:
                    if V <= 79.6:
                        EFi_CO = 0.6830 * (V**-0.7666)
                        EFi_HC = 0.0943 * (V**-1.0745)
                    else:
                        EFi_CO = 4.4393 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.8667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.6373 * (10**-6) * (V**2) + 5.4560 * (10**-4) * V + 5.0013 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif car_birth == 2014:
                    if V <= 79.6:
                        EFi_CO = 0.6602 * (V**-0.7666)
                        EFi_HC = 0.0911 * (V**-1.0745)
                    else:
                        EFi_CO = 4.2909 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.5667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.5413 * (10**-6) * (V**2) + 5.3120 * (10**-4) * V + 4.8693 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2015 <= car_birth:
                    if V <= 79.6:
                        EFi_CO = 0.6374 * (V**-0.7666)
                        EFi_HC = 0.0879 * (V**-1.0745)
                    else:
                        EFi_CO = 4.1425 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.2667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.4453 * (10**-6) * (V**2) + 5.1680 * (10**-4) * V + 4.7373 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                if V <= 60:
                    EFi_NH3 = 0.0095
                elif 60 < V <= 90:
                    EFi_NH3 = 0.0082
                else:
                    EFi_NH3 = 0.022
            elif ( (car_type == '승용') and ( (car_size == '중형') or (car_size == '대형') ) or ( (car_type == '특수') and (car_size == '중형') ) ): # 최이사님 확인(2023.04.20) : 특수 중형 -> 승용 중형 조건으로 계산
                if car_birth <= 1990:
                    EFi_CO = 72.338 * (V**-0.7587)
                    EFi_HC = 26.532 * (V**-1.0423)
                    EFi_NOx = 9.3011 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1991 <= car_birth == 1996:
                    EFi_CO = 29.825 * (V**-0.6771)
                    EFi_HC = 26.520 * (V**-1.4041)
                    EFi_NOx = 11.287 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1997 <= car_birth <= 1999:
                    EFi_CO = 29.825 * (V**-0.6771)
                    EFi_HC = 4.7595 * (V**-0.9512)
                    EFi_NOx = 12.562 * (V**-0.8606)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2000 <= car_birth <= 2005:
                    EFi_CO = 17.829 * (V**-0.6778)
                    EFi_HC = 6.3668 * (V**-1.2849)
                    EFi_NOx = 8.8952 * (V**-0.8119)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2006 <= car_birth <= 2007:
                    EFi_CO = 73.022 * (V**-1.2078)
                    EFi_HC = 4.4166 * (V**-1.5356)
                    EFi_NOx = 2.0280 * (V**-0.7978)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif car_birth == 2008:
                    if V <= 79.6:
                        EFi_CO = 0.7693 * (V**-0.7666)
                        EFi_HC = 0.1063 * (V**-1.0745)
                    else:
                        EFi_CO = 5.0000 * (10**-16) * (V**7.2766)
                        EFi_HC = 1.0000 * (10**-15) * (V**6.2696)
                    EFi_NOx = -4.0000 * (10**-6) * (V**2) + 6.0000 * (10**-4) * V + 5.5000 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2009 <= car_birth <= 2011:
                    if V <= 79.6:
                        EFi_CO = 0.7059 * (V**-0.7666)
                        EFi_HC = 0.0974 * (V**-1.0745)
                    else:
                        EFi_CO = 4.5878 * (10**-16) * (V**7.2766)
                        EFi_HC = 9.1667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.7333 * (10**-6) * (V**2) + 5.6000 * (10**-4) * V + 5.1333 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2012 <= car_birth <= 2013:
                    if V <= 79.6:
                        EFi_CO = 0.6830 * (V**-0.7666)
                        EFi_HC = 0.0943 * (V**-1.0745)
                    else:
                        EFi_CO = 4.4393 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.8667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.6373 * (10**-6) * (V**2) + 5.4560 * (10**-4) * V + 5.0013 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif car_birth == 2014:
                    if V <= 79.6:
                        EFi_CO = 0.6602 * (V**-0.7666)
                        EFi_HC = 0.0911 * (V**-1.0745)
                    else:
                        EFi_CO = 4.2909 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.5667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.5413 * (10**-6) * (V**2) + 5.3120 * (10**-4) * V + 4.8693 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2015 <= car_birth:
                    if V <= 79.6:
                        EFi_CO = 0.6374 * (V**-0.7666)
                        EFi_HC = 0.0879 * (V**-1.0745)
                    else:
                        EFi_CO = 4.1425 * (10**-16) * (V**7.2766)
                        EFi_HC = 8.2667 * (10**-16) * (V**6.2696)
                    EFi_NOx = -3.4453 * (10**-6) * (V**2) + 5.1680 * (10**-4) * V + 4.7373 * (10**-3)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                if V <= 60:
                    EFi_NH3 = 0.0095
                elif 60 < V <= 90:
                    EFi_NH3 = 0.0082
                else:
                    EFi_NH3 = 0.022
            elif ( (car_type == '승합') and (car_size == '경형') ) or ( (car_type == '화물') and (car_size == '경형') ):
                if car_birth <= 1996:
                    if V <= 45:
                        EFi_CO = 22.498 * (V**-0.6579)
                    else:
                        EFi_CO = 0.0006 * (V**2) + 0.0004 * V + 0.8272
                    EFi_HC = 12.961 * (V**-0.8364)
                    EFi_NOx = 4.0131 * (V**-0.3236)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1997 <= car_birth == 1999:
                    if V <= 45:
                        EFi_CO = 19.887 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0004 * (V**2) + 0.0911 * V - 2.2698
                    EFi_HC = 2.2714 * (V**-0.7830)
                    EFi_NOx = 1.8528 * (V**-0.3889)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2000 <= car_birth <= 2002:
                    if V <= 45:
                        EFi_CO = 19.887 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0004 * (V**2) + 0.0911 * V - 2.2698
                    EFi_HC = 2.2714 * (V**-0.7830)
                    EFi_NOx = 5.8289 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2003 <= car_birth <= 2005:
                    if V <= 45:
                        EFi_CO = 19.887 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0004 * (V**2) + 0.0911 * V - 2.2698
                    EFi_HC = 1.1073 * (V**-0.7830)
                    EFi_NOx = 5.8289 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2006 <= car_birth:
                    if V <= 45:
                        EFi_CO = 8.9904 * (V**-0.8461)
                    else:
                        EFi_CO = -0.0002 * (V**2) + 0.0457 * V - 1.1403
                    EFi_HC = 0.3549 * (V**-0.7830)
                    EFi_NOx = 0.7228 * (V**-0.9198)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                if V <= 60:
                    EFi_NH3 = 0.0095
                elif 60 < V <= 90:
                    EFi_NH3 = 0.0082
                else:
                    EFi_NH3 = 0.022
            elif (car_type == '승합') and (car_size == '소형'):
                if car_birth <= 1990:
                    EFi_CO = 72.338 * (V**-0.7587)
                    EFi_HC = 26.532 * (V**-1.0423)
                    EFi_NOx = 9.3011 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1991 <= car_birth == 1996:
                    EFi_CO = 29.825 * (V**-0.6771)
                    EFi_HC = 26.520 * (V**-1.4041)
                    EFi_NOx = 11.287 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1997 <= car_birth <= 1999:
                    EFi_CO = 29.825 * (V**-0.6771)
                    EFi_HC = 4.7595 * (V**-0.9512)
                    EFi_NOx = 12.562 * (V**-0.8606)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2000 <= car_birth <= 2005:
                    EFi_CO = 17.829 * (V**-0.6778)
                    EFi_HC = 6.3668 * (V**-1.2849)
                    EFi_NOx = 8.8952 * (V**-0.8119)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2006 <= car_birth:
                    if V <= 65.4:
                        EFi_CO = 6.1701 * (V**-1.0719)
                        EFi_HC = 2.3782 * (V**-1.9979)
                    else:
                        EFi_CO = 3.0000 * (10**-10) * (V**4.5809)
                        EFi_HC = 5.0000 * (10**-10) * (V**3.4895)
                    if V <= 97.3:
                        EFi_NOx = 0.4758 * (V**-1.0665)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                if V <= 60:
                    EFi_NH3 = 0.0095
                elif 60 < V <= 90:
                    EFi_NH3 = 0.0082
                else:
                    EFi_NH3 = 0.022
            # 최이사님 확인(2023.04.20) : 화물 중형 -> 승합 중형 조건, 화물 대형 -> 승합 대형 조건으로 계산
            elif ( (car_type == '승합') and ( (car_size == '중형') or (car_size == '대형') ) ) or ( (car_type == '화물') and ((car_size == '소형') or (car_size == '중형') or (car_size == '대형')) ):
                if car_birth <= 1990:
                    EFi_CO = 72.338 * (V**-0.7587)
                    EFi_HC = 26.532 * (V**-1.0423)
                    EFi_NOx = 9.3011 * (V**-0.6781)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1991 <= car_birth == 1996:
                    EFi_CO = 29.825 * (V**-0.6771)
                    EFi_HC = 26.520 * (V**-1.4041)
                    EFi_NOx = 11.287 * (V**-0.7634)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 1997 <= car_birth <= 1999:
                    EFi_CO = 29.825 * (V**-0.6771)
                    EFi_HC = 4.7595 * (V**-0.9512)
                    EFi_NOx = 12.562 * (V**-0.8606)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2000 <= car_birth <= 2005:
                    EFi_CO = 17.829 * (V**-0.6778)
                    EFi_HC = 6.3668 * (V**-1.2849)
                    EFi_NOx = 8.8952 * (V**-0.8119)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                elif 2006 <= car_birth:
                    if V <= 65.4:
                        EFi_CO = 16.491 * (V**-1.4713)
                        EFi_HC = 1.9218 * (V**-1.8418)
                    else:
                        EFi_CO = 3.0000 * (10**-13) * (V**6.0619)
                        EFi_HC = 3.0000 * (10**-9) * (V**3.0639)
                    if V <= 97.3:
                        EFi_NOx = 0.1246 * (V**-0.441)
                    if V < 85:
                        EFi_PM10 = 0.0002
                        EFi_PM2_5 = k * 0.0002
                    else:
                        EFi_PM10 = 0.0005
                        EFi_PM2_5 = k * 0.0005
                if V <= 60:
                    EFi_NH3 = 0.0095
                elif 60 < V <= 90:
                    EFi_NH3 = 0.0082
                else:
                    EFi_NH3 = 0.022
        # else:
        #     # 값 고민 : 1
        #     # EFi_CO, EFi_HC, EFi_NOx, EFi_PM10, EFi_PM2_5 = np.nan, np.nan, np.nan, np.nan, np.nan
        #     EFi_CO, EFi_HC, EFi_NOx, EFi_PM10, EFi_PM2_5 = 1, 1, 1, 1, 1

        if V < 60:
            EFi_COm += EFi_CO*0.34
            EFi_HCm += EFi_HC*0.34
            EFi_NOxm += EFi_NOx*0.34
            EFi_PM10m += EFi_PM10*0.34
            EFi_PM2_5m += EFi_PM2_5*0.34
            EFi_NH3m += EFi_NH3*0.34
        elif 60 <= V < 90:
            EFi_COm += EFi_CO*0.33
            EFi_HCm += EFi_HC*0.33
            EFi_NOxm += EFi_NOx*0.33
            EFi_PM10m += EFi_PM10*0.33
            EFi_PM2_5m += EFi_PM2_5*0.33
            EFi_NH3m += EFi_NH3*0.33
        elif V > 90:
            EFi_COm += EFi_CO*0.33
            EFi_HCm += EFi_HC*0.33
            EFi_NOxm += EFi_NOx*0.33
            EFi_PM10m += EFi_PM10*0.33
            EFi_PM2_5m += EFi_PM2_5*0.33
            EFi_NH3m += EFi_NH3*0.33

    # 배출계수(EFi) 리스트에 저장
    EFi_CO_list.append(EFi_COm)
    EFi_HC_list.append(EFi_HCm)
    EFi_NOx_list.append(EFi_NOxm)
    EFi_PM10_list.append(EFi_PM10m)
    EFi_PM2_5_list.append(EFi_PM2_5m)
    EFi_NH3_list.append(EFi_NH3m)

df2['EFi_CO'] = EFi_CO_list
df2['EFi_HC'] = EFi_HC_list
df2['EFi_NOx'] = EFi_NOx_list
df2['EFi_PM10'] = EFi_PM10_list
df2['EFi_PM2_5'] = EFi_PM2_5_list
check_EFi_col = ['EFi_CO', 'EFi_HC', 'EFi_NOx', 'EFi_PM10', 'EFi_PM2_5']

## 배출량 계산
# E = VKT * (EFi / 1000) * DF * (1 - R / 100)
df2['E_HOT_CO'] = df2['VKT'] * (df2['EFi_CO'] / 1000) * df2['DF_CO'] * (1 - df2['R_CO'] / 100)
df2['E_HOT_HC'] = df2['VKT'] * (df2['EFi_HC'] / 1000) * df2['DF_HC'] * (1 - df2['R_HC'] / 100)
df2['E_HOT_NOx'] = df2['VKT'] * (df2['EFi_NOx'] / 1000) * df2['DF_NOx'] * (1 - 0 / 100)
df2['E_HOT_PM10'] = df2['VKT'] * (df2['EFi_PM10'] / 1000) * df2['DF_PM'] * (1 - df2['R_PM'] / 100)
df2['E_HOT_PM2_5'] = df2['VKT'] * (df2['EFi_PM2_5'] / 1000) * df2['DF_PM'] * (1 - df2['R_PM'] / 100)
check_E_HOT_col = ['E_HOT_CO', 'E_HOT_HC', 'E_HOT_NOx', 'E_HOT_PM10', 'E_HOT_PM2_5']

# 자동차-엔진 미가열(Cold-start) 배출
## 배출량 식
# $$E_{COLD :i, j} = \beta_{i, j} \times N_j \times M_j \times e^{HOT}_j \times (e^{COLD} / e^{HOT} \vert_{i, j} - 1)$$

# - $E_{COLD :i, j}$ : 차종 $j$에서 배출되는 오염물질 $i$의 엔진미가열 배출량
# - $\beta_{i, j}$ : 차종 $j$의 엔진미가열 상태의 주행거리 분율
# - $N_j$ : 차종 $j$의 수
# - $M_j$ : 차종 $j$의 주행거리
# - $e^{HOT}_j$ : 차종 $j$의 엔진가열 상태에서의 배출계수
# - $e^{COLD} / e^{HOT} \vert_{i, j}$ : 차종 $j$의 엔진가열상태 대비 엔진미가열 상태에서의 배출 비율

## 베타($\beta$)
# - 1회 평균주행거리(1 trip length)와 대기온도, 자동차 이용 패턴을 고려
#     - 1회 평균주행거리 정보는 모든 차종에 대하여 각각 적용하는 방안은 수집자료의 한계로 국내 연구결과를 바탕으로 수도권 지역 승용차의 1회 평균 주행거리 12.35km를 적용
#     - 이때 과대산정 가능성을 염두하여, 도시지역의 택시를 제외한 승용차(경형, 소형, 중형, 대형), 승합차(경형, 소형), RV(소형, 중형)에 적용

#     - 1회 주행거리 : 12.35 km (2002년 기준)
#     - 미가열 배출 적용 차종 : 승용차(경형, 소형, 중형, 대형) / 승합차(경형, 소형) / RV(소형, 중형)
#     - 미가열 배출 적용 지역 : 도시지역(고속도로 구간 제외)

# - 산정식

# -|Factor $\beta$의 산정식
# -|-
# $Estimated$ $l_{trip}$|$0.647-0.025 \times l_{trip} - (0.00974 - 0.000385 \times l_{trip}) \times T_a$
# $Measured$ $l_{trip}$|$0.698-0.051 \times l_{trip} - (0.01051 - 0.000770 \times l_{trip}) \times T_a$

# - $T_a$ : 대기온도
# - $Estimated$ $l_{trip}$ : 유럽평균(1985) 약 11.8km / 유럽평균(1998) 약 12.4km(Andre et al., 1998)
l_trip = 12.35 # 국립환경과학원(2007), 도로 이동오염원 대기오염 배출량 산정방법 개선 및 장래 배출량 예측방법 연구
Ta = 12.4 # e-나라지표 (https://www.index.go.kr/unity/potal/main/EachDtlPageDetail.do?idx_cd=1400) 2002년 년 평균 기온
df2['Beta(Estimated)'] = 0.647 - 0.025 * l_trip - (0.00974 - 0.000385 * l_trip) * Ta
# df2['Beta(Measured)'] = 0.698 - 0.051 * l_trip - (0.01051 - 0.000770 * l_trip) * Ta

## $e^{COLD} / e^{HOT}$
# 휘발유(자동 제어)
df2.loc[(df2['연료'] == '휘발유') & (df2['차량연식'] < 1991) & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ), 'eCOLD/eHOT_CO'] = 3.7 - 0.09 * Ta
df2.loc[(df2['연료'] == '휘발유') & (df2['차량연식'] >= 1991) & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ), 'eCOLD/eHOT_CO'] = 9.04 - 0.09 * Ta
df2.loc[(df2['연료'] == '휘발유') & (df2['차량연식'] < 1991) & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ), 'eCOLD/eHOT_NOx'] = 1.14 - 0.006 * Ta
df2.loc[(df2['연료'] == '휘발유') & (df2['차량연식'] >= 1991) & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ), 'eCOLD/eHOT_NOx'] = 3.66 - 0.006 * Ta
df2.loc[(df2['연료'] == '휘발유') & (df2['차량연식'] < 1991) & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ), 'eCOLD/eHOT_HC'] = 2.8 - 0.06 * Ta
df2.loc[(df2['연료'] == '휘발유') & (df2['차량연식'] >= 1991) & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ), 'eCOLD/eHOT_HC'] = 12.59 - 0.06 * Ta
df2.loc[df2['연료'] == '휘발유', 'eCOLD/eHOT_PM'] = 1

# 경유
df2.loc[(df2['연료'] == '경유') & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ),'eCOLD/eHOT_CO'] = 1.9 - 0.03 * Ta
df2.loc[(df2['연료'] == '경유') & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ),'eCOLD/eHOT_NOx'] = 1.3 - 0.013 * Ta
df2.loc[(df2['연료'] == '경유') & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ),'eCOLD/eHOT_HC'] = 3.1 - 0.09 * Ta
df2.loc[(df2['연료'] == '경유') & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ),'eCOLD/eHOT_PM'] = 3.1 - 0.1 * Ta

# LPG(액화석유가스)
df2.loc[(df2['연료'] == 'LPG(액화석유가스)') & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ),'eCOLD/eHOT_CO'] = 3.66 - 0.09 * Ta
df2.loc[(df2['연료'] == 'LPG(액화석유가스)') & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ),'eCOLD/eHOT_NOx'] = 0.98 - 0.006 * Ta
df2.loc[(df2['연료'] == 'LPG(액화석유가스)') & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ),'eCOLD/eHOT_HC'] = 2.24 - 0.06 * Ta
df2.loc[(df2['연료'] == 'LPG(액화석유가스)') & ( (df2['차종'] == '승용') | ( (df2['차종'] == '승합') & ((df2['차종유형'] == '경형') | (df2['차종유형'] == '소형')) ) ),'eCOLD/eHOT_PM'] = 1
check_ecoldhot_col = ['eCOLD/eHOT_CO', 'eCOLD/eHOT_NOx', 'eCOLD/eHOT_HC', 'eCOLD/eHOT_PM']
df2[check_ecoldhot_col] = df2[check_ecoldhot_col].fillna(1)

## 배출량 계산
### 🔺 계산식 고민
# $$E_{COLD :i, j} = \beta_{i, j} \times N_j \times M_j \times e^{HOT}_j \times (e^{COLD} / e^{HOT} \vert_{i, j} - 1)$$
# - $M_j$ : 차종 $j$의 주행거리
#     - 1년 주행거리로 계산? 
#         - VKT
#     - 1회 주행거리로 계산?
#         - l_trip

#         E      =             B          *     M      *    e(HOT)     * ( e(COLD)/e(HOT)      - 1 )
df2['E_COLD_CO'] = df2['Beta(Estimated)'] * df2['VKT'] * df2['EFi_CO'] * (df2['eCOLD/eHOT_CO'] - 1)
df2['E_COLD_NOx'] = df2['Beta(Estimated)'] * df2['VKT'] * df2['EFi_NOx'] * (df2['eCOLD/eHOT_NOx'] - 1)
df2['E_COLD_HC'] = df2['Beta(Estimated)'] * df2['VKT'] * df2['EFi_HC'] * (df2['eCOLD/eHOT_HC'] - 1)
df2['E_COLD_PM10'] = df2['Beta(Estimated)'] * df2['VKT'] * df2['EFi_PM10'] * (df2['eCOLD/eHOT_PM'] - 1)
df2['E_COLD_PM2_5'] = df2['Beta(Estimated)'] * df2['VKT'] * df2['EFi_PM2_5'] * (df2['eCOLD/eHOT_PM'] - 1)
check_E_cold_col = ['E_COLD_CO', 'E_COLD_NOx', 'E_COLD_HC', 'E_COLD_PM10', 'E_COLD_PM2_5']

# 자동차-휘발유 증발 배출
## 배출량 식
# - 3가지
#     - 주간증발손실(Diurnal loss)
#     - 고온증발손실(Hot and warm soak)
#     - 주행손실(Running loss)
# - 현재('19 배출량 기준) 휘발유 증발 배출원
#     - 주간증발손실, 주행손실 고려
#     - 우리나라 휘발유 차량 대부분이 방지설비로 카본 캐니스터를 장착하고 있으므로 $e^{S,HOT}$, $e^{S,WARM}$, 고온증발손실(hot and warm soak) 배출은 없는 것으로 가정

# $$E_{EVA, VOC:j} = 365 \times N_j \times (e^d + S^c + S^{fi}) + R$$
# $S^c = (1-q)(p \times x \times e^{S, HOT} + w \times x \times e^{S,WARM})$

# $S^{fi} = q \times e^{fi} \times x$
# $$R = m_j(p \times e^{R,HOT} + w \times e^{R,WARM})$$

# - $E_{EVA, VOC:j}$ : 차종 j의 증발손실에 의한 VOC 배출량(g/yr)
# - $N_j$ : 차종 j의 휘발유 사용 차량 등록대수(대)
# - $e^d$ : 금속탱크를 가지고 있는 휘발유 차량의 일중 VOC 배출량(g/day-대)
# - $S^c$ : Carburetor 휘발유 차량의 일중 hot and warm soak 배출량(g/day-대)
# - $S^{fi}$ : Fuel injection 휘발유 차량의 일중 hot and warm soak 배출량(g/day-대)
# - $R$ : Running loss에 의한 휘발유 차량의 연중 배출량(g/yr)
# - $q$ : Fuel injection 휘발유 차량의 비율
# - $p$ : 가열 엔진 상태로 끝나는 trip의 비율
# - $x$ : 일평균 통행횟수 $= V_j / 365 \times l_{trip}$
# - $w$ : 미가열 엔진 상태료 끝나는 trip의 비율
# - $e^{S,HOT}$ : Hot soak emission의 평균 배출계수
# - $e^{S,WARM}$ : Cold and warm soak emission의 평균 배출계수
# - $e^{fi}$ : Fuel injection 휘발유 차량의 평균 hot and warm soak 배출계수
# - $e^{R,HOT}$ : 휘발유 차량의 평균 hot running loss 배출계수
# - $e^{R,WARM}$ : 휘발유 차량의 평균 warm running loss 배출계수
# - $m_j$ : 차종 j의 연간 총 주행거리
# - $V_j$ : 차종 j의 1대당 평균 연간 주행거리

## 배출계수
### 🔺 수정 중
### $e^d$ Diurnal(g/day)
# - $e^d$ : 금속탱크를 가지고 있는 휘발유 차량의 일중 VOC 배출량(g/day-대)
#### 🔺 RVP(증기압)
# - 참고 COPERT 3(p.80) Table 6.10: Gasoline fuel specifications
#     - summer : 60
#     - winter : 70
#     - 평균 : 65
# - 수정 사항(2023.04.20, 최)
#     - 국내 과학기술원, 환경부 자료 찾아서 사용 추천
RVP = 65
#### ❗ t_a(기온)
# - t_a, t_min, t_rise
#     - 참고 COPERT 3(p.72) Table 5.32: Summary of emission factors for estimating evaporative emissions of gasoline vehicles(all RVp in kPa, all temperatures in °C)
#         - t_a = (t_max + t_min) / 2
#         - t_rise = t_max - t_min
#     - 기상자료개방포털(2002년) 연평균 기온
#         - https://data.kma.go.kr/stcs/grnd/grndTaList.do?pgmNo=70
#         - 7.5°C(최저), 12.4°C(평균), 17.9°C(최고)
t_min = 7.5 # 평균 최저 온도
t_max = 17.9
t_a = (t_min + t_max) / 2
t_rise = 12.4 # 평균 상승 온도

df2.loc[df2['차량연식'] < 1991, 'e_d'] = 9.1 * np.exp( 0.0158 * (RVP - 61.2) + 0.0574 * (t_min - 22.5) + 0.0614 * (t_rise - 11.7) )
df2.loc[df2['차량연식'] >= 1991, 'e_d'] = 0.2 * (9.1 * np.exp( 0.0158 * (RVP - 61.2) + 0.0574 * (t_min - 22.5) + 0.0614 * (t_rise - 11.7) ))

### $e^{R,HOT}$ Hot running loss(g/km)
# - $e^{R,HOT}$ : 휘발유 차량의 평균 hot running loss 배출계수
df2.loc[df2['차량연식'] < 1991, 'e_RHOT'] = 0.136 * np.exp( - 5.967 + 0.04259 * RVP + 0.1773 * t_a)
df2.loc[df2['차량연식'] >= 1991, 'e_RHOT'] = 0.1 * (0.136 * np.exp( - 5.967 + 0.04259 * RVP + 0.1773 * t_a))

### $e^{S,HOT}$ Hot soak(g/procedure)
# - $e^{S,HOT}$ : Hot soak emission의 평균 배출계수
df2.loc[df2['차량연식'] < 1991, 'e_SHOT'] = 3.0042 * np.exp(0.02 * RVP)
df2.loc[df2['차량연식'] >= 1991, 'e_SHOT'] = 0.3 * np.exp(-2.14 + 0.02302 * RVP + 0.09408 * t_a)

### $e^{S,WARM}$ Waram soak(g/procedure)
# - $e^{S,WARM}$ : Cold and warm soak emission의 평균 배출계수
df2.loc[df2['차량연식'] < 1991, 'e_SWARM'] = np.exp(-1.644 + 0.01993 * RVP + 0.07521 * t_a)
df2.loc[df2['차량연식'] >= 1991, 'e_SWARM'] = 0.2 * np.exp(-2.41 + 0.02302 * RVP + 0.09408 * t_a)

### $e^{fi}$ Warm and hot soak for Fuel injected vehicle(g/procedure)
# - $e^{fi}$ : Fuel injection 휘발유 차량의 평균 hot and warm soak 배출계수
df2.loc[df2['차량연식'] < 1991, 'e_fi'] = 0.7
df2.loc[df2['차량연식'] >= 1991, 'e_fi'] = 0

### $e^{R,WARM}$ Warm running loss(g/km)
# - $e^{R,HOT}$ : 휘발유 차량의 평균 hot running loss 배출계수
df2.loc[df2['차량연식'] < 1991, 'e_RWARM'] = 0.1 * np.exp(-5.967 + 0.04259 * RVP + 0.1773 * t_a)
df2.loc[df2['차량연식'] >= 1991, 'e_RWARM'] = 0.1 * (0.1 * np.exp(-5.967 + 0.04259 * RVP + 0.1773 * t_a))

## $R$
# $R = m_j(p \times e^{R,HOT} + w \times e^{R,WARM})$
# - Running loss에 의한 휘발유 차량의 연중 배출량(g/yr)
# - $m_j$ : 차종 j의 연간 총 주행거리
# - $p$ : 가열 엔진 상태로 끝나는 trip의 비율
# - $e^{R,HOT}$ : 휘발유 차량의 평균 hot running loss 배출계수
# - $w$ : 미가열 엔진 상태로 끝나는 trip의 비율
# - $e^{R,WARM}$ : 휘발유 차량의 평균 warm running loss 배출계수
### ❓ 확인 중
# - 수정 사항(2023.04.20, 최)
    # - 찾는 중
    # - p:w = 0.2:0.8(2023.06.29, 최)
p = 0.2 # 가열 엔진 상태로 끝나는 trip의 비율
w = 0.8 # 미가열 엔진 상태로 끝나는 trip의 비율
df2['R'] = df2['VKT'] * (p * df2['e_RHOT'] + w * df2['e_RWARM'])

## $S^{fi}$
# $S^{fi} = q \times e^{fi} \times x$
# - Fuel injection 휘발유 차량의 일중 hot and warm soak 배출량(g/day-대)
# - $q$ : Fuel injection 휘발유 차량의 비율
# - $e^{fi}$ : Fuel injection 휘발유 차량의 평균 hot and warm soak 배출계수
# - $x$ : 일평균 통행횟수 $= V_j / 365 \times l_{trip}$
# - $V_j$ : 차종 j의 1대당 평균 연간 주행거리
not_injection = df2[(df2['연료'] == '휘발유') & (df2['차량연식'] < 1991)].shape[0]
injection = df2[(df2['연료'] == '휘발유') & (df2['차량연식'] >= 1991)].shape[0]

# Fuel injection 휘발유 차량의 비율
q = injection / (injection + not_injection)

# x = df2['VKT'] / (365 * l_trip)
df2['S_fi'] = q * df2['e_fi'] * ( df2['VKT'] / (365 * l_trip) )

## $S^c$
# $S^c = (1-q)(p \times x \times e^{S, HOT} + w \times x \times e^{S,WARM})$
# - Carburetor 휘발유 차량의 일중 hot and warm soak 배출량(g/day-대)
# - $q$ : Fuel injection 휘발유 차량의 비율
# - $p$ : 가열 엔진 상태로 끝나는 trip의 비율
# - $x$ : 일평균 통행횟수 $= V_j / 365 \times l_{trip}$
# - $e^{S,HOT}$ : Hot soak emission의 평균 배출계수
# - $w$ : 미가열 엔진 상태료 끝나는 trip의 비율
# - $x$ : 일평균 통행횟수 $= V_j / 365 \times l_{trip}$
# - $V_j$ : 차종 j의 1대당 평균 연간 주행거리
# - $e^{S,WARM}$ : Cold and warm soak emission의 평균 배출계수
# x = df2['VKT'] / (365 * l_trip)
df2['S_c'] = (1-q) * (p * (df2['VKT'] / (365 * l_trip)) * df2['e_SHOT'] + w * (df2['VKT'] / (365 * l_trip)) * df2['e_SWARM'])

## $E_{EVA,VOC}$
# $E_{EVA, VOC:j} = 365 \times N_j \times (e^d + S^c + S^{fi}) + R$
# - $N_j$ : 차종 j의 휘발유 사용 차량 등록대수(대)
# - $e^d$ : 금속탱크를 가지고 있는 휘발유 차량의 일중 VOC 배출량(g/day-대)
# - $S^c$ : Carburetor 휘발유 차량의 일중 hot and warm soak 배출량(g/day-대)
# - $S^{fi}$ : Fuel injection 휘발유 차량의 일중 hot and warm soak 배출량(g/day-대)
# - $R$ : Running loss에 의한 휘발유 차량의 연중 배출량(g/yr)
df2['E_EVA_VOC'] = 365 * (df2['e_d'] + df2['S_c'] + df2['S_fi']) + df2['R']

# 배출량 합계
# $E_{total}(kg) = E_{HOT}(kg) + E_{COLD}(g) + E_{EVAP}(g)$
# E_COLD_NOx 음수 -> 0으로 처리(2023.04.24 from 최이사님)
df2.loc[df2['E_COLD_NOx'] < 0, 'E_COLD_NOx'] = 0

# 단위 변환(g -> kg)
df2['E_COLD_CO'] = df2['E_COLD_CO'] / 1000
df2['E_COLD_HC'] = df2['E_COLD_HC'] / 1000
df2['E_COLD_NOx'] = df2['E_COLD_NOx'] / 1000
df2['E_COLD_PM10'] = df2['E_COLD_PM10'] / 1000
df2['E_COLD_PM2_5'] = df2['E_COLD_PM2_5'] / 1000
df2['E_EVA_VOC'] = df2['E_EVA_VOC'] / 1000

fuel = '휘발유'
df2.loc[df2['연료'] == fuel, 'E_CO_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_CO'] + df2.loc[df2['연료'] == fuel , 'E_COLD_CO']
df2.loc[df2['연료'] == fuel, 'E_HC_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_HC'] + df2.loc[df2['연료'] == fuel , 'E_COLD_HC'] + df2.loc[df2['연료'] == fuel , 'E_EVA_VOC']
df2.loc[df2['연료'] == fuel, 'E_NOx_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_NOx'] + df2.loc[df2['연료'] == fuel , 'E_COLD_NOx']
df2.loc[df2['연료'] == fuel, 'E_PM10_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_PM10'] + df2.loc[df2['연료'] == fuel , 'E_COLD_PM10']
df2.loc[df2['연료'] == fuel, 'E_PM2_5_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_PM2_5'] + df2.loc[df2['연료'] == fuel , 'E_COLD_PM2_5']

fuel = '경유'
df2.loc[df2['연료'] == fuel, 'E_CO_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_CO'] + df2.loc[df2['연료'] == fuel , 'E_COLD_CO']
df2.loc[df2['연료'] == fuel, 'E_HC_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_HC'] + df2.loc[df2['연료'] == fuel , 'E_COLD_HC']
df2.loc[df2['연료'] == fuel, 'E_NOx_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_NOx'] + df2.loc[df2['연료'] == fuel , 'E_COLD_NOx']
df2.loc[df2['연료'] == fuel, 'E_PM10_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_PM10'] + df2.loc[df2['연료'] == fuel , 'E_COLD_PM10']
df2.loc[df2['연료'] == fuel, 'E_PM2_5_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_PM2_5'] + df2.loc[df2['연료'] == fuel , 'E_COLD_PM2_5']

fuel = 'LPG(액화석유가스)'
df2.loc[df2['연료'] == fuel, 'E_CO_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_CO'] + df2.loc[df2['연료'] == fuel , 'E_COLD_CO']
df2.loc[df2['연료'] == fuel, 'E_HC_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_HC'] + df2.loc[df2['연료'] == fuel , 'E_COLD_HC']
df2.loc[df2['연료'] == fuel, 'E_NOx_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_NOx'] + df2.loc[df2['연료'] == fuel , 'E_COLD_NOx']
df2.loc[df2['연료'] == fuel, 'E_PM10_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_PM10'] + df2.loc[df2['연료'] == fuel , 'E_COLD_PM10']
df2.loc[df2['연료'] == fuel, 'E_PM2_5_total'] = df2.loc[df2['연료'] == fuel , 'E_HOT_PM2_5'] + df2.loc[df2['연료'] == fuel , 'E_COLD_PM2_5']
check_E_col = ['E_CO_total', 'E_HC_total', 'E_NOx_total', 'E_PM10_total', 'E_PM2_5_total']

## 배출량
# - 연료 : 휘발유, 경유, LPG(액화석유가스)
# - 물질 : CO, HC, NOx, PM10, PM2.5

## 시도/시군구별 배출량 합계
grp1 = df2.groupby(['시도', '시군구_수정'], as_index=False).agg({'E_CO_total':'sum', 'E_HC_total':'sum', 'E_NOx_total':'sum', 'E_PM10_total':'sum', 'E_PM2_5_total':'sum'})

# 연도 설정
today_date = datetime.today().strftime("%Y%m%d")
# grp1['연도'] = today_date[:4]
grp1['연도'] = '2022' # 하드코딩
grp1 = grp1[['연도', '시도', '시군구_수정', 'E_CO_total', 'E_HC_total', 'E_NOx_total', 'E_PM10_total', 'E_PM2_5_total']]
grp1['테이블생성일자'] = today_date

# 기준연월 추가
grp1['기준연월'] = '2022.12'
# grp1['기준연월'] = today_date[:4] + '.' + today_date[4:6]
chc_dict = {
    '테이블생성일자':'LOAD_DT', 
    '기준연월':'CRTR_YM', 
    '연도':'YR', 
    '시도':'CTPV', 
    '시군구_수정':'SGG', 
    'E_CO_total':'CO_EXHST_MSS', 
    'E_HC_total':'HC_EXHST_MSS', 
    'E_NOx_total':'NOx_EXHST_MSS',
    'E_PM10_total':'PM10_EXHST_MSS', 
    'E_PM2_5_total':'PM2_5_EXHST_MSS', 
}
STD_BD_GRD4_EXHST_GAS_MSS_TOT = grp1.rename(columns=chc_dict)

# STD_BD_GRD4_EXHST_GAS_MSS_TOT.columns

### [출력] STD_BD_GRD4_EXHST_GAS_MSS_TOT

# expdf = STD_BD_GRD4_EXHST_GAS_MSS_TOT
# table_nm = 'STD_BD_GRD4_EXHST_GAS_MSS_TOT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_EXHST_GAS_MSS_TOT')

## 출력(4등급)
STD_BD_GRD4_EXHST_GAS_MSS = df2[[
    '차대번호',
    '자동차등록번호',
    '제원관리번호',
    '차종', 
    '용도', 
    '최초등록일자',
    '차량연식',
    '제작일자',
    '검사유효일',
    '차명',
    '차종분류',
    '차종유형', 
    '자동차형식', 
    '제작사명',
    '연료', 
    '엔진형식',
    '총중량', 
    '적재중량', 
    '엔진출력', 
    '배기량', 
    '시도', 
    '시군구',
    '소유자구분',
    '차량말소YN',
    '배출가스인증번호',
    '배출가스등급',
    'DPF_YN',
    '법정동코드', 
    'E_CO_total', 
    'E_HC_total', 
    'E_NOx_total', 
    'E_PM10_total',
    'E_PM2_5_total', 
    '법정동코드_mod',
    ]]

# 날짜 설정
STD_BD_GRD4_EXHST_GAS_MSS['기준연월'] = '2022.12'
today_date = datetime.today().strftime("%Y%m%d")
# STD_BD_GRD4_EXHST_GAS_MSS['기준연월'] = today_date[:4] + '.' + today_date[4:6]

STD_BD_GRD4_EXHST_GAS_MSS['테이블생성일자'] = today_date
# RH법정동코드 문자열타입으로 변경
STD_BD_GRD4_EXHST_GAS_MSS['법정동코드_mod'] = STD_BD_GRD4_EXHST_GAS_MSS['법정동코드_mod'].astype('str')
# 기준연월 추가 고민
STD_BD_GRD4_EXHST_GAS_MSS = STD_BD_GRD4_EXHST_GAS_MSS[[
    '테이블생성일자', 
    '기준연월', 
    '차대번호',
    '자동차등록번호',
    '제원관리번호',
    '차종', 
    '용도', 
    '최초등록일자',
    '차량연식',
    '제작일자',
    '검사유효일',
    '차명',
    '차종분류',
    '차종유형', 
    '자동차형식', 
    '제작사명',
    '연료', 
    '엔진형식',
    '총중량', 
    '적재중량', 
    '엔진출력', 
    '배기량', 
    '법정동코드', 
    '시도', 
    '시군구',
    '소유자구분',
    '차량말소YN',
    '배출가스인증번호',
    '배출가스등급',
    'DPF_YN',
    'E_CO_total', 
    'E_HC_total', 
    'E_NOx_total', 
    'E_PM10_total',
    'E_PM2_5_total', 
    '법정동코드_mod',
    ]]
ch_col_dict = {
                '테이블생성일자':'LOAD_DT', 
                '기준연월':'CRTR_YM',
                '차대번호':'VIN', 
                '자동차등록번호':'VHRNO', 
                '제원관리번호':'MANG_MNG_NO', 
                '차종':'VHCTY_CD', 
                '용도':'PURPS_CD2', 
                '최초등록일자':'FRST_REG_YMD', 
                '차량연식':'YRIDNW', 
                '제작일자':'FBCTN_YMD', 
                '검사유효일':'EXHST_GAS_INSP_VLD_YMD',
                '차명':'VHCNM', 
                '차종분류':'VHCTY_CL_CD', 
                '차종유형':'VHCTY_TY', 
                '자동차형식':'CAR_FRM', 
                '제작사명':'MNFCTR_NM', 
                '연료':'FUEL_CD', 
                '엔진형식':'EGIN_TY',
                '총중량':'TOTL_WGHT', 
                '적재중량':'CRYNG_WGHT', 
                '엔진출력':'EGIN_OTPT',
                '배기량':'DSPLVL', 
                '법정동코드':'STDG_CD', 
                '시도':'CTPV_NM', 
                '시군구':'SGG_NM', 
                '소유자구분':'OWNR_SE', 
                '차량말소YN':'ERSR_YN',
                '배출가스인증번호':'EXHST_GAS_CERT_NO', 
                '배출가스등급':'EXHST_GAS_GRD_CD', 
                'DPF유무_수정':'DPF_MNTNG_YN', 
                'Grade':'GRD4_MLSFC',
                'E_CO_total':'CO_EXHST_MSS',
                'E_HC_total':'HC_EXHST_MSS',
                'E_NOx_total':'NOx_EXHST_MSS', 
                'E_PM10_total':'PM10_EXHST_MSS',
                'E_PM2_5_total':'PM2_5_EXHST_MSS',
                '법정동코드_mod':'STDG_CD_MOD',
                }
                
STD_BD_GRD4_EXHST_GAS_MSS = STD_BD_GRD4_EXHST_GAS_MSS.rename(columns=ch_col_dict)

# STD_BD_GRD4_EXHST_GAS_MSS.columns

### [출력] STD_BD_GRD4_EXHST_GAS_MSS

# expdf = STD_BD_GRD4_EXHST_GAS_MSS
# table_nm = 'STD_BD_GRD4_EXHST_GAS_MSS'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 13s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_EXHST_GAS_MSS')

## 배출량 현황
grp2 = df2.groupby(['시도', '시군구_수정', '연료', '차종', '차종유형', '용도']).agg({'E_CO_total':'sum', 'E_HC_total':'sum', 'E_NOx_total':'sum', 'E_PM10_total':'sum', 'E_PM2_5_total':'sum'}).reset_index()
grp2 = grp2.rename(columns={'E_CO_total':'E_CO_total_sum', 'E_HC_total':'E_HC_total_sum', 'E_NOx_total':'E_NOx_total_sum', 'E_PM10_total':'E_PM10_total_sum', 'E_PM2_5_total':'E_PM2_5_total_sum'})

# 연도 설정
grp2['연도'] = '2022'
today_date = datetime.today().strftime("%Y%m%d")
# grp2['연도'] = today_date[:4]
grp2['테이블생성일자'] = today_date

STD_BD_DAT_GRD4_EXHST_MSS_CURSTT = grp2[[
    '연도',
    '시도',
    '시군구_수정',
    '연료',
    '차종',
    '차종유형',
    '용도',
    'E_CO_total_sum',
    'E_HC_total_sum',
    'E_NOx_total_sum',
    'E_PM10_total_sum',
    'E_PM2_5_total_sum',
    '테이블생성일자',
]]
cdict = {
    '연도':'YR',
    '시도':'CTPV',
    '시군구_수정':'SGG',
    '연료':'FUEL_CD',
    '차종':'VHCTY_CD',
    '차종유형':'VHCTY_TY',
    '용도':'PURPS_CD2',
    'E_CO_total_sum':'CO_EXHST_MSS_SM',
    'E_HC_total_sum':'HC_EXHST_MSS_SM',
    'E_NOx_total_sum':'NOx_EXHST_MSS_SM',
    'E_PM10_total_sum':'PM10_EXHST_MSS_SM',
    'E_PM2_5_total_sum':'PM2_5_EXHST_MSS_SM',
    '테이블생성일자':'LOAD_DT',
}
STD_BD_DAT_GRD4_EXHST_MSS_CURSTT = STD_BD_DAT_GRD4_EXHST_MSS_CURSTT.rename(columns=cdict)

# STD_BD_DAT_GRD4_EXHST_MSS_CURSTT.columns

### [출력] STD_BD_DAT_GRD4_EXHST_MSS_CURSTT

# expdf = STD_BD_DAT_GRD4_EXHST_MSS_CURSTT
# table_nm = 'STD_BD_DAT_GRD4_EXHST_MSS_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 30.7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_GRD4_EXHST_MSS_CURSTT')

## 측정치 현황
grp3 = df2.groupby(['시도', '시군구_수정', '연료', '차종', '차종유형', '용도']).agg({'E_CO_total':'mean', 'E_HC_total':'mean', 'E_NOx_total':'mean', 'E_PM10_total':'mean', 'E_PM2_5_total':'mean'}).reset_index()
grp3 = grp3.rename(columns={'E_CO_total':'E_CO_total_mean', 'E_HC_total':'E_HC_total_mean', 'E_NOx_total':'E_NOx_total_mean', 'E_PM10_total':'E_PM10_total_mean', 'E_PM2_5_total':'E_PM2_5_total_mean'})

# 연도 설정
grp3['연도'] = '2022'
# grp3['연도'] = today_date[:4]
grp3['테이블생성일자'] = today_date

STD_BD_DAT_GRD4_MEVLU = grp3[[
    '연도',
    '시도',
    '시군구_수정',
    '연료',
    '차종',
    '차종유형',
    '용도',
    'E_CO_total_mean',
    'E_HC_total_mean',
    'E_NOx_total_mean',
    'E_PM10_total_mean',
    'E_PM2_5_total_mean',
    '테이블생성일자',
]]
cdict = {
    '연도':'YR',
    '시도':'CTPV',
    '시군구_수정':'SGG',
    '연료':'FUEL_CD',
    '차종':'VHCTY_CD',
    '차종유형':'VHCTY_TY',
    '용도':'PURPS_CD2',
    'E_CO_total_mean':'CO_EXHST_MSS_AVRG',
    'E_HC_total_mean':'HC_EXHST_MSS_AVRG',
    'E_NOx_total_mean':'NOx_EXHST_MSS_AVRG',
    'E_PM10_total_mean':'PM10_EXHST_MSS_AVRG',
    'E_PM2_5_total_mean':'PM2_5_EXHST_MSS_AVRG',
    '테이블생성일자':'LOAD_DT',
}
STD_BD_DAT_GRD4_MEVLU = STD_BD_DAT_GRD4_MEVLU.rename(columns=cdict)

# STD_BD_DAT_GRD4_MEVLU.columns

### [출력] STD_BD_DAT_GRD4_MEVLU

# expdf = STD_BD_DAT_GRD4_MEVLU
# table_nm = 'STD_BD_DAT_GRD4_MEVLU'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 30.7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_GRD4_MEVLU')

## 1-3 code end ##################################################################

## 3-1 start

## 전체 등록정보(STD_CEG_CAR_MIG)
# exasol db
# 4m 21s
car = wd.export_to_pandas("SELECT VIN, BSPL_STDG_CD, EXHST_GAS_GRD_CD, EXHST_GAS_CERT_NO, VHCL_ERSR_YN, MANP_MNG_NO, YRIDNW, VHCTY_CD, PURPS_CD2, FRST_REG_YMD, VHCL_MNG_NO FROM STD_CEG_CAR_MIG;")
car_ch_col = {
    'VIN':'차대번호', 
    'BSPL_STDG_CD':'법정동코드', 
    'EXHST_GAS_GRD_CD':'배출가스등급', 
    'EXHST_GAS_CERT_NO':'배출가스인증번호',
    'VHCL_ERSR_YN':'차량말소YN',
    'MANP_MNG_NO':'제원관리번호', 
    'YRIDNW':'차량연식', 
    'VHCTY_CD':'차종', 
    'PURPS_CD2':'용도', 
    'FRST_REG_YMD':'최초등록일자',
    'VHCL_MNG_NO':'차량관리번호'
}
carr = car.rename(columns=car_ch_col)

## 중복 차대번호 제거
carr['최초등록일자'] = pd.to_numeric(carr['최초등록일자'], errors='coerce')
carr = carr.sort_values('최초등록일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

## 배출가스등급 코드 변환
grd_dict = {
    'A0501':'1', 
    'A0502':'2', 
    'A0503':'3', 
    'A0504':'4', 
    'A0505':'5', 
    'A05T2':'2',
    'A05T3':'3', 
    'A05T4':'4', 
    'A05T5':'5', 
    'A05X':'X', 
}
carr['배출가스등급'] = carr['배출가스등급'].replace(grd_dict)

## 차종 코드 변환
cd_dict = {
    'A31M':'이륜', 
    'A31P':'승용', 
    'A31S':'특수', 
    'A31T':'화물', 
    'A31V':'승합'
}
carr['차종'] = carr['차종'].replace(cd_dict)

## 용도 코드 변환
purps_dict = {
    'A08P':'개인용', 
    'A08B':'영업용', 
    'A08O':'관용',
}
carr['용도'] = carr['용도'].replace(purps_dict)

## 등록정보 말소 제거
carm = carr[carr['차량말소YN'] == 'N'].reset_index(drop=True)

## 등록&제원 병합
# 19.4s
cs = carm.merge(srcr, on='제원관리번호', how='left')

# cs.shape

cs['법정동코드'] = cs['법정동코드'].astype('str')
cs['법정동코드'] = cs['법정동코드'].str[:5] + '00000'
cs['법정동코드'] = pd.to_numeric(cs['법정동코드'])

## 등록&제원&법정동코드 병합(df)
df = cs.merge(coder, on='법정동코드', how='left')

# df['시도'].isnull().sum()

# ### 매칭 안되는 지역 처리
# # 주소 수정
# df.loc[df['법정동코드'] == 5172035031, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 강원특별자치도 홍천군
# df.loc[df['법정동코드'] == 5180031023, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# df.loc[df['법정동코드'] == 5180031031, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# df.loc[df['법정동코드'] == 5172035030, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# df.loc[df['법정동코드'] == 5180031028, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 

# df.loc[df['법정동코드'] == 5172035021, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# df.loc[df['법정동코드'] == 5180031025, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# df.loc[df['법정동코드'] == 4165052000, ['시도', '시군구']] = ['경기도', '포천시'] # 경기도 포천시 선단동
# df.loc[df['법정동코드'] == 5172035023, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# df.loc[df['법정동코드'] == 5180031027, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 

# df.loc[df['법정동코드'] == 5172035024, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# df.loc[df['법정동코드'] == 5175037022, ['시도', '시군구']] = ['강원특별자치도', '영월군'] # 
# df.loc[df['법정동코드'] == 4163055000, ['시도', '시군구']] = ['경기도', '양주시'] # 경기도 양주시 회천3동
# df.loc[df['법정동코드'] == 5180031033, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 경기도 양주시 회천3동

# rdf = df.copy()
# rdf['법정동코드'] = rdf['법정동코드'].astype('str')
# rdf.loc[rdf['시도'].isnull() == True, '법정동코드'] = rdf.loc[rdf['시도'].isnull() == True, '법정동코드'].str[:5] + '00000'
# rdf['법정동코드'] = pd.to_numeric(rdf['법정동코드'])

# rdfy = rdf[rdf['시도'].isnull() == False]
# rdfn = rdf[rdf['시도'].isnull() == True]

# rdfn = rdfn.drop(['시도', '시군구'], axis=1)
# rdfnm = rdfn.merge(coder, on='법정동코드', how='left')

# df = pd.concat([rdfy, rdfnm], ignore_index=False)

## 등록(말소 유지) & 제원 병합
# 10.3s
cse = carr.merge(srcr, on='제원관리번호', how='left')

## 등록&제원&이력 병합
# 2m 6.0s
ersr = cse.merge(hisr, on='차량관리번호', how='left')

# 1. 등록 차량말소와 등록이력 차량 말소 둘 모두 해당되는 데이터 추출
ersr = ersr.loc[(ersr['차량말소YN_x'] == 'Y') & (ersr['차량말소YN_y'] == 'Y')].reset_index(drop=True)

# 2. 변경일자 기준 최신 데이터만 남기고 차대번호 중복 제거
ersr = ersr.sort_values('변경일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

# 3. 변경일자 2019.01.01 이후만 추출
ersr = ersr[ersr['변경일자'] >= 20190101].reset_index(drop=True)

ersr['법정동코드'] = ersr['법정동코드'].astype('str')
ersr['법정동코드'] = ersr['법정동코드'].str[:5] + '00000'
ersr['법정동코드'] = pd.to_numeric(ersr['법정동코드'])

## 등록&제원&이력&법정동코드 병합(errc)
# 매칭 후 '시도' 빈값 없음
errc = ersr.merge(coder, on='법정동코드', how='left')

# errc['시도'].isnull().sum()

# 분석
## 연료 하이브리드 수정
# about 23.6s
df.loc[(df['연료'] == '휘발유') | (df['연료'] == '휘발유 하이브리드'), 'fuel'] = '휘발유'
df.loc[(df['연료'] == '경유') | (df['연료'] == '경유 하이브리드'), 'fuel'] = '경유'
df.loc[(df['연료'] == 'LPG(액화석유가스)') | (df['연료'] == 'LPG 하이브리드'), 'fuel'] = 'LPG'
df.loc[(df['연료'] == 'CNG(압축천연가스)') | (df['연료'] == 'CNG 하이브리드'), 'fuel'] = 'CNG'
df.loc[(df['연료'] == 'LNG(액화천연가스)') | (df['연료'] == 'LNG 하이브리드'), 'fuel'] = 'LNG'
df.loc[df['연료'] == '전기', 'fuel'] = '전기'
df.loc[df['연료'] == '수소', 'fuel'] = '수소'
df.loc[df['연료'] == '태양열', 'fuel'] = '태양열'
df.loc[df['연료'] == '알코올', 'fuel'] = '알코올'
df.loc[df['연료'] == '등유', 'fuel'] = '등유'
df.loc[df['연료'] == '기타연료', 'fuel'] = '기타연료'
df.loc[df['연료'].isnull() == True, 'fuel'] = np.nan

# about 23.6s
df.loc[(df['연료'] == '휘발유') | (df['연료'] == '휘발유 하이브리드'), 'fuel2'] = '휘발유'
df.loc[(df['연료'] == '경유') | (df['연료'] == '경유 하이브리드'), 'fuel2'] = '경유'
df.loc[(df['연료'] == 'LPG(액화석유가스)') | (df['연료'] == 'LPG 하이브리드'), 'fuel2'] = 'LPG(액화석유가스)'
df.loc[(df['연료'] == 'CNG(압축천연가스)') | (df['연료'] == 'CNG 하이브리드'), 'fuel2'] = '기타'
df.loc[(df['연료'] == 'LNG(액화천연가스)') | (df['연료'] == 'LNG 하이브리드'), 'fuel2'] = '기타'
df.loc[(df['연료'] == '전기') | (df['연료'] == '수소'), 'fuel2'] = '전기수소'
df.loc[df['연료'] == '태양열', 'fuel2'] = '기타'
df.loc[df['연료'] == '알코올', 'fuel2'] = '기타'
df.loc[df['연료'] == '등유', 'fuel2'] = '기타'
df.loc[df['연료'] == '기타연료', 'fuel2'] = '기타'
df.loc[df['연료'].isnull() == True, 'fuel2'] = '기타'

# about 3.3s
errc.loc[(errc['연료'] == '휘발유') | (errc['연료'] == '휘발유 하이브리드'), 'fuel'] = '휘발유'
errc.loc[(errc['연료'] == '경유') | (errc['연료'] == '경유 하이브리드'), 'fuel'] = '경유'
errc.loc[(errc['연료'] == 'LPG(액화석유가스)') | (errc['연료'] == 'LPG 하이브리드'), 'fuel'] = 'LPG'
errc.loc[(errc['연료'] == 'CNG(압축천연가스)') | (errc['연료'] == 'CNG 하이브리드'), 'fuel'] = 'CNG'
errc.loc[(errc['연료'] == 'LNG(액화천연가스)') | (errc['연료'] == 'LNG 하이브리드'), 'fuel'] = 'LNG'
errc.loc[errc['연료'] == '전기', 'fuel'] = '전기'
errc.loc[errc['연료'] == '수소', 'fuel'] = '수소'
errc.loc[errc['연료'] == '태양열', 'fuel'] = '태양열'
errc.loc[errc['연료'] == '알코올', 'fuel'] = '알코올'
errc.loc[errc['연료'] == '등유', 'fuel'] = '등유'
errc.loc[errc['연료'] == '기타연료', 'fuel'] = '기타연료'
errc.loc[errc['연료'].isnull() == True, 'fuel'] = np.nan

# about 23.6s
errc.loc[(errc['연료'] == '휘발유') | (errc['연료'] == '휘발유 하이브리드'), 'fuel2'] = '휘발유'
errc.loc[(errc['연료'] == '경유') | (errc['연료'] == '경유 하이브리드'), 'fuel2'] = '경유'
errc.loc[(errc['연료'] == 'LPG(액화석유가스)') | (errc['연료'] == 'LPG 하이브리드'), 'fuel2'] = 'LPG(액화석유가스)'
errc.loc[(errc['연료'] == 'CNG(압축천연가스)') | (errc['연료'] == 'CNG 하이브리드'), 'fuel2'] = '기타'
errc.loc[(errc['연료'] == 'LNG(액화천연가스)') | (errc['연료'] == 'LNG 하이브리드'), 'fuel2'] = '기타'
errc.loc[(errc['연료'] == '전기') | (errc['연료'] == '수소'), 'fuel2'] = '전기수소'
errc.loc[errc['연료'] == '태양열', 'fuel2'] = '기타'
errc.loc[errc['연료'] == '알코올', 'fuel2'] = '기타'
errc.loc[errc['연료'] == '등유', 'fuel2'] = '기타'
errc.loc[errc['연료'] == '기타연료', 'fuel2'] = '기타'
errc.loc[errc['연료'].isnull() == True, 'fuel2'] = '기타'

# 6s
# 수도권 : 서울특별시, 인천광역시, 경기도
# 비수도권 : 수도권 외
df.loc[(df['시도'] == '서울특별시') | (df['시도'] == '인천광역시') | (df['시도'] == '경기도'), '지역'] = '수도권'
df['지역'] = df['지역'].fillna('비수도권')

dfm = df.copy()
dfm['최초등록일자'] = dfm['최초등록일자'].astype('str')
dfm['최초등록일자_년'] = dfm['최초등록일자'].str[:4]
dfm['최초등록일자_월'] = dfm['최초등록일자'].str[4:6]
dfm['최초등록일자_일'] = dfm['최초등록일자'].str[6:8]

# 시군구명 앞쪽 지역명만 남기기(dfm)
dfm['시군구_수정'] = dfm['시군구'].str.split(' ').str[0]

# 수도권 : 서울특별시, 인천광역시, 경기도
# 비수도권 : 수도권 외
errc.loc[(errc['시도'] == '서울특별시') | (errc['시도'] == '인천광역시') | (errc['시도'] == '경기도'), '지역'] = '수도권'
errc['지역'] = errc['지역'].fillna('비수도권')
errc['변경일자'] = errc['변경일자'].astype('str')
errc['변경일자_년'] = errc['변경일자'].str[:4]
errc['변경일자_월'] = errc['변경일자'].str[4:6]
errc['변경일자_일'] = errc['변경일자'].str[6:8]
errc['변경일자'] = errc['변경일자_년'] + errc['변경일자_월'] + errc['변경일자_일']

# 시군구명 앞쪽 지역명만 남기기(errc)
errc['시군구_수정'] = errc['시군구'].str.split(' ').str[0]

# 2m 41.5s 
# 경유, 휘발유, LPG, 전기, 수소만 추출
dfm2 = dfm.loc[(dfm['fuel'] == '경유') | (dfm['fuel'] == '휘발유') | (dfm['fuel'] == 'LPG') | (dfm['fuel'] == '전기') | (dfm['fuel'] == '수소')].reset_index(drop=True)
errc2 = errc.loc[(errc['fuel'] == '경유') | (errc['fuel'] == '휘발유') | (errc['fuel'] == 'LPG') | (errc['fuel'] == '전기') | (errc['fuel'] == '수소')].reset_index(drop=True)

## 등급, 지역별 차량현황
# 연도 설정
today_date = datetime.today().strftime("%Y%m%d")
year = 2022
# year = int(today_date[:4])

# 2022년 차량 대수
grp1 = dfm.groupby(['지역', '시도', '배출가스등급'], as_index=False)['차대번호'].count()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})
grp1['연도'] = f'{year}'
grp1 = grp1[['연도', '지역', '시도', '배출가스등급', '차량대수']]

# 차량 통계 기본 데이터셋
yr_list = []
rgn_list = []
ctpv_list = []
grd_list = []
for ctpv in grp1['시도'].unique():
    if ('서울특별시' in ctpv) or ('인천광역시' in ctpv) or ('경기도' in ctpv):
        rgn = '수도권'
    else:
        rgn = '비수도권'
    for grd in ['1', '2', '3', '4', '5', 'X']:
        for yr in range(2019, year + 1):
            yr_list.append(str(yr))
            rgn_list.append(rgn)
            ctpv_list.append(ctpv)
            grd_list.append(grd)
base = pd.DataFrame({'연도':yr_list, '지역':rgn_list, '시도':ctpv_list, '배출가스등급':grd_list})

# 연도별 등록대수
grp2 = dfm.groupby(['최초등록일자_년', '지역', '시도', '배출가스등급'], as_index=False)['차대번호'].count()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '차대번호':'등록대수'})

# 연도별 말소대수
grp3 = errc.groupby(['변경일자_년', '지역', '시도', '배출가스등급'], as_index=False)['차대번호'].count()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '차대번호':'말소대수'})
base1 = base.merge(grp1, on=['연도', '지역', '시도', '배출가스등급'], how='left')
base2 = base1.merge(grp2, on=['연도', '지역', '시도', '배출가스등급'], how='left')
base3 = base2.merge(grp3, on=['연도', '지역', '시도', '배출가스등급'], how='left')
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

n = len(base3['연도'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

df1 = base3[['연도', '지역', '시도', '배출가스등급', '차량대수']]
df1['기준연월'] = df1['연도'] + '12'

df1['배출가스등급'] = df1['배출가스등급'].map({'1':'1', '2':'2', '3':'3', '4':'4', '5':'5', 'X':'미분류'})

today_date = datetime.today().strftime("%Y%m%d")
df1['테이블생성일자'] = today_date

cdict = {
    '테이블생성일자':'LOAD_DT', 
    '기준연월':'CRTR_YM', 
    '연도':'YR', 
    '지역':'RGN', 
    '시도':'CTPV_NM', 
    '배출가스등급':'EXHST_GAS_GRD_CD', 
    '차량대수':'VHCL_MKCNT',
}
STD_BD_CAR_CURSTT_MOD = df1.rename(columns=cdict)

# STD_BD_CAR_CURSTT_MOD.columns

### [출력] STD_BD_CAR_CURSTT_MOD

# expdf = STD_BD_CAR_CURSTT_MOD
# table_nm = 'STD_BD_CAR_CURSTT_MOD'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_CAR_CURSTT_MOD')

## 등급, 연료별 차량현황
# 2022년 차량 대수
grp1 = dfm.groupby(['fuel2', '배출가스등급'], as_index=False)['차대번호'].count()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})
grp1['연도'] = f'{year}'
grp1 = grp1[['연도', 'fuel2', '배출가스등급', '차량대수']]

# 차량 통계 기본 데이터셋
yr_list = []
fuel_list = []
grd_list = []
for fuel in grp1['fuel2'].unique():
    for grd in ['1', '2', '3', '4', '5', 'X']:
        for yr in range(2019, year + 1):
            yr_list.append(str(yr))
            fuel_list.append(fuel)
            grd_list.append(grd)
base = pd.DataFrame({'연도':yr_list, 'fuel2':fuel_list, '배출가스등급':grd_list})

# 연도별 등록대수
grp2 = dfm.groupby(['최초등록일자_년', 'fuel2', '배출가스등급'], as_index=False)['차대번호'].count()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '차대번호':'등록대수'})

# 연도별 말소대수
grp3 = errc.groupby(['변경일자_년', 'fuel2', '배출가스등급'], as_index=False)['차대번호'].count()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '차대번호':'말소대수'})
base1 = base.merge(grp1, on=['연도', 'fuel2', '배출가스등급'], how='left')
base2 = base1.merge(grp2, on=['연도', 'fuel2', '배출가스등급'], how='left')
base3 = base2.merge(grp3, on=['연도', 'fuel2', '배출가스등급'], how='left')
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

n = len(base3['연도'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

df2 = base3[['연도', 'fuel2', '배출가스등급', '차량대수']]
df2 = df2.rename(columns={'fuel2':'연료'})

# df2['배출가스등급'] = df2['배출가스등급'].map({'1':'1', '2':'2', '3':'3', '4':'4', '5':'5', 'X':'미분류'})

df2['기준연월'] = df2['연도'] + '12'
today_date = datetime.today().strftime("%Y%m%d")
df2['테이블생성일자'] = today_date

cdict = {
    '테이블생성일자':'LOAD_DT', 
    '기준연월':'CRTR_YM', 
    '연도':'YR', 
    '연료':'FUEL_CD', 
    '배출가스등급':'EXHST_GAS_GRD_CD', 
    '차량대수':'VHCL_MKCNT',
}
STD_BD_CAR_CURSTT_MOD2 = df2.rename(columns=cdict)

# STD_BD_CAR_CURSTT_MOD2.columns

### [출력] STD_BD_CAR_CURSTT_MOD2

# expdf = STD_BD_CAR_CURSTT_MOD2
# table_nm = 'STD_BD_CAR_CURSTT_MOD2'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_CAR_CURSTT_MOD2')

## 전체 차량 현황(등급, 차종, 지역별 차량현황)
today_date = datetime.today().strftime("%Y%m%d")
dfm2['테이블생성일자'] = today_date
df3 = dfm2[[
    '테이블생성일자', 
    '법정동코드', 
    '차종',
    '용도', 
    '차대번호', 
    '제원관리번호', 
    '배출가스인증번호', 
    '배출가스등급', 
    '차량말소YN', 
    'fuel', 
    '시도', 
    '시군구_수정'
]]
cdict = {
    '테이블생성일자':'LOAD_DT',
    '법정동코드':'BSPL_STDG_CD', 
    '차종':'VHCTY_CD',
    '용도':'PURPS_CD2', 
    '차대번호':'VIN', 
    '제원관리번호':'MANG_MNG_NO', 
    '배출가스인증번호':'EXHST_GAS_CERT_NO', 
    '배출가스등급':'EXHST_GAS_GRD_CD', 
    '차량말소YN':'VHCL_ERSR_YN', 
    'fuel':'FUEL_CD', 
    '시도':'CTPV', 
    '시군구_수정':'SGG', 
}
STD_BD_CAR_CURSTT = df3.rename(columns=cdict)

# STD_BD_CAR_CURSTT.columns

### [출력] STD_BD_CAR_CURSTT

# expdf = STD_BD_CAR_CURSTT
# table_nm = 'STD_BD_CAR_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 3m 51.1s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_CAR_CURSTT')

## 지역, 연료, 연도별 차량 현황 분석
# 2022년 차량 대수
grp1 = dfm2.groupby(['fuel', '시도'], as_index=False)['차대번호'].count()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})
grp1['연도'] = f'{year}'
grp1 = grp1[['연도', 'fuel', '시도', '차량대수']]

# 차량 통계 기본 데이터셋
yr_list = []
fuel_list = []
ctpv_list = []
for fuel in grp1['fuel'].unique():
    for ctpv in grp1['시도'].unique():
        for yr in range(2019, year + 1):
            yr_list.append(str(yr))
            fuel_list.append(fuel)
            ctpv_list.append(ctpv)
base = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '시도':ctpv_list})

# 연도별 등록대수
grp2 = dfm2.groupby(['최초등록일자_년', 'fuel', '시도'], as_index=False)['차대번호'].count()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '차대번호':'등록대수'})

# 연도별 말소대수
grp3 = errc2.groupby(['변경일자_년', 'fuel', '시도'], as_index=False)['차대번호'].count()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '차대번호':'말소대수'})
base1 = base.merge(grp1, on=['연도', 'fuel', '시도'], how='left')
base2 = base1.merge(grp2, on=['연도', 'fuel', '시도'], how='left')
base3 = base2.merge(grp3, on=['연도', 'fuel', '시도'], how='left')
base3[['차량대수', '등록대수', '말소대수']].isnull().sum()
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

n = len(base3['연도'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

gp = base3.groupby(['연도', '시도', 'fuel'], as_index=False)['차량대수'].sum()
gp['연료비율'] = round((gp['차량대수'] / gp.groupby(['연도', '시도'])['차량대수'].transform('sum')), 2)

today_date = datetime.today().strftime("%Y%m%d")
gp['테이블생성일자'] = today_date
gp1 = gp[['테이블생성일자', '연도', 'fuel', '시도', '차량대수', '연료비율']]
cdict = {
    '테이블생성일자':'LOAD_DT', 
    '연도':'YR', 
    'fuel':'FUEL_CD', 
    '시도':'CTPV', 
    '차량대수':'VHCL_MKCNT', 
    '연료비율':'FUEL_RT'
}
STD_BD_CAR_REG_MKCNT = gp1.rename(columns=cdict)

# STD_BD_CAR_REG_MKCNT.columns

### [출력] STD_BD_CAR_REG_MKCNT

# expdf = STD_BD_CAR_REG_MKCNT
# table_nm = 'STD_BD_CAR_REG_MKCNT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_CAR_REG_MKCNT')

## 내연차 연료, 연도별 차량 현황 예측
# - 경유, 휘발유, LPG
dfm2dgl = dfm2.loc[(dfm2['연료'] == '경유') | (dfm2['연료'] == '휘발유') | (dfm2['연료'] == 'LPG(액화석유가스)')].reset_index(drop=True)
errc2dgl = errc2.loc[(errc2['연료'] == '경유') | (errc2['연료'] == '휘발유') | (errc2['연료'] == 'LPG(액화석유가스)')].reset_index(drop=True)

# 2022년 차량 대수
grp1 = dfm2dgl.groupby('연료', as_index=False)['차대번호'].count()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})
grp1['연도'] = f'{year}'
grp1 = grp1[['연도', '연료', '차량대수']]

# 차량 통계 기본 데이터셋
yr_list = []
fuel_list = []
for fuel in grp1['연료'].unique():
    for yr in range(2019, year + 1):
        yr_list.append(str(yr))
        fuel_list.append(fuel)
base = pd.DataFrame({'연도':yr_list, '연료':fuel_list})

# 연도별 등록대수
grp2 = dfm2dgl.groupby(['최초등록일자_년', '연료'], as_index=False)['차대번호'].count()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '차대번호':'등록대수'})

# 연도별 말소대수
grp3 = errc2dgl.groupby(['변경일자_년', '연료'], as_index=False)['차대번호'].count()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '차대번호':'말소대수'})
base1 = base.merge(grp1, on=['연도', '연료'], how='left')
base2 = base1.merge(grp2, on=['연도', '연료'], how='left')
base3 = base2.merge(grp3, on=['연도', '연료'], how='left')
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

n = len(base3['연도'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

die = base3.loc[base3['연료'] == '경유', ['연도', '연료', '차량대수']].reset_index(drop=True)
gas = base3.loc[base3['연료'] == '휘발유', ['연도', '연료', '차량대수']].reset_index(drop=True)
lpg = base3.loc[base3['연료'] == 'LPG(액화석유가스)', ['연도', '연료', '차량대수']].reset_index(drop=True)

die['연도'] = die['연도'].astype('int')
gas['연도'] = gas['연도'].astype('int')
lpg['연도'] = lpg['연도'].astype('int')

# 선형 예측
fit1 = np.polyfit(die['연도'], die['차량대수'], 1)
fit2 = np.polyfit(gas['연도'], gas['차량대수'], 1)
fit3 = np.polyfit(lpg['연도'], lpg['차량대수'], 1)
a1, b1 = fit1
a2, b2 = fit2
a3, b3 = fit3

# BSpline 예측
spl1 = intp.BSpline(die['연도'], die['차량대수'], 1, extrapolate=True)
spl2 = intp.BSpline(gas['연도'], gas['차량대수'], 1, extrapolate=True)
spl3 = intp.BSpline(lpg['연도'], lpg['차량대수'], 1, extrapolate=True)
spl1pred = spl1(range(year + 1, 2036))
spl2pred = spl2(range(year + 1, 2036))
spl3pred = spl3(range(year + 1, 2036))

# akima 예측
aki1 = intp.Akima1DInterpolator(die['연도'], die['차량대수'])
aki2 = intp.Akima1DInterpolator(gas['연도'], gas['차량대수'])
aki3 = intp.Akima1DInterpolator(lpg['연도'], lpg['차량대수'])
aki1pred = aki1([x for x in range(year + 1, 2036)], extrapolate=True)
aki2pred = aki2([x for x in range(year + 1, 2036)], extrapolate=True)
aki3pred = aki3([x for x in range(year + 1, 2036)], extrapolate=True)

yr_list = []
fuel_list = []
pred_list = []
fuel = '경유'
for yr in range(year + 1, 2036):
    pred = a1 * yr + b1
    yr_list.append(yr)
    fuel_list.append(fuel)
    pred_list.append(pred)
die_pred = pd.DataFrame({'연도':yr_list, '연료':fuel_list, '경유_예측':pred_list})
die_pred['경유_예측_BSpline'] = spl1pred
die_pred['경유_예측_Akima'] = aki1pred

yr_list = []
fuel_list = []
pred_list = []
fuel = '휘발유'
for yr in range(year + 1, 2036):
    pred = a2 * yr + b2
    yr_list.append(yr)
    fuel_list.append(fuel)
    pred_list.append(pred)
gas_pred = pd.DataFrame({'연도':yr_list, '연료':fuel_list, '휘발유_예측':pred_list})
gas_pred['휘발유_예측_BSpline'] = spl2pred
gas_pred['휘발유_예측_Akima'] = aki2pred

yr_list = []
fuel_list = []
pred_list = []
fuel = 'LPG'
for yr in range(year + 1, 2036):
    pred = a3 * yr + b3
    yr_list.append(yr)
    fuel_list.append(fuel)
    pred_list.append(pred)
lpg_pred = pd.DataFrame({'연도':yr_list, '연료':fuel_list, 'LPG_예측':pred_list})
lpg_pred['LPG_예측_BSpline'] = spl3pred
lpg_pred['LPG_예측_Akima'] = aki3pred

die_t = pd.concat([die, die_pred], ignore_index=True)
gas_t = pd.concat([gas, gas_pred], ignore_index=True)
lpg_t = pd.concat([lpg, lpg_pred], ignore_index=True)
die_t = die_t.rename(columns={'차량대수':'경유_대수'})
gas_t = gas_t.rename(columns={'차량대수':'휘발유_대수'})
lpg_t = lpg_t.rename(columns={'차량대수':'LPG_대수'})

df5 = pd.concat([die_t[['연도', '경유_대수', '경유_예측', '경유_예측_BSpline', '경유_예측_Akima']], gas_t[['휘발유_대수', '휘발유_예측', '휘발유_예측_BSpline', '휘발유_예측_Akima']], lpg_t[['LPG_대수', 'LPG_예측', 'LPG_예측_BSpline', 'LPG_예측_Akima']]], axis=1)

# 음수 0으로 처리
df5.loc[df5['경유_예측'] < 0, '경유_예측'] = 0
df5.loc[df5['경유_예측_BSpline'] < 0, '경유_예측_BSpline'] = 0
df5.loc[df5['경유_예측_Akima'] < 0, '경유_예측_Akima'] = 0
df5.loc[df5['휘발유_예측'] < 0, '휘발유_예측'] = 0
df5.loc[df5['휘발유_예측_BSpline'] < 0, '휘발유_예측_BSpline'] = 0
df5.loc[df5['휘발유_예측_Akima'] < 0, '휘발유_예측_Akima'] = 0
df5.loc[df5['LPG_예측'] < 0, 'LPG_예측'] = 0
df5.loc[df5['LPG_예측_BSpline'] < 0, 'LPG_예측_BSpline'] = 0
df5.loc[df5['LPG_예측_Akima'] < 0, 'LPG_예측_Akima'] = 0

# 첫째자리까지 반올림
df5[['경유_대수', '휘발유_대수', 'LPG_대수', '경유_예측', '경유_예측_BSpline','경유_예측_Akima', '휘발유_예측', '휘발유_예측_BSpline', '휘발유_예측_Akima', 'LPG_예측', 'LPG_예측_BSpline', 'LPG_예측_Akima']] = df5[['경유_대수', '휘발유_대수', 'LPG_대수', '경유_예측', '경유_예측_BSpline','경유_예측_Akima', '휘발유_예측', '휘발유_예측_BSpline', '휘발유_예측_Akima', 'LPG_예측', 'LPG_예측_BSpline', 'LPG_예측_Akima']].round(0)

today_date = datetime.today().strftime("%Y%m%d")
df5['테이블생성일자'] = today_date
df5 = df5[[
   '테이블생성일자',
   '연도',
   '경유_대수',
   '휘발유_대수',
   'LPG_대수',
   '경유_예측',
   '경유_예측_BSpline',
   '경유_예측_Akima',
   '휘발유_예측',
   '휘발유_예측_BSpline',
   '휘발유_예측_Akima',
   'LPG_예측',
   'LPG_예측_BSpline',
   'LPG_예측_Akima',
    ]]
cdict = {
    '테이블생성일자':'LOAD_DT', 
    '연도':'YR', 
    '경유_대수':'DSL', 
    '휘발유_대수':'GSL', 
    'LPG_대수':'LPG', 
    '경유_예측':'DSL_PRET', 
    '경유_예측_BSpline':'DSL_PRET_BSPLN', 
    '경유_예측_Akima':'DSL_PRET_AKM', 
    '휘발유_예측':'GSL_PRET', 
    '휘발유_예측_BSpline':'GSL_PRET_BSPLN', 
    '휘발유_예측_Akima':'GSL_PRET_AKM', 
    'LPG_예측':'LPG_PRET',
    'LPG_예측_BSpline':'LPG_PRET_BSPLN',
    'LPG_예측_Akima':'LPG_PRET_AKM',
}
STD_BD_CAR_PRET = df5.rename(columns=cdict)

# STD_BD_CAR_PRET.columns

### [출력] STD_BD_CAR_PRET

# expdf = STD_BD_CAR_PRET
# table_nm = 'STD_BD_CAR_PRET'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_CAR_PRET')

## 하이브리드 연료, 연도별 차량 현황 예측
# - 경유 하이브리드, 휘발유 하이브리드, LPG 하이브리드
dfm2h = dfm2.loc[(dfm2['연료'] == '경유 하이브리드') | (dfm2['연료'] == '휘발유 하이브리드') | (dfm2['연료'] == 'LPG 하이브리드')].reset_index(drop=True)
errc2h = errc2.loc[(errc2['연료'] == '경유 하이브리드') | (errc2['연료'] == '휘발유 하이브리드') | (errc2['연료'] == 'LPG 하이브리드')].reset_index(drop=True)

# 2022년 차량 대수
grp1 = dfm2h.groupby('연료', as_index=False)['차대번호'].count()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})
grp1['연도'] = f'{year}'
grp1 = grp1[['연도', '연료', '차량대수']]

# 차량 통계 기본 데이터셋
yr_list = []
fuel_list = []
for fuel in grp1['연료'].unique():
    for yr in range(2019, year + 1):
        yr_list.append(str(yr))
        fuel_list.append(fuel)
base = pd.DataFrame({'연도':yr_list, '연료':fuel_list})

# 연도별 등록대수
grp2 = dfm2h.groupby(['최초등록일자_년', '연료'], as_index=False)['차대번호'].count()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '차대번호':'등록대수'})

# 연도별 말소대수
grp3 = errc2h.groupby(['변경일자_년', '연료'], as_index=False)['차대번호'].count()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '차대번호':'말소대수'})
base1 = base.merge(grp1, on=['연도', '연료'], how='left')
base2 = base1.merge(grp2, on=['연도', '연료'], how='left')
base3 = base2.merge(grp3, on=['연도', '연료'], how='left')
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

n = len(base3['연도'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

die = base3.loc[base3['연료'] == '경유 하이브리드', ['연도', '연료', '차량대수']].reset_index(drop=True)
gas = base3.loc[base3['연료'] == '휘발유 하이브리드', ['연도', '연료', '차량대수']].reset_index(drop=True)
lpg = base3.loc[base3['연료'] == 'LPG 하이브리드', ['연도', '연료', '차량대수']].reset_index(drop=True)
die['연도'] = die['연도'].astype('int')
gas['연도'] = gas['연도'].astype('int')
lpg['연도'] = lpg['연도'].astype('int')

# 선형예측
fit1 = np.polyfit(die['연도'], die['차량대수'], 1)
fit2 = np.polyfit(gas['연도'], gas['차량대수'], 1)
fit3 = np.polyfit(lpg['연도'], lpg['차량대수'], 1)
a1, b1 = fit1
a2, b2 = fit2
a3, b3 = fit3

# BSpline 예측
spl1 = intp.BSpline(die['연도'], die['차량대수'], 1, extrapolate=True)
spl2 = intp.BSpline(gas['연도'], gas['차량대수'], 1, extrapolate=True)
spl3 = intp.BSpline(lpg['연도'], lpg['차량대수'], 1, extrapolate=True)
spl1pred = spl1(range(year + 1, 2036))
spl2pred = spl2(range(year + 1, 2036))
spl3pred = spl3(range(year + 1, 2036))

# akima 예측
aki1 = intp.Akima1DInterpolator(die['연도'], die['차량대수'])
aki2 = intp.Akima1DInterpolator(gas['연도'], gas['차량대수'])
aki3 = intp.Akima1DInterpolator(lpg['연도'], lpg['차량대수'])
aki1pred = aki1([x for x in range(year + 1, 2036)], extrapolate=True)
aki2pred = aki2([x for x in range(year + 1, 2036)], extrapolate=True)
aki3pred = aki3([x for x in range(year + 1, 2036)], extrapolate=True)

yr_list = []
fuel_list = []
pred_list = []
fuel = '경유_하이브리드'
for yr in range(year + 1, 2036):
    pred = a1 * yr + b1
    yr_list.append(yr)
    fuel_list.append(fuel)
    pred_list.append(pred)
die_pred = pd.DataFrame({'연도':yr_list, '연료':fuel_list, f'{fuel}_예측':pred_list})
die_pred[f'{fuel}_예측_BSpline'] = spl1pred
die_pred[f'{fuel}_예측_Akima'] = aki1pred

# die_pred.columns

yr_list = []
fuel_list = []
pred_list = []
fuel = '휘발유_하이브리드'
for yr in range(year + 1, 2036):
    pred = a2 * yr + b2
    yr_list.append(yr)
    fuel_list.append(fuel)
    pred_list.append(pred)
gas_pred = pd.DataFrame({'연도':yr_list, '연료':fuel_list, f'{fuel}_예측':pred_list})
gas_pred[f'{fuel}_예측_BSpline'] = spl2pred
gas_pred[f'{fuel}_예측_Akima'] = aki2pred

# gas_pred.columns

yr_list = []
fuel_list = []
pred_list = []
fuel = 'LPG_하이브리드'
for yr in range(year + 1, 2036):
    pred = a3 * yr + b3
    yr_list.append(yr)
    fuel_list.append(fuel)
    pred_list.append(pred)
lpg_pred = pd.DataFrame({'연도':yr_list, '연료':fuel_list, f'{fuel}_예측':pred_list})
lpg_pred[f'{fuel}_예측_BSpline'] = spl3pred
lpg_pred[f'{fuel}_예측_Akima'] = aki3pred

# lpg_pred.columns

die_t = pd.concat([die, die_pred], ignore_index=True)
gas_t = pd.concat([gas, gas_pred], ignore_index=True)
lpg_t = pd.concat([lpg, lpg_pred], ignore_index=True)
die_t = die_t.rename(columns={'차량대수':'경유_하이브리드_대수'})
gas_t = gas_t.rename(columns={'차량대수':'휘발유_하이브리드_대수'})
lpg_t = lpg_t.rename(columns={'차량대수':'LPG_하이브리드_대수'})

# die_t.columns

die_t = die_t.drop('연료', axis=1)
gas_t = gas_t.drop(['연도', '연료'], axis=1)
lpg_t = lpg_t.drop(['연도', '연료'], axis=1)

df5 = pd.concat([die_t, gas_t, lpg_t], axis=1)

# df5.columns

# df5.head()

# df5.iloc[:, 1:].head()

# # 음수 확인
# print(df5.loc[df5['경유_하이브리드_대수'] < 0, '경유_하이브리드_대수'].shape)
# print(df5.loc[df5['경유_하이브리드_예측'] < 0, '경유_하이브리드_예측'].shape)
# print(df5.loc[df5['경유_하이브리드_예측_BSpline'] < 0, '경유_하이브리드_예측_BSpline'].shape)
# print(df5.loc[df5['경유_하이브리드_예측_Akima'] < 0, '경유_하이브리드_예측_Akima'].shape)
# print(df5.loc[df5['휘발유_하이브리드_대수'] < 0, '휘발유_하이브리드_대수'].shape)
# print(df5.loc[df5['휘발유_하이브리드_예측'] < 0, '휘발유_하이브리드_예측'].shape)
# print(df5.loc[df5['휘발유_하이브리드_예측_BSpline'] < 0, '휘발유_하이브리드_예측_BSpline'].shape)
# print(df5.loc[df5['휘발유_하이브리드_예측_Akima'] < 0, '휘발유_하이브리드_예측_Akima'].shape)
# print(df5.loc[df5['LPG_하이브리드_대수'] < 0, 'LPG_하이브리드_대수'].shape)
# print(df5.loc[df5['LPG_하이브리드_예측'] < 0, 'LPG_하이브리드_예측'].shape)
# print(df5.loc[df5['LPG_하이브리드_예측_BSpline'] < 0, 'LPG_하이브리드_예측_BSpline'].shape)
# print(df5.loc[df5['LPG_하이브리드_예측_Akima'] < 0, 'LPG_하이브리드_예측_Akima'].shape)

# 음수 0으로 처리
df5.loc[df5['경유_하이브리드_대수'] < 0, '경유_하이브리드_대수'] = 0
df5.loc[df5['경유_하이브리드_예측'] < 0, '경유_하이브리드_예측'] = 0
df5.loc[df5['경유_하이브리드_예측_BSpline'] < 0, '경유_하이브리드_예측_BSpline'] = 0
df5.loc[df5['경유_하이브리드_예측_Akima'] < 0, '경유_하이브리드_예측_Akima'] = 0
df5.loc[df5['휘발유_하이브리드_대수'] < 0, '휘발유_하이브리드_대수'] = 0
df5.loc[df5['휘발유_하이브리드_예측'] < 0, '휘발유_하이브리드_예측'] = 0
df5.loc[df5['휘발유_하이브리드_예측_BSpline'] < 0, '휘발유_하이브리드_예측_BSpline'] = 0
df5.loc[df5['휘발유_하이브리드_예측_Akima'] < 0, '휘발유_하이브리드_예측_Akima'] = 0
df5.loc[df5['LPG_하이브리드_대수'] < 0, 'LPG_하이브리드_대수'] = 0
df5.loc[df5['LPG_하이브리드_예측'] < 0, 'LPG_하이브리드_예측'] = 0
df5.loc[df5['LPG_하이브리드_예측_BSpline'] < 0, 'LPG_하이브리드_예측_BSpline'] = 0
df5.loc[df5['LPG_하이브리드_예측_Akima'] < 0, 'LPG_하이브리드_예측_Akima'] = 0

# # 음수 확인
# print(df5.loc[df5['경유_하이브리드_대수'] < 0, '경유_하이브리드_대수'].shape)
# print(df5.loc[df5['경유_하이브리드_예측'] < 0, '경유_하이브리드_예측'].shape)
# print(df5.loc[df5['경유_하이브리드_예측_BSpline'] < 0, '경유_하이브리드_예측_BSpline'].shape)
# print(df5.loc[df5['경유_하이브리드_예측_Akima'] < 0, '경유_하이브리드_예측_Akima'].shape)
# print(df5.loc[df5['휘발유_하이브리드_대수'] < 0, '휘발유_하이브리드_대수'].shape)
# print(df5.loc[df5['휘발유_하이브리드_예측'] < 0, '휘발유_하이브리드_예측'].shape)
# print(df5.loc[df5['휘발유_하이브리드_예측_BSpline'] < 0, '휘발유_하이브리드_예측_BSpline'].shape)
# print(df5.loc[df5['휘발유_하이브리드_예측_Akima'] < 0, '휘발유_하이브리드_예측_Akima'].shape)
# print(df5.loc[df5['LPG_하이브리드_대수'] < 0, 'LPG_하이브리드_대수'].shape)
# print(df5.loc[df5['LPG_하이브리드_예측'] < 0, 'LPG_하이브리드_예측'].shape)
# print(df5.loc[df5['LPG_하이브리드_예측_BSpline'] < 0, 'LPG_하이브리드_예측_BSpline'].shape)
# print(df5.loc[df5['LPG_하이브리드_예측_Akima'] < 0, 'LPG_하이브리드_예측_Akima'].shape)

# 첫째자리까지 반올림
df5.iloc[:, 1:] = df5.iloc[:, 1:].round(0)

# df5.head()

today_date = datetime.today().strftime("%Y%m%d")
df5['테이블생성일자'] = today_date

df5 = df5[[
    '테이블생성일자', 
    '연도', 
    '휘발유_하이브리드_대수', 
    '휘발유_하이브리드_예측',
    '휘발유_하이브리드_예측_BSpline',
    '휘발유_하이브리드_예측_Akima',
    '경유_하이브리드_대수', 
    '경유_하이브리드_예측',
    '경유_하이브리드_예측_BSpline',
    '경유_하이브리드_예측_Akima',
    'LPG_하이브리드_대수', 
    'LPG_하이브리드_예측',
    'LPG_하이브리드_예측_BSpline', 
    'LPG_하이브리드_예측_Akima', 
    ]]
cdict = {
    '테이블생성일자':'LOAD_DT', 
    '연도':'YR', 
    '휘발유_하이브리드_대수':'GSLH', 
    '휘발유_하이브리드_예측':'GSLH_PRET', 
    '휘발유_하이브리드_예측_BSpline':'GSLH_PRET_BSPLN', 
    '휘발유_하이브리드_예측_Akima':'GSLH_PRET_AKM', 
    '경유_하이브리드_대수':'DSLH', 
    '경유_하이브리드_예측':'DSLH_PRET', 
    '경유_하이브리드_예측_BSpline':'DSLH_PRET_BSPLN', 
    '경유_하이브리드_예측_Akima':'DSLH_PRET_AKM', 
    'LPG_하이브리드_대수':'LPGH', 
    'LPG_하이브리드_예측':'LPGH_PRET',
    'LPG_하이브리드_예측_BSpline':'LPGH_PRET_BSPLN',
    'LPG_하이브리드_예측_Akima':'LPGH_PRET_AKM',
}
STD_BD_HYBRD_CAR_PRET = df5.rename(columns=cdict)

# STD_BD_HYBRD_CAR_PRET.columns

### [출력] STD_BD_HYBRD_CAR_PRET

# expdf = STD_BD_HYBRD_CAR_PRET
# table_nm = 'STD_BD_HYBRD_CAR_PRET'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_HYBRD_CAR_PRET')

## 내연차 연료, 등급, 연도별 차량 현황 예측
# - 경유, 휘발유, LPG
# 2022년 차량 대수
grp1 = dfm2dgl.groupby(['fuel', '배출가스등급'], as_index=False)['차대번호'].count()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})
grp1['연도'] = f'{year}'
grp1 = grp1[['연도', 'fuel', '배출가스등급', '차량대수']]

# 차량 통계 기본 데이터셋
yr_list = []
fuel_list = []
grd_list = []
for fuel in grp1['fuel'].unique():
    for grd in grp1['배출가스등급'].unique():
        for yr in range(2019, year + 1):
            yr_list.append(str(yr))
            fuel_list.append(fuel)
            grd_list.append(grd)
base = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list})

# 연도별 등록대수
grp2 = dfm2dgl.groupby(['최초등록일자_년', 'fuel', '배출가스등급'], as_index=False)['차대번호'].count()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '차대번호':'등록대수'})

# 연도별 말소대수
grp3 = errc2dgl.groupby(['변경일자_년', 'fuel', '배출가스등급'], as_index=False)['차대번호'].count()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '차대번호':'말소대수'})
base1 = base.merge(grp1, on=['연도', 'fuel', '배출가스등급'], how='left')
base2 = base1.merge(grp2, on=['연도', 'fuel', '배출가스등급'], how='left')
base3 = base2.merge(grp3, on=['연도', 'fuel', '배출가스등급'], how='left')
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

n = len(base3['연도'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

# die = base3.loc[base3['fuel'] == '경유', ['연도', 'fuel', '배출가스등급', '차량대수']].reset_index(drop=True)
# gas = base3.loc[base3['fuel'] == '휘발유', ['연도', 'fuel', '배출가스등급', '차량대수']].reset_index(drop=True)
# lpg = base3.loc[base3['fuel'] == 'LPG', ['연도', 'fuel', '배출가스등급', '차량대수']].reset_index(drop=True)

# die['연도'] = die['연도'].astype('int')
# gas['연도'] = gas['연도'].astype('int')
# lpg['연도'] = lpg['연도'].astype('int')

# die1 = die.loc[die['배출가스등급'] == '1'].reset_index(drop=True)
# die2 = die.loc[die['배출가스등급'] == '2'].reset_index(drop=True)
# die3 = die.loc[die['배출가스등급'] == '3'].reset_index(drop=True)
# die4 = die.loc[die['배출가스등급'] == '4'].reset_index(drop=True)
# die5 = die.loc[die['배출가스등급'] == '5'].reset_index(drop=True)
# gas1 = gas.loc[gas['배출가스등급'] == '1'].reset_index(drop=True)
# gas2 = gas.loc[gas['배출가스등급'] == '2'].reset_index(drop=True)
# gas3 = gas.loc[gas['배출가스등급'] == '3'].reset_index(drop=True)
# gas4 = gas.loc[gas['배출가스등급'] == '4'].reset_index(drop=True)
# gas5 = gas.loc[gas['배출가스등급'] == '5'].reset_index(drop=True)
# lpg1 = lpg.loc[lpg['배출가스등급'] == '1'].reset_index(drop=True)
# lpg2 = lpg.loc[lpg['배출가스등급'] == '2'].reset_index(drop=True)
# lpg3 = lpg.loc[lpg['배출가스등급'] == '3'].reset_index(drop=True)
# lpg4 = lpg.loc[lpg['배출가스등급'] == '4'].reset_index(drop=True)
# lpg5 = lpg.loc[lpg['배출가스등급'] == '5'].reset_index(drop=True)

# fit_d1 = np.polyfit(die1['연도'], die1['차량대수'], 1)
# fit_d2 = np.polyfit(die2['연도'], die2['차량대수'], 1)
# fit_d3 = np.polyfit(die3['연도'], die3['차량대수'], 1)
# fit_d4 = np.polyfit(die4['연도'], die4['차량대수'], 1)
# fit_d5 = np.polyfit(die5['연도'], die5['차량대수'], 1)
# fit_g1 = np.polyfit(gas1['연도'], gas1['차량대수'], 1)
# fit_g2 = np.polyfit(gas2['연도'], gas2['차량대수'], 1)
# fit_g3 = np.polyfit(gas3['연도'], gas3['차량대수'], 1)
# fit_g4 = np.polyfit(gas4['연도'], gas4['차량대수'], 1)
# fit_g5 = np.polyfit(gas5['연도'], gas5['차량대수'], 1)
# fit_l1 = np.polyfit(lpg1['연도'], lpg1['차량대수'], 1)
# fit_l2 = np.polyfit(lpg2['연도'], lpg2['차량대수'], 1)
# fit_l3 = np.polyfit(lpg3['연도'], lpg3['차량대수'], 1)
# fit_l4 = np.polyfit(lpg4['연도'], lpg4['차량대수'], 1)
# fit_l5 = np.polyfit(lpg5['연도'], lpg5['차량대수'], 1)

# ad1, bd1 = fit_d1
# ad2, bd2 = fit_d2
# ad3, bd3 = fit_d3
# ad4, bd4 = fit_d4
# ad5, bd5 = fit_d5
# ag1, bg1 = fit_g1
# ag2, bg2 = fit_g2
# ag3, bg3 = fit_g3
# ag4, bg4 = fit_g4
# ag5, bg5 = fit_g5
# al1, bl1 = fit_l1
# al2, bl2 = fit_l2
# al3, bl3 = fit_l3
# al4, bl4 = fit_l4
# al5, bl5 = fit_l5

# # 경유 1등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '경유'
# grd = '1'
# for yr in range(year + 1, 2036):
#     pred = ad1 * yr + bd1
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# die1_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'경유_예측':pred_list})
# # 경유 2등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '경유'
# grd = '2'
# for yr in range(year + 1, 2036):
#     pred = ad2 * yr + bd2
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# die2_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'경유_예측':pred_list})
# # 경유 3등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '경유'
# grd = '3'
# for yr in range(year + 1, 2036):
#     pred = ad3 * yr + bd3
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# die3_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'경유_예측':pred_list})
# # 경유 4등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '경유'
# grd = '4'
# for yr in range(year + 1, 2036):
#     pred = ad4 * yr + bd4
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# die4_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'경유_예측':pred_list})
# # 경유 5등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '경유'
# grd = '5'
# for yr in range(year + 1, 2036):
#     pred = ad5 * yr + bd5
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# die5_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'경유_예측':pred_list})

# # 휘발유 1등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '휘발유'
# grd = '1'
# for yr in range(year + 1, 2036):
#     pred = ag1 * yr + bg1
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# gas1_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'휘발유_예측':pred_list})
# # 휘발유 2등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '휘발유'
# grd = '2'
# for yr in range(year + 1, 2036):
#     pred = ag2 * yr + bg2
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# gas2_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'휘발유_예측':pred_list})
# # 휘발유 3등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '휘발유'
# grd = '3'
# for yr in range(year + 1, 2036):
#     pred = ag3 * yr + bg3
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# gas3_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'휘발유_예측':pred_list})
# # 휘발유 4등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '휘발유'
# grd = '4'
# for yr in range(year + 1, 2036):
#     pred = ag4 * yr + bg4
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# gas4_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'휘발유_예측':pred_list})
# # 휘발유 5등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = '휘발유'
# grd = '5'
# for yr in range(year + 1, 2036):
#     pred = ag5 * yr + bg5
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# gas5_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'휘발유_예측':pred_list})

# # LPG 1등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = 'LPG'
# grd = '1'
# for yr in range(year + 1, 2036):
#     pred = al1 * yr + bl1
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# lpg1_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'LPG_예측':pred_list})
# # LPG 2등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = 'LPG'
# grd = '2'
# for yr in range(year + 1, 2036):
#     pred = al2 * yr + bl2
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# lpg2_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'LPG_예측':pred_list})
# # LPG 3등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = 'LPG'
# grd = '3'
# for yr in range(year + 1, 2036):
#     pred = al3 * yr + bl3
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# lpg3_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'LPG_예측':pred_list})
# # LPG 4등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = 'LPG'
# grd = '4'
# for yr in range(year + 1, 2036):
#     pred = al4 * yr + bl4
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# lpg4_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'LPG_예측':pred_list})
# # LPG 5등급 예측
# yr_list = []
# fuel_list = []
# grd_list = []
# pred_list = []
# fuel = 'LPG'
# grd = '5'
# for yr in range(year + 1, 2036):
#     pred = al5 * yr + bl5
#     yr_list.append(yr)
#     fuel_list.append(fuel)
#     grd_list.append(grd)
#     pred_list.append(pred)
# lpg5_pred = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list, '배출가스등급':grd_list,'LPG_예측':pred_list})

# die_t = pd.concat([die1, die1_pred, die2, die2_pred, die3, die3_pred, die4, die4_pred, die5, die5_pred], ignore_index=True)
# gas_t = pd.concat([gas1, gas1_pred, gas2, gas2_pred, gas3, gas3_pred, gas4, gas4_pred, gas5, gas5_pred], ignore_index=True)
# lpg_t = pd.concat([lpg1, lpg1_pred, lpg2, lpg2_pred, lpg3, lpg3_pred, lpg4, lpg4_pred, lpg5, lpg5_pred], ignore_index=True)
# die_t = die_t.rename(columns={'경유_예측':'차량예측'})
# gas_t = gas_t.rename(columns={'휘발유_예측':'차량예측'})
# lpg_t = lpg_t.rename(columns={'LPG_예측':'차량예측'})

# df6 = pd.concat([die_t, gas_t, lpg_t], ignore_index=True)

# # 음수 차량 대수 수정
# df6.loc[df6['차량예측'] < 0, '차량예측'] = 0

# # 첫째자리까지 반올림
# df6[['차량대수', '차량예측']] = df6[['차량대수', '차량예측']].round(0)

base3['연도'] = base3['연도'].astype('int')

total = pd.DataFrame()
for ctpv in base3['시도'].unique():
    for fuel in base3['fuel'].unique():
        for grd in base3['배출가스등급'].unique():
            temp = base3.loc[(base3['시도'] == ctpv) & (base3['fuel'] == fuel) & (base3['배출가스등급'] == grd)].reset_index(drop=True)
            a, b = np.polyfit(temp['연도'], temp['차량대수'], 1)
            yr_list, ctpv_list, fuel_list, grd_list, pred_list = [], [], [], [], []
            for yr in range(year + 1, 2036):
                pred = a * yr + b
                yr_list.append(yr)
                ctpv_list.append(ctpv)
                fuel_list.append(fuel)
                grd_list.append(grd)
                pred_list.append(pred)
            temp_pred = pd.DataFrame({'연도':yr_list, '시도':ctpv_list, 'fuel':fuel_list, '배출가스등급':grd_list, '차량예측':pred_list})
            ttemp = pd.concat([temp, temp_pred], ignore_index=True)
            total = pd.concat([total, ttemp], ignore_index=True)

# 음수 차량 대수 수정
total.loc[total['차량예측'] < 0, '차량예측'] = 0            
# 첫째자리까지 반올림
total[['차량대수', '차량예측']] = total[['차량대수', '차량예측']].round(0)

df6 = total[['연도', '시도', 'fuel', '배출가스등급', '차량대수', '차량예측']]

today_date = datetime.today().strftime("%Y%m%d")
df6['테이블생성일자'] = today_date
cdict = {
    '테이블생성일자':'LOAD_DT', 
    '연도':'YR', 
    '시도':'CTPV', 
    'fuel':'FUEL_CD', 
    '배출가스등급':'EXHST_GAS_GRD_CD', 
    '차량대수':'VHCL_MKCNT', 
    '차량예측':'VHCL_PRET', 
}
STD_BD_FUEL_GRD_VHCL_CURSTT_PRET = df6.rename(columns=cdict)

### [출력] STD_BD_FUEL_GRD_VHCL_CURSTT_PRET

# expdf = STD_BD_FUEL_GRD_VHCL_CURSTT_PRET
# table_nm = 'STD_BD_FUEL_GRD_VHCL_CURSTT_PRET'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_FUEL_GRD_VHCL_CURSTT_PRET')

## 무공해차 연료, 연도별 차량 현황 예측
# - 전기, 수소
dfm2bh = dfm2.loc[(dfm2['fuel'] == '전기') | (dfm2['fuel'] == '수소')].reset_index(drop=True)
errc2bh = errc2.loc[(errc2['fuel'] == '전기') | (errc2['fuel'] == '수소')].reset_index(drop=True)

# 2022년 차량 대수
grp1 = dfm2bh.groupby('fuel', as_index=False)['차대번호'].count()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})
grp1['연도'] = f'{year}'
grp1 = grp1[['연도', 'fuel', '차량대수']]

# 차량 통계 기본 데이터셋
yr_list = []
fuel_list = []
for fuel in grp1['fuel'].unique():
    for yr in range(2019, year + 1):
        yr_list.append(str(yr))
        fuel_list.append(fuel)
base = pd.DataFrame({'연도':yr_list, 'fuel':fuel_list})

# 연도별 등록대수
grp2 = dfm2bh.groupby(['최초등록일자_년', 'fuel'], as_index=False)['차대번호'].count()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '차대번호':'등록대수'})

# 연도별 말소대수
grp3 = errc2bh.groupby(['변경일자_년', 'fuel'], as_index=False)['차대번호'].count()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '차대번호':'말소대수'})
base1 = base.merge(grp1, on=['연도', 'fuel'], how='left')
base2 = base1.merge(grp2, on=['연도', 'fuel'], how='left')
base3 = base2.merge(grp3, on=['연도', 'fuel'], how='left')
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

n = len(base3['연도'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

bt = base3.loc[base3['fuel'] == '전기', ['연도', 'fuel', '차량대수']].reset_index(drop=True)
hy = base3.loc[base3['fuel'] == '수소', ['연도', 'fuel', '차량대수']].reset_index(drop=True)
bt['연도'] = bt['연도'].astype('int')
hy['연도'] = hy['연도'].astype('int')

# 선형예측
fit1 = np.polyfit(bt['연도'], bt['차량대수'], 1)
fit2 = np.polyfit(hy['연도'], hy['차량대수'], 1)
a1, b1 = fit1
a2, b2 = fit2



# BSpline 예측
spl1 = intp.BSpline(bt['연도'], bt['차량대수'], 1, extrapolate=True)
spl2 = intp.BSpline(hy['연도'], hy['차량대수'], 1, extrapolate=True)
spl1pred = spl1(range(year + 1, 2036))
spl2pred = spl2(range(year + 1, 2036))

# akima 예측
aki1 = intp.Akima1DInterpolator(bt['연도'], bt['차량대수'])
aki2 = intp.Akima1DInterpolator(hy['연도'], hy['차량대수'])
aki1pred = aki1([x for x in range(year + 1, 2036)], extrapolate=True)
aki2pred = aki2([x for x in range(year + 1, 2036)], extrapolate=True)

yr_list = []
fuel_list = []
pred_list = []
fuel = '전기'
for yr in range(year + 1, 2036):
    pred = a1 * yr + b1
    yr_list.append(yr)
    fuel_list.append(fuel)
    pred_list.append(pred)
bt_pred = pd.DataFrame({'연도':yr_list, '연료':fuel_list, f'{fuel}_예측':pred_list})
bt_pred[f'{fuel}_예측_BSpline'] = spl1pred
bt_pred[f'{fuel}_예측_Akima'] = aki1pred

# bt_pred.columns

yr_list = []
fuel_list = []
pred_list = []
fuel = '수소'
for yr in range(year + 1, 2036):
    pred = a2 * yr + b2
    yr_list.append(yr)
    fuel_list.append(fuel)
    pred_list.append(pred)
hy_pred = pd.DataFrame({'연도':yr_list, '연료':fuel_list, f'{fuel}_예측':pred_list})
hy_pred[f'{fuel}_예측_BSpline'] = spl2pred
hy_pred[f'{fuel}_예측_Akima'] = aki2pred

# hy_pred.columns

bt_t = pd.concat([bt, bt_pred], ignore_index=True)
hy_t = pd.concat([hy, hy_pred], ignore_index=True)
bt_t = bt_t.rename(columns={'차량대수':'전기_대수'})
hy_t = hy_t.rename(columns={'차량대수':'수소_대수'})

bt_t = bt_t.drop('연료', axis=1)
hy_t = hy_t.drop(['연료', '연도'], axis=1)

df7 = pd.concat([bt_t, hy_t], axis=1)

# df7.columns

# df7.head()

# # 음수 확인
# print(df7.loc[df7['전기_대수'] < 0, '전기_대수'].shape)
# print(df7.loc[df7['전기_예측'] < 0, '전기_예측'].shape)
# print(df7.loc[df7['전기_예측_BSpline'] < 0, '전기_예측_BSpline'].shape)
# print(df7.loc[df7['전기_예측_Akima'] < 0, '전기_예측_Akima'].shape)
# print(df7.loc[df7['수소_대수'] < 0, '수소_대수'].shape)
# print(df7.loc[df7['수소_예측'] < 0, '수소_예측'].shape)
# print(df7.loc[df7['수소_예측_BSpline'] < 0, '수소_예측_BSpline'].shape)
# print(df7.loc[df7['수소_예측_Akima'] < 0, '수소_예측_Akima'].shape)

# 음수 0으로 처리
df7.loc[df7['전기_대수'] < 0, '전기_대수'] = 0
df7.loc[df7['전기_예측'] < 0, '전기_예측'] = 0
df7.loc[df7['전기_예측_BSpline'] < 0, '전기_예측_BSpline'] = 0
df7.loc[df7['전기_예측_Akima'] < 0, '전기_예측_Akima'] = 0
df7.loc[df7['수소_대수'] < 0, '수소_대수'] = 0
df7.loc[df7['수소_예측'] < 0, '수소_예측'] = 0
df7.loc[df7['수소_예측_BSpline'] < 0, '수소_예측_BSpline'] = 0
df7.loc[df7['수소_예측_Akima'] < 0, '수소_예측_Akima'] = 0

# # 음수 확인
# print(df7.loc[df7['전기_대수'] < 0, '전기_대수'].shape)
# print(df7.loc[df7['전기_예측'] < 0, '전기_예측'].shape)
# print(df7.loc[df7['전기_예측_BSpline'] < 0, '전기_예측_BSpline'].shape)
# print(df7.loc[df7['전기_예측_Akima'] < 0, '전기_예측_Akima'].shape)
# print(df7.loc[df7['수소_대수'] < 0, '수소_대수'].shape)
# print(df7.loc[df7['수소_예측'] < 0, '수소_예측'].shape)
# print(df7.loc[df7['수소_예측_BSpline'] < 0, '수소_예측_BSpline'].shape)
# print(df7.loc[df7['수소_예측_Akima'] < 0, '수소_예측_Akima'].shape)

# 첫째자리까지 반올림
df7.iloc[:, 1:] = df7.iloc[:, 1:].round(0)

# df7.head()

today_date = datetime.today().strftime("%Y%m%d")
df7['테이블생성일자'] = today_date
df7 = df7[[
    '테이블생성일자', 
    '연도', 
    '전기_대수', 
    '전기_예측',
    '전기_예측_BSpline',
    '전기_예측_Akima',
    '수소_대수', 
    '수소_예측',
    '수소_예측_BSpline',
    '수소_예측_Akima',
    ]]
cdict = {
    '테이블생성일자':'LOAD_DT', 
    '연도':'YR', 
    '전기_대수':'BTYCR', 
    '전기_예측':'BTYCR_PRET',
    '전기_예측_BSpline':'BTYCR_PRET_BSPLN',
    '전기_예측_Akima':'BTYCR_PRET_AKM',
    '수소_대수':'HY', 
    '수소_예측':'HY_PRET',
    '수소_예측_BSpline':'HY_PRET_BSPLN',
    '수소_예측_Akima':'HY_PRET_AKM',    
}
STD_BD_ECO_CAR_PRET = df7.rename(columns=cdict)

# STD_BD_ECO_CAR_PRET.columns

### [출력] STD_BD_ECO_CAR_PRET

# expdf = STD_BD_ECO_CAR_PRET
# table_nm = 'STD_BD_ECO_CAR_PRET'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_ECO_CAR_PRET')

## 지역, 등급별 말소 차량 현황
errc2['배출가스등급'] = errc2['배출가스등급'].map({'1':'1.0', '2':'2.0', '3':'3.0', '4':'4.0', '5':'5.0', 'X':'X'})

today_date = datetime.today().strftime("%Y%m%d")
errc2['테이블생성일자'] = today_date
df8 = errc2[[
    '테이블생성일자', 
    '법정동코드',
    '시도',
    '시군구', 
    '차대번호', 
    '변경일자', 
    '배출가스등급', 
    '연료'
    ]]
chc_col = {
    '테이블생성일자':'LOAD_DT', 
    '법정동코드':'BSPL_STDG_CD',
    '시도':'CTPV',
    '시군구':'SGG', 
    '차대번호':'VIN', 
    '변경일자':'CHG_YMD', 
    '배출가스등급':'EXHST_GAS_GRD_CD', 
    '연료':'FUEL_CD', 
}
STD_BD_ERSR_RSLT = df8.rename(columns=chc_col)

# STD_BD_ERSR_RSLT.columns

### [출력] STD_BD_ERSR_RSLT

# expdf = STD_BD_ERSR_RSLT
# table_nm = 'STD_BD_ERSR_RSLT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 7s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_ERSR_RSLT')

## 등급별현황 테이블
# - 시도, 연도, 월, 등급, 연료, 차종, 차량유형, 용도별 / 차량대수, 말소차량대수, 차량 비율
df9 = dfm.copy()

# 데이터 연도 설정
year = 2022
month = 12
today_date = datetime.today().strftime("%Y%m%d")
# year = int(today_date[:4])
# month = int(today_date[4:6])

# 차량 대수
grp1 = df9.groupby(['시도', '배출가스등급', '연료', '차종', '차종유형', '용도'], as_index=False)['차대번호'].count()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})
grp1['연도'] = f'{year}'
grp1['월'] = f'{month}'

# 날짜 설정
date_date = '20221231'
# date_date = datetime.today().strftime("%Y%m%d")
# 37.5s

y_plist = list(pd.date_range(end=date_date, periods=4, freq="MS").year)
mth_plist = list(pd.date_range(end=date_date, periods=4, freq="MS").month)

# y_plist, mth_plist

# 37.5s
# 차량 통계 기본 데이터셋
ctpv_list, yr_list, month_list, grd_list, fuel_list, vhcty_list, ty_list, purps_list = [], [], [], [], [], [], [], []
for ctpv in grp1['시도'].unique():
    for yr, month in zip(y_plist, mth_plist):
        for grd in grp1['배출가스등급'].unique():
            for fuel in grp1['연료'].unique():
                for vhcty in grp1['차종'].unique():
                    for ty in grp1['차종유형'].unique():
                        for purps in grp1['용도'].unique():
                            ctpv_list.append(ctpv)
                            yr_list.append(str(yr))
                            month_list.append(f'{month:0>2}')
                            grd_list.append(grd)
                            fuel_list.append(fuel)
                            vhcty_list.append(vhcty)
                            ty_list.append(ty)
                            purps_list.append(purps)
base = pd.DataFrame({
    '시도':ctpv_list, 
    '연도':yr_list, 
    '월':month_list, 
    '배출가스등급':grd_list, 
    '연료':fuel_list, 
    '차종':vhcty_list, 
    '차종유형':ty_list, 
    '용도':purps_list, 
    })

# 13.6s
# 연도별 등록대수
grp2 = df9.groupby(['시도', '최초등록일자_년', '최초등록일자_월', '배출가스등급', '연료', '차종', '차종유형', '용도'], as_index=False)['차대번호'].count()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '최초등록일자_월':'월', '차대번호':'등록대수'})

#2.5s
# 연도별 말소대수
grp3 = errc.groupby(['시도', '변경일자_년', '변경일자_월', '배출가스등급', '연료', '차종', '차종유형', '용도'], as_index=False)['차대번호'].count()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '변경일자_월':'월', '차대번호':'말소대수'})

base1 = base.merge(grp1, on=['시도', '연도', '월', '배출가스등급', '연료', '차종', '차종유형', '용도'], how='left')
base2 = base1.merge(grp2, on=['시도', '연도', '월', '배출가스등급', '연료', '차종', '차종유형', '용도'], how='left')
base3 = base2.merge(grp3, on=['시도', '연도', '월', '배출가스등급', '연료', '차종', '차종유형', '용도'], how='left')
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)
base3 = base3.sort_values(['시도', '배출가스등급', '연료', '차종', '차종유형', '용도', '연도', '월']).reset_index(drop=True)

# 1m 28.6s
n = len(base3['월'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

anl1 = base3[['시도', '연도', '월', '배출가스등급', '연료', '차종', '차종유형', '용도', '차량대수', '말소대수']]
anl1['연도별_차량대수'] = anl1.groupby(['연도'])['차량대수'].transform('sum')
anl1['연도_연료별_차량대수'] = anl1.groupby(['연도', '연료'])['차량대수'].transform('sum')
anl1['연도_연료차량비율'] = anl1['연도_연료별_차량대수'] / anl1['연도별_차량대수']
anl1['테이블생성일자'] = today_date

STD_BD_DAT_GRD_CURSTT = anl1[[
    '시도',
    '연도',
    '월',
    '배출가스등급',
    '연료',
    '차종',
    '차종유형',
    '용도',
    '차량대수',
    '말소대수',
    '연도_연료차량비율',
    '테이블생성일자'
    ]]
cdict = {
    '시도':'CTPV', 
    '연도':'YR', 
    '월':'MM', 
    '배출가스등급':'EXHST_GAS_GRD_CD', 
    '연료':'FUEL_CD', 
    '차종':'VHCTY_CD', 
    '차종유형':'VHCTY_TY', 
    '용도':'PURPS_CD2', 
    '차량대수':'VHCL_MKCNT', 
    '말소대수':'ERSR_MKCNT', 
    '연도_연료차량비율':'YR_FUEL_VHCL_RT', 
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_GRD_CURSTT = STD_BD_DAT_GRD_CURSTT.rename(columns=cdict)

# STD_BD_DAT_GRD_CURSTT.columns

### [출력] STD_BD_DAT_GRD_CURSTT

# expdf = STD_BD_DAT_GRD_CURSTT
# table_nm = 'STD_BD_DAT_GRD_CURSTT'.upper()

# # 테이블 생성
# try:
#     sql = 'create table ' + table_nm + '( \n'

#     for idx,column in enumerate(expdf.columns):
#         # if 'float' in expdf[column].dtype.name:
#         #     sql += column + ' float'
#         # elif 'int' in expdf[column].dtype.name:
#         #     sql += column + ' number'
#         # else:
#         sql += column + ' varchar(255)'

#         if len(expdf.columns) - 1 != idx:
#             sql += ','
#         sql += '\n'
#     sql += ')'    
#     we.execute(sql)
#     # 데이터 추가
#     we.import_from_pandas(expdf, table_nm)
# except:
#     # 데이터 추가
#     we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_GRD_CURSTT')

## 내연기관차 감소추이
grp1 = dfm2dgl.groupby(['배출가스등급', '연료'])['차대번호'].count().reset_index()
grp1 = grp1.rename(columns={'차대번호':'차량대수'})

year = '2022'
month = '12'
today_date = datetime.today().strftime("%Y%m%d")
# year = today_date[:4]
# month = today_date[4:6]
grp1[['연도', '월']] = [year, month]

yr_list, month_list, grd_list, fuel_list = [], [], [], []
for grd in ['1', '2', '3', '4', '5', 'X']:
    for fuel in grp1['연료'].unique():
        for yr in range(2019, int(year) + 1):
            for month in range(1, 13):
                yr_list.append(str(yr))
                month_list.append(f'{month:0>2}')
                grd_list.append(grd)
                fuel_list.append(fuel)
base = pd.DataFrame({'연도':yr_list, '월':month_list, '배출가스등급':grd_list, '연료':fuel_list})

grp2 = dfm2dgl.groupby(['최초등록일자_년', '최초등록일자_월', '배출가스등급', '연료'])['차대번호'].count().reset_index()
grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '최초등록일자_월':'월', '차대번호':'등록대수'})
grp3 = errc2dgl.groupby(['변경일자_년', '변경일자_월', '배출가스등급', '연료'])['차대번호'].count().reset_index()
grp3 = grp3.rename(columns={'변경일자_년':'연도', '변경일자_월':'월', '차대번호':'말소대수'})

base1 = base.merge(grp1, on=['연도', '월', '배출가스등급', '연료'], how='left')
base2 = base1.merge(grp2, on=['연도', '월', '배출가스등급', '연료'], how='left')
base3 = base2.merge(grp3, on=['연도', '월', '배출가스등급', '연료'], how='left')
base3 = base3.sort_values(['배출가스등급', '연료', '연도', '월']).reset_index(drop=True)
base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

n = len(base3['연도'].unique()) * len(base3['월'].unique())
for i in range(base3.shape[0] // n):
    for j in range(2, n+1):
        base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

pred_grd_list, pred_fuel_list, pred_yr_list, pred_month_list = [], [], [], []
for grd in ['1', '2', '3', '4', '5', 'X']:
    for fuel in grp1['연료'].unique():
        for yr in range(int(year) + 1, 2036):
            for month in range(1, 13):
                pred_grd_list.append(grd)
                pred_fuel_list.append(fuel)
                pred_yr_list.append(str(yr))
                pred_month_list.append(f'{month:0>2}')
pred_df = pd.DataFrame({'연도':pred_yr_list, '연료':pred_fuel_list, '월':pred_month_list, '배출가스등급':pred_grd_list})
total_base = pd.concat([base3, pred_df], ignore_index=True)
total_base = total_base.sort_values(['배출가스등급', '연료', '연도', '월']).reset_index(drop=True)

total_pred_df = pd.DataFrame()
for fuel in total_base['연료'].unique():
    for grd in total_base['배출가스등급'].unique():
        temp = total_base.loc[(total_base['연료'] == fuel) & (total_base['배출가스등급'] == grd)].reset_index(drop=True).reset_index()
        present = temp[temp['연도'] <= year]
        future = temp.loc[temp['연도'] > year, ['index', '연도', '월', '배출가스등급', '연료']]
        if fuel == 'LPG(액화석유가스)':
            fuel_mod = 'LPG'
        else:
            fuel_mod = fuel
        present = present.rename(columns={'차량대수':f"{fuel_mod}_대수"})
        # 선형예측
        a, b = np.polyfit(present['index'], present[f"{fuel_mod}_대수"], 1)
        future[f'{fuel_mod}_예측'] = a * future['index'] + b
        # BSpline 예측
        spl = intp.BSpline(present['index'], present[f"{fuel_mod}_대수"], 1, extrapolate=True)
        future[f'{fuel_mod}_예측_BSpline'] = spl(future['index'])
        # Akima 예측
        aki = intp.Akima1DInterpolator(present['index'], present[f"{fuel_mod}_대수"])
        future[f'{fuel_mod}_예측_Akima'] = aki(future['index'], extrapolate=True)
        temp2 = pd.concat([present, future], ignore_index=True)
        total_pred_df = pd.concat([total_pred_df, temp2], ignore_index=True)       

# total_pred_df.head()

# total_pred_df.tail()

df5 = total_pred_df[[
    '연도',
    '월',
    '배출가스등급',
    '연료',
    'LPG_대수',
    'LPG_예측',
    'LPG_예측_BSpline',
    'LPG_예측_Akima',
    '경유_대수',
    '경유_예측',
    '경유_예측_BSpline',
    '경유_예측_Akima',
    '휘발유_대수',
    '휘발유_예측',
    '휘발유_예측_BSpline',
    '휘발유_예측_Akima'
]]

# 음수 0으로 처리
df5.loc[df5['경유_예측'] < 0, '경유_예측'] = 0
df5.loc[df5['경유_예측_BSpline'] < 0, '경유_예측_BSpline'] = 0
df5.loc[df5['경유_예측_Akima'] < 0, '경유_예측_Akima'] = 0
df5.loc[df5['휘발유_예측'] < 0, '휘발유_예측'] = 0
df5.loc[df5['휘발유_예측_BSpline'] < 0, '휘발유_예측_BSpline'] = 0
df5.loc[df5['휘발유_예측_Akima'] < 0, '휘발유_예측_Akima'] = 0
df5.loc[df5['LPG_예측'] < 0, 'LPG_예측'] = 0
df5.loc[df5['LPG_예측_BSpline'] < 0, 'LPG_예측_BSpline'] = 0
df5.loc[df5['LPG_예측_Akima'] < 0, 'LPG_예측_Akima'] = 0

# 첫째자리에서 반올림
df5[['경유_대수', '휘발유_대수', 'LPG_대수', '경유_예측', '경유_예측_BSpline','경유_예측_Akima', '휘발유_예측', '휘발유_예측_BSpline', '휘발유_예측_Akima', 'LPG_예측', 'LPG_예측_BSpline', 'LPG_예측_Akima']] = df5[['경유_대수', '휘발유_대수', 'LPG_대수', '경유_예측', '경유_예측_BSpline','경유_예측_Akima', '휘발유_예측', '휘발유_예측_BSpline', '휘발유_예측_Akima', 'LPG_예측', 'LPG_예측_BSpline', 'LPG_예측_Akima']].round(0)

# 분기 정보 추가
df6 = df5.loc[(df5['월'] == '03') | (df5['월'] == '06') | (df5['월'] == '09') | (df5['월'] == '12')]
df6.loc[df6['월'] == '03' , '분기'] = '1'
df6.loc[df6['월'] == '06' , '분기'] = '2'
df6.loc[df6['월'] == '09' , '분기'] = '3'
df6.loc[df6['월'] == '12' , '분기'] = '4'

df6 = df6[[
    '연도',
    '분기', 
    '배출가스등급',
    '연료',
    'LPG_대수',
    'LPG_예측',
    'LPG_예측_BSpline',
    'LPG_예측_Akima',
    '경유_대수',
    '경유_예측',
    '경유_예측_BSpline',
    '경유_예측_Akima',
    '휘발유_대수',
    '휘발유_예측',
    '휘발유_예측_BSpline',
    '휘발유_예측_Akima',
]]

today_date = datetime.today().strftime("%Y%m%d")
df6['테이블생성일자'] = today_date
cdict = {
    '연도':'YR',
    '분기':'QRT',
    '배출가스등급':'EXHST_GAS_GRD_CD',
    '연료':'FUEL_CD',
    'LPG_대수':'LPG_MKCNT',
    'LPG_예측':'LPG_PRET',
    'LPG_예측_BSpline':'LPG_PRET_BSPLN',
    'LPG_예측_Akima':'LPG_PRET_AKM',
    '경유_대수':'DSL_MKCNT',
    '경유_예측':'DSL_PRET',
    '경유_예측_BSpline':'DSL_PRET_BSPLN',
    '경유_예측_Akima':'DSL_PRET_AKM',
    '휘발유_대수':'GSL_MKCNT',
    '휘발유_예측':'GSL_PRET',
    '휘발유_예측_BSpline':'GSL_PRET_BSPLN',
    '휘발유_예측_Akima':'GSL_PRET_AKM',
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_FUEL_CAR_DEC = df6.rename(columns=cdict)

# STD_BD_DAT_FUEL_CAR_DEC.columns

### [출력] STD_BD_DAT_FUEL_CAR_DEC

# expdf = STD_BD_DAT_FUEL_CAR_DEC
# table_nm = 'STD_BD_DAT_FUEL_CAR_DEC'.upper()

# # 테이블 생성
# try:
#     sql = 'create or replace table ' + table_nm + '( \n'

#     for idx,column in enumerate(expdf.columns):
#         # if 'float' in expdf[column].dtype.name:
#         #     sql += column + ' float'
#         # elif 'int' in expdf[column].dtype.name:
#         #     sql += column + ' number'
#         # else:
#         sql += column + ' varchar(255)'

#         if len(expdf.columns) - 1 != idx:
#             sql += ','
#         sql += '\n'
#     sql += ')'    
#     we.execute(sql)
#     we.import_from_pandas(expdf, table_nm)
# except:
#     # 데이터 추가
#     # 7s
#     we.import_from_pandas(expdf, table_nm)
    
# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_FUEL_CAR_DEC')

## 3-1 code end ##################################################################

## 3-2 start

## 등록정보(STD_CEG_CAR_MIG) 5등급만
# 8.6s
car = wd.export_to_pandas("SELECT VIN, BSPL_STDG_CD, VHCL_ERSR_YN, MANP_MNG_NO, YRIDNW, VHCTY_CD, PURPS_CD2, FRST_REG_YMD, VHCL_FBCTN_YMD, VHCL_MNG_NO FROM STD_CEG_CAR_MIG WHERE EXHST_GAS_GRD_CD = 'A0505' OR EXHST_GAS_GRD_CD = 'A05T5';")

# car.info()

car_ch_col = {
    'VIN':'차대번호', 
    'BSPL_STDG_CD':'법정동코드', 
    'VHCL_ERSR_YN':'차량말소YN',
    'MANP_MNG_NO':'제원관리번호',
    'YRIDNW':'차량연식', 
    'VHCTY_CD':'차종', 
    'PURPS_CD2':'용도', 
    'FRST_REG_YMD':'최초등록일자',
    'VHCL_FBCTN_YMD':'제작일자',
    'VHCL_MNG_NO':'차량관리번호'
}
carr = car.rename(columns=car_ch_col)

## 중복 차대번호 제거
carr['최초등록일자'] = pd.to_numeric(carr['최초등록일자'], errors='coerce')
carr = carr.sort_values('최초등록일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

## 차종 코드 변환
cd_dict = {
    'A31M':'이륜', 
    'A31P':'승용', 
    'A31S':'특수', 
    'A31T':'화물', 
    'A31V':'승합'
}
carr['차종'] = carr['차종'].replace(cd_dict)

## 용도 코드 변환
purps_dict = {
    'A08P':'개인용', 
    'A08B':'영업용', 
    'A08O':'관용',
}
carr['용도'] = carr['용도'].replace(purps_dict)

## 등록정보 말소 제거
carm = carr[carr['차량말소YN'] == 'N'].reset_index(drop=True)

## 등록&제원 병합
# 19.4s
cs = carm.merge(srcr, on='제원관리번호', how='left')

## 등록&제원&저감이력 병합
# 1.7s
csa = cs.merge(attr[['차대번호', 'DPF_YN']], on='차대번호', how='left')

csa['법정동코드'] = csa['법정동코드'].astype('str')
csa['법정동코드'] = csa['법정동코드'].str[:5] + '00000'
csa['법정동코드'] = pd.to_numeric(csa['법정동코드'])

## 지역정보 병합
csac = csa.merge(coder, on='법정동코드', how='left')

# csac['시도'].isnull().sum()

# ### 매칭 안되는 지역 처리
# # 주소 수정
# csac.loc[csac['법정동코드'] == 5172035031, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 강원특별자치도 홍천군
# csac.loc[csac['법정동코드'] == 5180031023, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csac.loc[csac['법정동코드'] == 5180031031, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csac.loc[csac['법정동코드'] == 5172035030, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csac.loc[csac['법정동코드'] == 5180031028, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 

# csac.loc[csac['법정동코드'] == 5172035021, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csac.loc[csac['법정동코드'] == 5180031025, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csac.loc[csac['법정동코드'] == 4165052000, ['시도', '시군구']] = ['경기도', '포천시'] # 경기도 포천시 선단동
# csac.loc[csac['법정동코드'] == 5172035023, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csac.loc[csac['법정동코드'] == 5180031027, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 

# csac.loc[csac['법정동코드'] == 5172035024, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csac.loc[csac['법정동코드'] == 5175037022, ['시도', '시군구']] = ['강원특별자치도', '영월군'] # 
# csac.loc[csac['법정동코드'] == 4163055000, ['시도', '시군구']] = ['경기도', '양주시'] # 경기도 양주시 회천3동
# csac.loc[csac['법정동코드'] == 5180031033, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 경기도 양주시 회천3동

## 조기폐차 정보들 병합
elp = pd.concat([aear, lgvr], ignore_index=True)
elpm = elp.sort_values('조기폐차최종승인YN', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)
elpm = elpm[elpm['조기폐차최종승인YN'] == 'Y'].reset_index(drop=True)

## 조기폐차 정보 추가
df = csac.merge(elpm, on='차대번호', how='left')

## 비상시 및 계절제 병합
# 41.3s
isdp= isdisr.merge(isper, on='적발번호', how='left')

# 9.5s
isdpi = isdp.merge(isisr, on='발령번호', how='left')

### 지역정보 추가
coder_dup = coder.sort_values('법정동코드', ascending=True).drop_duplicates(['시도코드', '시군구코드']).reset_index(drop=True)

# isdpi.columns

# 28.4s
is_total1 = isdpi.merge(coder_dup[['시도코드', '시군구코드', '시도', '시군구']], left_on=['등록시도코드', '등록시군구코드'], right_on=['시도코드', '시군구코드'], how='left')
is_total1 = is_total1.drop(['시도코드', '시군구코드'], axis=1)
is_total1 = is_total1.rename(columns={'시도':'등록시도', '시군구':'등록시군구'})

# 1m 19.2s
is_total1['적발지역코드'] = is_total1['적발지역코드'].astype('str')
is_total1['적발시도코드'] = is_total1['적발지역코드'].str[:2]
is_total1['적발시군구코드'] = is_total1['적발지역코드'].str[2:5]
is_total1[['적발시도코드', '적발시군구코드']] = is_total1[['적발시도코드', '적발시군구코드']].astype('int')

# 16.5s
is_total = is_total1.merge(coder_dup[['시도코드', '시군구코드', '시도', '시군구']], left_on=['적발시도코드', '적발시군구코드'], right_on=['시도코드', '시군구코드'], how='left')

# 1m 12.8s
is_total = is_total.drop(['시도코드', '시군구코드'], axis=1)
is_total = is_total.rename(columns={'시도':'적발시도', '시군구':'적발시군구'})

# !!! 수정(2023.09.01)
# 30s
is_total.loc[(is_total['적발시도'] == '서울특별시') | (is_total['적발시도'] == '경기도') | (is_total['적발시도'] == '인천광역시'), '적발지역'] = '수도권'
is_total.loc[is_total['적발지역'].isnull(), '적발지역'] = '수도권외'

## 상시 병합
# 1s
usdp = usdisr.merge(usper, on='번호', how='left')

### 지역정보 추가
us_total1 = usdp.merge(coder_dup, left_on=['등록시도코드', '등록시군구코드'], right_on=['시도코드', '시군구코드'], how='left')
us_total1 = us_total1.drop(['시도코드', '시군구코드'], axis=1) # !!! 수정(2023.09.01)
us_total1 = us_total1.rename(columns={'시도':'등록시도', '시군구':'등록시군구'}) # !!! 수정(2023.09.01)

# !!! 수정(2023.09.01)
# 4s
us_total1['단속지역코드'] = us_total1['단속지역코드'].astype('str')
us_total1['단속시도코드'] = us_total1['단속지역코드'].str[:2]
us_total1['단속시군구코드'] = us_total1['단속지역코드'].str[2:5]
us_total1[['단속시도코드', '단속시군구코드']] = us_total1[['단속시도코드', '단속시군구코드']].astype('int')

# !!! 수정(2023.09.01)
# 2s
us_total = us_total1.merge(coder_dup[['시도코드', '시군구코드', '시도', '시군구']], left_on=['단속시도코드', '단속시군구코드'], right_on=['시도코드', '시군구코드'], how='left')
us_total = us_total.drop(['시도코드', '시군구코드'], axis=1)
us_total = us_total.rename(columns={'시도':'단속시도', '시군구':'단속시군구'})

# !!! 수정(2023.09.01)
# 1s
us_total.loc[(us_total['단속시도'] == '서울특별시') | (us_total['단속시도'] == '경기도') | (us_total['단속시도'] == '인천광역시'), '단속지역'] = '수도권'
# us_total.loc[us_total['단속지역'].isnull(), '단속지역'] = '수도권외'

## 등록(말소 유지) & 제원 병합
# 10.3s
cse = carr.merge(srcr, on='제원관리번호', how='left')

## 등록&제원&이력 병합
# 2m 6.0s
ersr = cse.merge(hisr, on='차량관리번호', how='left')

# 1. 등록 차량말소와 등록이력 차량 말소 둘 모두 해당되는 데이터 추출
ersr = ersr.loc[(ersr['차량말소YN_x'] == 'Y') & (ersr['차량말소YN_y'] == 'Y')].reset_index(drop=True)

# 2. 변경일자 기준 최신 데이터만 남기고 차대번호 중복 제거
ersr = ersr.sort_values('변경일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

# 3. 변경일자 2019.01.01 이후만 추출
ersr = ersr[ersr['변경일자'] >= 20190101].reset_index(drop=True)

ersr['법정동코드'] = ersr['법정동코드'].astype('str')
ersr['법정동코드'] = ersr['법정동코드'].str[:5] + '00000'
ersr['법정동코드'] = pd.to_numeric(ersr['법정동코드'])

## 등록&제원&이력&법정동코드 병합(errc)
# 매칭 후 '시도' 빈값 없음
errc = ersr.merge(coder, on='법정동코드', how='left')

# errc['시도'].isnull().sum()

## Load

### 5등급 지역별 조기폐차(STD_BD_GRD5_ELPDSRC_CURSTT)(한글파일 내용 입력)

# 0s
df1 = we.export_to_pandas("SELECT * FROM STD_BD_GRD5_ELPDSRC;")

# df1.info()

### 5등급 저공해 미조치(STD_BD_GRD5_LEM_N_MOD)(한글파일 내용 입력)

# 0s
no_dpf = we.export_to_pandas("SELECT * FROM STD_BD_GRD5_LEM_N;")

# no_dpf.info()

# 분석
## 5등급 지역별 조기폐차 현황
# dfm = df.copy()
# dfm['최초등록일자'] = dfm['최초등록일자'].astype('str')
# dfm['최초등록일자_년'] = dfm['최초등록일자'].str[:4]
# dfm['최초등록일자_월'] = dfm['최초등록일자'].str[4:6]
# dfm['최초등록일자_일'] = dfm['최초등록일자'].str[6:8]
# errc['변경일자'] = errc['변경일자'].astype('str')
# errc['변경일자_년'] = errc['변경일자'].str[:4]
# errc['변경일자_월'] = errc['변경일자'].str[4:6]
# errc['변경일자_일'] = errc['변경일자'].str[6:8]

# ## 시도, 연도별 차량 현황 분석
# # 2022년 차량 대수
# grp1 = dfm.groupby(['시도'], as_index=False)['차대번호'].count()
# grp1 = grp1.rename(columns={'차대번호':'차량대수'})
# year = 2022
# year = int(datetime.today().strftime("%Y"))
# grp1['연도'] = f'{year}'
# grp1 = grp1[['연도', '시도', '차량대수']]

# # 차량 통계 기본 데이터셋
# yr_list = []
# ctpv_list = []
# for ctpv in grp1['시도'].unique():
#     for yr in range(year - 3, year + 1): # !!! 수정(2023.08.31)
#         yr_list.append(str(yr))
#         ctpv_list.append(ctpv)
# base = pd.DataFrame({'연도':yr_list, '시도':ctpv_list})

# # 연도별 등록대수
# grp2 = dfm.groupby(['최초등록일자_년', '시도'], as_index=False)['차대번호'].count()
# grp2 = grp2.rename(columns={'최초등록일자_년':'연도', '차대번호':'등록대수'})

# # 연도별 말소대수
# grp3 = errc.groupby(['변경일자_년', '시도'], as_index=False)['차대번호'].count()
# grp3 = grp3.rename(columns={'변경일자_년':'연도', '차대번호':'말소대수'})
# base1 = base.merge(grp1, on=['연도', '시도'], how='left')
# base2 = base1.merge(grp2, on=['연도', '시도'], how='left')
# base3 = base2.merge(grp3, on=['연도', '시도'], how='left')
# base3[['차량대수', '등록대수', '말소대수']] = base3[['차량대수', '등록대수', '말소대수']].fillna(0)

# n = len(base3['연도'].unique())
# for i in range(base3.shape[0] // n):
#     for j in range(2, n+1):
#         base3.loc[(i+1)*n - j, '차량대수'] = base3.loc[(i+1)*n - (j-1), '차량대수'] + base3.loc[(i+1)*n - (j-1), '말소대수'] - base3.loc[(i+1)*n - (j-1), '등록대수']

# dfm['말소일자'] = dfm['말소일자'].astype('str')
# dfm['말소일자_년'] = dfm['말소일자'].str[:4]
# dfm['말소일자_월'] = dfm['말소일자'].str[4:6]
# dfm['말소일자_일'] = dfm['말소일자'].str[6:8]
# grp4 = dfm.loc[dfm['조기폐차최종승인YN'] == 'Y'].groupby(['말소일자_년'], as_index=False)['차대번호'].count()
# grp4 = grp4.rename(columns={'말소일자_년':'연도', '차대번호':'조기폐차대수'})
# base4 = base3.merge(grp4, on='연도', how='left')
# base4['조기폐차대수'] = base4['조기폐차대수'].fillna(0)
# base4 = base4.drop(['등록대수', '말소대수'], axis=1)

# n = len(base4['연도'].unique())
# for i in range(base4.shape[0] // n):
#     for j in range(n-1):
#         base4.loc[i*4 + j+1, '감소대수'] = base4.loc[i*4 + j, '차량대수'] - base4.loc[i*4 + j+1, '차량대수']
# base4['자연감소대수'] = base4['감소대수'] - base4['조기폐차대수']

today_date = datetime.today().strftime("%Y%m%d")
df1['LOAD_DT'] = today_date
STD_BD_GRD5_ELPDSRC_CURSTT = df1.copy()

# STD_BD_GRD5_ELPDSRC_CURSTT.columns

### [출력] [D] STD_BD_GRD5_ELPDSRC_CURSTT

# expdf = STD_BD_GRD5_ELPDSRC_CURSTT
# table_nm = 'STD_BD_GRD5_ELPDSRC_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD5_ELPDSRC_CURSTT')

## 5등급 지역별 저공해미조치 차량현황
today_date = datetime.today().strftime("%Y%m%d")
no_dpf['테이블생성일자'] = today_date
cdict = {
    '테이블생성일자':'LOAD_DT',
    '지역':'RGN',
    '구분':'SEASON',
    '차량대수':'VHCL_MKCNT', 
}
STD_BD_GRD5_LEM_N_MOD = no_dpf.rename(columns=cdict)

# STD_BD_GRD5_LEM_N_MOD.columns

### [출력] [D] STD_BD_GRD5_LEM_N_MOD

# expdf = STD_BD_GRD5_LEM_N_MOD
# table_nm = 'STD_BD_GRD5_LEM_N_MOD'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD5_LEM_N_MOD')

## 차대번호별 운행제한 적발 현황
# DNSTY_STDR_ID(농도기준아이디) : 실발령(C011), 모의발령(C012)
# TY_STDR_ID(유형기준아이디) : 비상시(T001), 계절제(T002)
is_season = is_total.loc[(is_total['농도기준아이디'] == 'C011') & (is_total['유형기준아이디'] == 'T002')].reset_index(drop=True)
today_date = datetime.today().strftime("%Y%m%d")

# 계절제 1차(2019.12 ~ 2020.3)
# 계절제 2차(2020.12 ~ 2021.3)
# 계절제 3차(2021.12 ~ 2022.3)
# 계절제 4차(2022.12 ~ 2023.3)
for yr in range(2019, int(today_date[:4])):
    start_date = f'{yr}1130'
    end_date = f'{yr+1}0401'
    is_season.loc[(is_season['단속일'] > int(start_date)) & (is_season['단속일'] < int(end_date)), f'계절제_{yr-2018}차여부'] = 'Y'

agg_dict = {x:'count' for x in is_season.columns if '계절제' in x}
limit_season_rename_dict = {x:x.replace('여부','') for x in agg_dict.keys()}

limit_season = is_season.groupby(['차대번호'], as_index=False).agg(agg_dict)
limit_season = limit_season.rename(columns=limit_season_rename_dict)

# 11.0s
# DNSTY_STDR_ID(농도기준아이디) : 실발령(C011), 모의발령(C012)
# TY_STDR_ID(유형기준아이디) : 비상시(T001), 계절제(T002)
is_high = is_total.loc[(is_total['농도기준아이디'] == 'C011') & (is_total['유형기준아이디'] == 'T001')].reset_index(drop=True)
limit_high = is_high.groupby(['차대번호'], as_index=False).agg({'단속일':'count'})
limit_high = limit_high.rename(columns={'단속일':'비상시'})

limit_alw = us_total.groupby('차대번호', as_index=False).agg({'적발건수':'sum'})
limit_alw = limit_alw.rename(columns={'적발건수':'상시'})
limit_sh = limit_season.merge(limit_high, on='차대번호', how='left')
limit = limit_sh.merge(limit_alw, on='차대번호', how='left')

limit.iloc[:, 1:] = limit.iloc[:, 1:].fillna(0)
limit['비상시'] = limit['비상시'].astype('int')
limit['상시'] = limit['상시'].astype('int')

lmt1 = df.merge(limit, on='차대번호', how='left')
lmt1.loc[(lmt1['시도'] == '서울특별시') | (lmt1['시도'] == '경기도') | (lmt1['시도'] == '인천광역시'), '지역'] = '수도권'
lmt1['지역'] = lmt1['지역'].fillna('수도권외')
lmt1['DPF_YN'] = lmt1['DPF_YN'].fillna('무')

season_start_date = datetime(2020, 12, 1)
season_end_date = datetime(2021, 3, 31)
days = (season_end_date - season_start_date).days
for one in [x for x in limit_season_rename_dict.values()]:
    lmt1[one + '_일평균'] = lmt1[one] / days

today_date = datetime.today().strftime("%Y%m%d")
lmt1['테이블생성일자'] = today_date

# season_col = ['테이블생성일자', '차대번호'] + ['지역', '시도', 'DPF_YN', '차종', '차종유형'] + [x for x in limit_season_rename_dict.values()]
# lmt1[[x for x in limit_season_rename_dict.values()] + [x + '_일평균' for x in limit_season_rename_dict.values()]] = lmt1[[x for x in limit_season_rename_dict.values()] + [x + '_일평균' for x in limit_season_rename_dict.values()]].fillna(0)
# season = lmt1[season_col]
# cdict = {
#     '테이블생성일자':'LOAD_DT', 
#     '차대번호':'VIN', 
#     '지역':'RGN', 
#     '시도':'CTPV', 
#     'DPF_YN':'DPF_EXTRNS_YN', 
#     '차종':'VHCTY_CD', 
#     '차종유형':'VHCTY_TY', 
# }
# for one in limit_season_rename_dict.values():
#     cdict[one] = one.replace('계절제', 'SEASON').replace('차', 'ODR_CRDN_NOCS')

# !!! 수정(2023.09.01)
ss_df = is_season.merge(df, on='차대번호', how='left')
ss_df['DPF_YN'] = ss_df['DPF_YN'].fillna('무')
ss_df['DPF_YN'].value_counts(dropna=False)
ss_df['테이블생성일자'] = today_date
season_col = ['테이블생성일자', '차대번호'] + ['적발지역', '적발시도', 'DPF_YN', '차종', '차종유형'] + [x for x in limit_season_rename_dict.keys()]
season = ss_df[season_col]
cdict = {
    '테이블생성일자':'LOAD_DT', 
    '차대번호':'VIN', 
    '적발지역':'DSCL_RGN', # !!! 수정(2023.09.01)
    '적발시도':'DSCL_CTPV', # !!! 수정(2023.09.01)
    'DPF_YN':'DPF_EXTRNS_YN', 
    '차종':'VHCTY_CD', 
    '차종유형':'VHCTY_TY', 
}
for one in limit_season_rename_dict.keys():
    cdict[one] = one.replace('계절제', 'SEASON').replace('차여부', 'ODR_CRDN_YN') # !!! 수정(2023.09.01)

STD_BD_SEASON_CRDN_NOCS_CURSTT = season.rename(columns=cdict)

# STD_BD_SEASON_CRDN_NOCS_CURSTT.columns

### [출력] STD_BD_SEASON_CRDN_NOCS_CURSTT

# expdf = STD_BD_SEASON_CRDN_NOCS_CURSTT
# table_nm = 'STD_BD_SEASON_CRDN_NOCS_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_SEASON_CRDN_NOCS_CURSTT')

## 계절제별 적발건수
season_tot = lmt1[[x + '_일평균' for x in limit_season_rename_dict.values()]].sum().reset_index()
season_tot = season_tot.rename(columns={'index':'계절제차수', 0:'일평균적발건수'})
season_tot['계절제차수'] = season_tot['계절제차수'].str.replace('계절제_', '').str.replace('_일평균', '')
today_date = datetime.today().strftime("%Y%m%d")
season_tot['테이블생성일자'] = today_date
cdict = {
    '계절제차수':'SEASON_ORD', 
    '일평균적발건수':'DY_AVRG_CRDN_NOCS', 
    '테이블생성일자':'LOAD_DT', 
    }
STD_BD_SEASON_DY_AVRG_CRDN_NOCS = season_tot.rename(columns=cdict)

# STD_BD_SEASON_DY_AVRG_CRDN_NOCS.columns

### [출력] STD_BD_SEASON_DY_AVRG_CRDN_NOCS

# expdf = STD_BD_SEASON_DY_AVRG_CRDN_NOCS
# table_nm = 'STD_BD_SEASON_DY_AVRG_CRDN_NOCS'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_SEASON_DY_AVRG_CRDN_NOCS')

## 지역별 상시운행제한 단속 현황
# us_total2 = us_total.merge(limit_alw, on='차대번호', how='left')
# us_total2.loc[(us_total2['시도'] == '서울특별시') | (us_total2['시도'] == '경기도') | (us_total2['시도'] == '인천광역시'), '지역'] = '수도권'
# us_total2['지역'] = us_total2['지역'].fillna('수도권외')
# us_total2['적발년월'] = us_total2['적발년월'].astype('str')
# us_total2['적발년월_년'] = us_total2['적발년월'].str[:4]
# us_total2 = us_total2.sort_values('적발년월_년', ascending=True).drop_duplicates('차대번호').reset_index(drop=True)
# us_total2 = us_total2.drop(['적발건수'], axis=1)
# us_total2 = us_total2.rename(columns={'적발년월_년':'적발년도', '상시':'적발건수'})

# orditm = us_total2.loc[(us_total2['적발건수'] > 0)& (us_total2['적발년도'].isnull() == False), [
#     '차대번호',
#     '적발년도',
#     '적발건수',
#     '지역',
#     '시도',
# ]]

# today_date = datetime.today().strftime("%Y%m%d")
# orditm['테이블생성일자'] = today_date
# cdict = {
#     '테이블생성일자':'LOAD_DT', 
#     '차대번호':'VIN', 
#     '적발년도':'DSCL_YR', 
#     '적발건수':'DSCL_NOCS', 
#     '지역':'RGN',
#     '시도':'CTPV', 
# }
# STD_BD_ORDITM_DSCL_CURSTT = orditm.rename(columns=cdict)

# !!! 수정(2023.09.01)
## 지역별 상시운행제한 단속 현황
us_total2 = us_total.merge(limit_alw, on='차대번호', how='left')
us_total2['적발년월'] = us_total2['적발년월'].astype('str')
us_total2['적발년월_년'] = us_total2['적발년월'].str[:4]
us_total2 = us_total2.sort_values('적발년월_년', ascending=True).drop_duplicates('차대번호').reset_index(drop=True)
us_total2 = us_total2.drop(['적발건수'], axis=1)
us_total2 = us_total2.rename(columns={'적발년월_년':'적발년도', '상시':'적발건수'})

orditm = us_total2.loc[(us_total2['적발건수'] > 0)& (us_total2['적발년도'].isnull() == False), [
    '차대번호',
    '적발년도',
    '적발건수',
    '단속지역', # !!! 수정(2023.09.01)
    '단속시도', # !!! 수정(2023.09.01)
]]

today_date = datetime.today().strftime("%Y%m%d")
orditm['테이블생성일자'] = today_date
cdict = {
    '테이블생성일자':'LOAD_DT', 
    '차대번호':'VIN', 
    '적발년도':'DSCL_YR', 
    '적발건수':'DSCL_NOCS', 
    '단속지역':'DSCL_RGN', # !!! 수정(2023.09.01)
    '단속시도':'DSCL_CTPV', # !!! 수정(2023.09.01)
}
STD_BD_ORDITM_DSCL_CURSTT = orditm.rename(columns=cdict)

# STD_BD_ORDITM_DSCL_CURSTT.columns

### [출력] STD_BD_ORDITM_DSCL_CURSTT

# expdf = STD_BD_ORDITM_DSCL_CURSTT
# table_nm = 'STD_BD_ORDITM_DSCL_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_ORDITM_DSCL_CURSTT')

## 적발지역별 계절제 단속 현황 출력
is_season2 = is_season.sort_values(['적발시도', '적발시군구']).drop_duplicates(['차대번호', '적발시도', '적발시군구']).reset_index(drop=True)
is_lmt = is_season2.merge(limit_season, on='차대번호', how='left')
# !!! 수정(2023.09.01)
# is_lmt.loc[(is_lmt['적발시도'] == '서울특별시') | (is_lmt['적발시도'] == '경기도') | (is_lmt['적발시도'] == '인천광역시'), '적발지역'] = '수도권'
# is_lmt['적발지역'] = is_lmt['적발지역'].fillna('수도권외')
# is_lmt.loc[is_lmt['등록시도'] == '강원도', '등록시도'] = '강원특별자치도'

is_lmt['적발시도코드'] = is_lmt['적발지역코드'].str[:2] # !!! 수정(2023.09.01)

is_lmt2 = is_lmt[[
    '적발지역',
    '적발시도',
    '적발시도코드',  # !!! 수정(2023.09.01)
    '등록시도',  # !!! 수정(2023.09.01)
    '차대번호', 
    '계절제_1차', 
    '계절제_2차', 
    '계절제_3차', 
    '계절제_4차', 
]]

dfm = df.sort_values('최초등록일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

slimit = is_lmt2.merge(dfm[['차대번호', '차종', '차종유형']], on='차대번호', how='left')
today_date = datetime.today().strftime("%Y%m%d")

# 시도명 2글자로 수정
slimit['적발시도'] = slimit['적발시도'].map({'경기도':'경기', '대구광역시':'대구', '부산광역시':'부산', '서울특별시':'서울', '인천광역시':'인천'})

# 건수 0 -> nan
slimit.loc[slimit['계절제_1차'] == 0, '계절제_1차'] = np.nan
slimit.loc[slimit['계절제_2차'] == 0, '계절제_2차'] = np.nan
slimit.loc[slimit['계절제_3차'] == 0, '계절제_3차'] = np.nan
slimit.loc[slimit['계절제_4차'] == 0, '계절제_4차'] = np.nan

slimit['테이블생성일자'] = today_date
cdict = {
    '테이블생성일자':'LOAD_DT', 
    '적발지역':'DSCL_RGN', 
    '적발시도':'DSCL_CTPV', 
    '적발시도코드':'DSCL_CTPV_CD', # !!! 수정(2023.09.01)
    '등록시도':'REG_CTPV', 
    '차대번호':'VIN', 
    '계절제_1차':'SEASON_1ODR_CRDN_NOCS', 
    '계절제_2차':'SEASON_2ODR_CRDN_NOCS', 
    '계절제_3차':'SEASON_3ODR_CRDN_NOCS', 
    '계절제_4차':'SEASON_4ODR_CRDN_NOCS', 
    '차종':'VHCTY_CD',
    '차종유형':'VHCTY_TY', 
}
STD_BD_SEASON_DSCL_RGN_CURSTT = slimit.rename(columns=cdict)

# STD_BD_SEASON_DSCL_RGN_CURSTT.columns

### [출력] STD_BD_SEASON_DSCL_RGN_CURSTT

# expdf = STD_BD_SEASON_DSCL_RGN_CURSTT
# table_nm = 'STD_BD_SEASON_DSCL_RGN_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_SEASON_DSCL_RGN_CURSTT')

## 5등급 저감사업
ce = carr.merge(elpm, on='차대번호', how='left')
dfe = ce.merge(attr, on='차대번호', how='left')

dfe['최초등록일자'] = dfe['최초등록일자'].astype('str')
dfe['최초등록일자_년'] = dfe['최초등록일자'].str[:4]
dfe['최초등록일자_월'] = dfe['최초등록일자'].str[4:6]
dfe['최초등록일자_일'] = dfe['최초등록일자'].str[6:8]
dfe['말소일자_년'] = dfe['말소일자'].astype('str').str[:4]
dfe['말소일자_월'] = dfe['말소일자'].astype('str').str[4:6]
dfe['말소일자_일'] = dfe['말소일자'].astype('str').str[6:8]

errc['변경일자'] = errc['변경일자'].astype('str')
errc['변경일자_년'] = errc['변경일자'].str[:4]
errc['변경일자_월'] = errc['변경일자'].str[4:6]
errc['변경일자_일'] = errc['변경일자'].str[6:8]

ere = errc.merge(elpm, on='차대번호', how='left')
erea = ere.merge(attr, on='차대번호', how='left')

# 연도 설정
# year = '2022'
year = today_date[:4] # !!! 수정(2023.09.01)

dfe['연도'] = year

def knd1(x):
    if '1종' in x.unique():
        return x.value_counts()['1종']
    else:
        return 0
def knd2(x):
    if '1종+SCR' in x.unique():
        return x.value_counts()['1종+SCR']
    else:
        return 0

# 2022년 차량 대수
grp1 = dfe[dfe['차량말소YN'] == 'N'].groupby(['연도']).agg({'차대번호':'count', '저감장치구분':[knd1, knd2]}).reset_index()
grp1.columns = ['연도', '차량대수', '저감장치(1종)', '저감장치(1종+SCR)']

# 연도별 등록대수
grp2 = dfe[dfe['차량말소YN'] == 'N'].groupby(['최초등록일자_년']).agg({'차대번호':'count', '저감장치구분':[knd1, knd2]}).reset_index()
grp2.columns = ['연도', '등록대수', '등록저감장치(1종)', '등록저감장치(1종+SCR)']

# 연도별 말소대수
grp3 = erea.groupby('변경일자_년').agg({'차대번호':'count', '저감장치구분':[knd1, knd2]}).reset_index()
grp3.columns = ['연도', '말소대수', '말소저감장치(1종)', '말소저감장치(1종+SCR)']

# 연도별 조기폐차 대수
grp4 = dfe.groupby('말소일자_년').agg({'조기폐차최종승인YN':'count'}).reset_index()
grp4 = grp4.rename(columns={'말소일자_년':'연도', '조기폐차최종승인YN':'조기폐차'})

# 4년간 차량 통계 기본 데이터셋
yr_list = []
for yr in range(int(year) - 3, int(year) + 1): # !!! 수정(2023.08.31)
    yr_list.append(str(yr))
base = pd.DataFrame({'연도':yr_list})

base1 = base.merge(grp1, on='연도', how='left')
base2 = base1.merge(grp2, on='연도', how='left')
base3 = base2.merge(grp3, on='연도', how='left')
base4 = base3.merge(grp4, on='연도', how='left')

base4[['차량대수', '조기폐차', '저감장치(1종)', '저감장치(1종+SCR)', '등록대수', '등록저감장치(1종)', '등록저감장치(1종+SCR)', '말소대수', '말소저감장치(1종)', '말소저감장치(1종+SCR)']] = base4[['차량대수', '조기폐차', '저감장치(1종)', '저감장치(1종+SCR)', '등록대수', '등록저감장치(1종)', '등록저감장치(1종+SCR)', '말소대수', '말소저감장치(1종)', '말소저감장치(1종+SCR)']].fillna(0)

n = len(base4['연도'].unique())
for i in range(base4.shape[0] // n):
    for j in range(2, n+1):
        base4.loc[(i+1)*n - j, '차량대수'] = base4.loc[(i+1)*n - (j-1), '차량대수'] + base4.loc[(i+1)*n - (j-1), '말소대수'] - base4.loc[(i+1)*n - (j-1), '등록대수']
        base4.loc[(i+1)*n - j, '저감장치(1종)'] = base4.loc[(i+1)*n - (j-1), '저감장치(1종)'] + base4.loc[(i+1)*n - (j-1), '말소저감장치(1종)'] - base4.loc[(i+1)*n - (j-1), '등록저감장치(1종)']
        base4.loc[(i+1)*n - j, '저감장치(1종+SCR)'] = base4.loc[(i+1)*n - (j-1), '저감장치(1종+SCR)'] + base4.loc[(i+1)*n - (j-1), '말소저감장치(1종+SCR)'] - base4.loc[(i+1)*n - (j-1), '등록저감장치(1종+SCR)']

base5 = base4[['연도', '차량대수', '조기폐차', '저감장치(1종)', '저감장치(1종+SCR)']]
base5['감소대수'] = base5['차량대수'].shift() - base5['차량대수']
base5['자연감소'] = base5['감소대수'] - base5['조기폐차']
base5['미장착'] = base5['차량대수'] - base5['저감장치(1종)'] - base5['저감장치(1종+SCR)']

base5['테이블생성일자'] = today_date
base5 = base5[[
    '연도',
    '차량대수',
    '자연감소',
    '조기폐차',
    '저감장치(1종)',
    '저감장치(1종+SCR)',
    '미장착',
    '테이블생성일자'
    ]]
cdict = {
    '연도':'YR', 
    '차량대수':'VHCL_MKCNT', 
    '자연감소':'NTRL_DCLN', 
    '조기폐차':'ELPDSRC', 
    '저감장치(1종)':'RDCDVC_1KND', 
    '저감장치(1종+SCR)':'RDCDVC_1KND_SCR', 
    '미장착':'UNMNTNG', 
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_GRD5_REDUC_BIZ = base5.rename(columns=cdict)

# STD_BD_DAT_GRD5_REDUC_BIZ.columns

### [출력] STD_BD_DAT_GRD5_REDUC_BIZ

# expdf = STD_BD_DAT_GRD5_REDUC_BIZ
# table_nm = 'STD_BD_DAT_GRD5_REDUC_BIZ'.upper()

# # 테이블 생성
# try:
#     sql = 'create table ' + table_nm + '( \n'

#     for idx,column in enumerate(expdf.columns):
#         # if 'float' in expdf[column].dtype.name:
#         #     sql += column + ' float'
#         # elif 'int' in expdf[column].dtype.name:
#         #     sql += column + ' number'
#         # else:
#         sql += column + ' varchar(255)'

#         if len(expdf.columns) - 1 != idx:
#             sql += ','
#         sql += '\n'
#     sql += ')'    
#     we.execute(sql)
#     we.import_from_pandas(expdf, table_nm)
# except:
#     # 데이터 추가
#     # 5s
#     we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_GRD5_REDUC_BIZ')

## 운행제한현황
run_lmt1 = lmt1.copy()
run_lmt1.loc[run_lmt1['DPF_YN'] == '무', '저감장치미장착'] = '미장착'

total_grp_lmt = pd.DataFrame()
for one in limit_season_rename_dict.values():
    temp1 = run_lmt1.loc[run_lmt1[one] > 0, ['차대번호', '지역', '시도', '차종', '차종유형', '저감장치미장착'] + [one]]
    temp2 = run_lmt1.loc[run_lmt1[one] > 1, ['차대번호', '지역', '시도', '차종', '차종유형'] + [one]]
    if temp1.shape[0] > 0 and temp2.shape[0] > 0:
        grp1 = temp1.groupby(['지역', '시도', '차종', '차종유형']).agg({'차대번호':'count', '저감장치미장착':'count'}).reset_index()
        grp1 = grp1.rename(columns={'차대번호':'적발차량대수', '저감장치미장착':'저공해미조치'})
        grp2 = temp2.groupby(['지역', '시도', '차종', '차종유형'])['차대번호'].count().reset_index()
        grp2 = grp2.rename(columns={'차대번호':'중복적발대수'})
        grp = grp1.merge(grp2, on=['지역', '시도', '차종', '차종유형'], how='left')
        grp['계절관리제'] = one
        total_grp_lmt = pd.concat([total_grp_lmt, grp], ignore_index=True)
    else:
        pass

today_date = datetime.today().strftime("%Y%m%d")
total_grp_lmt['테이블생성일자'] = today_date
total_grp_lmt = total_grp_lmt[[
    '계절관리제', 
    '지역', 
    '시도', 
    '차종', 
    '차종유형', 
    '적발차량대수', 
    '저공해미조치', 
    '중복적발대수', 
    '테이블생성일자', 
]]
cdict = {
    '계절관리제':'SEASON', 
    '지역':'RGN', 
    '시도':'CTPV', 
    '차종':'VHCTY_CD', 
    '차종유형':'VHCTY_TY', 
    '적발차량대수':'DSCL_VHCL_MKCNT', 
    '저공해미조치':'UNLEM', 
    '중복적발대수':'DUP_DSCL_MKCNT', 
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_RUN_LMT_CURSTT = total_grp_lmt.rename(columns=cdict)

# STD_BD_DAT_RUN_LMT_CURSTT.columns

### [출력] STD_BD_DAT_RUN_LMT_CURSTT

# expdf = STD_BD_DAT_RUN_LMT_CURSTT
# table_nm = 'STD_BD_DAT_RUN_LMT_CURSTT'.upper()

# # 테이블 생성
# try:
#     sql = 'create table ' + table_nm + '( \n'

#     for idx,column in enumerate(expdf.columns):
#         # if 'float' in expdf[column].dtype.name:
#         #     sql += column + ' float'
#         # elif 'int' in expdf[column].dtype.name:
#         #     sql += column + ' number'
#         # else:
#         sql += column + ' varchar(255)'

#         if len(expdf.columns) - 1 != idx:
#             sql += ','
#         sql += '\n'
#     sql += ')'    
#     we.execute(sql)
#     we.import_from_pandas(expdf, table_nm)
# except:
#     # 데이터 추가
#     # 5s
#     we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_RUN_LMT_CURSTT')

## 3-2 code end ##################################################################

## 3-3 start

## 등록정보(STD_CEG_CAR_MIG) 4, 5등급만
# 20.2s
car = wd.export_to_pandas("SELECT VIN, BSPL_STDG_CD, VHCL_ERSR_YN, MANP_MNG_NO, EXHST_GAS_GRD_CD, YRIDNW, VHCTY_CD, PURPS_CD2, FRST_REG_YMD, VHCL_FBCTN_YMD, VHRNO FROM STD_CEG_CAR_MIG WHERE EXHST_GAS_GRD_CD = 'A0504' OR EXHST_GAS_GRD_CD = 'A05T4' OR EXHST_GAS_GRD_CD = 'A0505' OR EXHST_GAS_GRD_CD = 'A05T5';")
car_ch_col = {
    'VIN':'차대번호', 
    'BSPL_STDG_CD':'법정동코드', 
    'VHCL_ERSR_YN':'차량말소YN',
    'MANP_MNG_NO':'제원관리번호', 
    'EXHST_GAS_GRD_CD':'배출가스등급', 
    'YRIDNW':'차량연식', 
    'VHCTY_CD':'차종', 
    'PURPS_CD2':'용도', 
    'FRST_REG_YMD':'최초등록일자',
    'VHCL_FBCTN_YMD':'제작일자',
    'VHRNO':'차량번호',
}
carr = car.rename(columns=car_ch_col)

## 중복 차대번호 제거
carr['최초등록일자'] = pd.to_numeric(carr['최초등록일자'], errors='coerce')
carr = carr.sort_values('최초등록일자', ascending=False).drop_duplicates('차대번호').reset_index(drop=True)

## 배출가스등급 코드 변환
grd_dict = {
    'A0501':'1', 
    'A0502':'2', 
    'A0503':'3', 
    'A0504':'4', 
    'A0505':'5', 
    'A05T2':'2',
    'A05T3':'3', 
    'A05T4':'4', 
    'A05T5':'5', 
    'A05X':'X', 
}
carr['배출가스등급'] = carr['배출가스등급'].replace(grd_dict)

## 차종 코드 변환
cd_dict = {
    'A31M':'이륜', 
    'A31P':'승용', 
    'A31S':'특수', 
    'A31T':'화물', 
    'A31V':'승합'
}
carr['차종'] = carr['차종'].replace(cd_dict)

## 용도 코드 변환
purps_dict = {
    'A08P':'개인용', 
    'A08B':'영업용', 
    'A08O':'관용',
}
carr['용도'] = carr['용도'].replace(purps_dict)

## 등록정보 말소 제거
carm = carr[carr['차량말소YN'] == 'N'].reset_index(drop=True)

## 등록&제원 병합
# 0.7s
cs = carm.merge(srcr, on='제원관리번호', how='left')

## 등록&제원&정기&정밀 병합
# 2m 0.5s
csi = cs.merge(insm, on='차대번호', how='left')

csi['법정동코드'] = csi['법정동코드'].astype('str')
csi['법정동코드'] = csi['법정동코드'].str[:5] + '00000'
csi['법정동코드'] = pd.to_numeric(csi['법정동코드'])

## 등록&제원&정기&정밀&법정동 병합
csic = csi.merge(coder, on='법정동코드', how='left')

# csic['시도'].isnull().sum()

# # 주소 수정
# csic.loc[csic['법정동코드'] == 5172035031, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 강원특별자치도 홍천군
# csic.loc[csic['법정동코드'] == 5180031023, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csic.loc[csic['법정동코드'] == 5180031031, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csic.loc[csic['법정동코드'] == 5172035030, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csic.loc[csic['법정동코드'] == 5180031028, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 

# csic.loc[csic['법정동코드'] == 5172035021, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csic.loc[csic['법정동코드'] == 5180031025, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 
# csic.loc[csic['법정동코드'] == 4165052000, ['시도', '시군구']] = ['경기도', '포천시'] # 경기도 포천시 선단동
# csic.loc[csic['법정동코드'] == 5172035023, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csic.loc[csic['법정동코드'] == 5180031027, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 

# csic.loc[csic['법정동코드'] == 5172035024, ['시도', '시군구']] = ['강원특별자치도', '홍천군'] # 
# csic.loc[csic['법정동코드'] == 5175037022, ['시도', '시군구']] = ['강원특별자치도', '영월군'] # 
# csic.loc[csic['법정동코드'] == 4163055000, ['시도', '시군구']] = ['경기도', '양주시'] # 경기도 양주시 회천3동
# csic.loc[csic['법정동코드'] == 5180031033, ['시도', '시군구']] = ['강원특별자치도', '양구군'] # 경기도 양주시 회천3동

## 등록&제원&정기&정밀&법정동&저감 병합
attr = attr.drop_duplicates('차대번호').reset_index(drop=True)
csica = csic.merge(attr, on='차대번호', how='left')

## 운행제한 건수
limit['운행제한건수'] = limit[['계절제_1차', '계절제_2차', '계절제_3차', '계절제_4차', '비상시', '상시']].sum(axis=1)

## 등록&제원&정기&정밀&법정동&저감&운행제한건수 병합
df = csica.merge(limit[['차대번호', '운행제한건수']], on='차대번호', how='left')

## 저감장치 부착 여부 result로 수정
## 4등급 result 파일 참고하여 DPF유무 수정
rdf = df.copy()
rs = rs.drop_duplicates('차대번호').reset_index(drop=True)
rdf1 = rdf.merge(rs, on='차대번호', how='left')
rdf1.loc[(rdf1['DPF_YN'] == '유') | (rdf1['DPF유무_수정'] == '유'), 'DPF_YN'] = '유'
rdf1.loc[(rdf1['DPF유무_수정'] == '무'), 'DPF_YN'] = '무'
rdf1.loc[(rdf1['DPF유무_수정'] == '확인불가'), 'DPF_YN'] = '확인불가'
df = rdf1.drop('DPF유무_수정', axis=1)

# 전처리
## 일일평균주행거리 계산
df['최초등록일자'] = pd.to_datetime(df['최초등록일자'], format="%Y%m%d", errors='coerce')
df['검사일자'] = pd.to_datetime(df['검사일자'], format="%Y%m%d", errors='coerce')

today_date = datetime.today().strftime("%Y-%m-%d")
df['현재날짜'] = today_date
df['현재날짜'] = pd.to_datetime(df['현재날짜'], format='%Y-%m-%d', errors='coerce')
df['최근검사경과일'] = df['현재날짜'] - df['검사일자']
df['최근검사경과일'] = df['최근검사경과일'].astype('str')
df['최근검사경과일'] = df['최근검사경과일'].str.split(' ').str[0]
df['최근검사경과일'] = pd.to_numeric(df['최근검사경과일'], errors='coerce')
df['등록일기준검사일'] = df['검사일자'] - df['최초등록일자']
df['등록일기준검사일'] = df['등록일기준검사일'].astype('str')
df['등록일기준검사일'] = df['등록일기준검사일'].str.split(' ').str[0]
df['등록일기준검사일'] = pd.to_numeric(df['등록일기준검사일'], errors='coerce')
df['일일평균주행거리'] = df['주행거리'] / df['등록일기준검사일']

## KOSIS 데이터 활용 일일평균주행거리 수정
### 빈 값 kosis로 대체
df1y = df[df['일일평균주행거리'].isnull() == False]
df1n = df[df['일일평균주행거리'].isnull() == True]

df1n = df1n.drop('일일평균주행거리', axis=1)
df1nm = df1n.merge(kosisr, on=['시도', '시군구', '차종'], how='left')

df2y = df1nm[df1nm['일일평균주행거리'].isnull() == False]
df2n = df1nm[df1nm['일일평균주행거리'].isnull() == True]
df2n = df2n.drop('일일평균주행거리', axis=1)
df2nm = df2n.merge(kosisr.drop_duplicates(['시도', '차종'])[['시도', '차종', '일일평균주행거리']], on=['시도', '차종'], how='left')

df3y = df2nm[df2nm['일일평균주행거리'].isnull() == False]
df3n = df2nm[df2nm['일일평균주행거리'].isnull() == True]

for ctpv, sgg, cd in df3n.loc[df3n['일일평균주행거리'].isnull() == True, ['시도', '시군구', '차종']].values:
    try:
        df3n.loc[(df3n['일일평균주행거리'].isnull() == True) & (df3n['시도'] == ctpv) & (df3n['시군구'] == sgg), '일일평균주행거리'] = kosisr.loc[(kosisr['시도'] == ctpv) & (kosisr['시군구'] == '소계') & (kosisr['차종'] == '합계'), '일일평균주행거리'].values[0]
    except:
        df3n.loc[(df3n['일일평균주행거리'].isnull() == True) & (df3n['시도'].isnull() == True) & (df3n['시군구'].isnull() == True), '일일평균주행거리'] = kosisr.loc[(kosisr['시도'] == '서울특별시') & (kosisr['시군구'] == '소계') & (kosisr['차종'] == '합계'), '일일평균주행거리'].values[0]

df2nm = pd.concat([df3y, df3n], ignore_index=True)
df1nm = pd.concat([df2y, df2nm], ignore_index=True)
df = pd.concat([df1y, df1nm], ignore_index=True)

## 4, 5등급 분리
g4 = df.loc[df['배출가스등급'] == '4'].reset_index(drop=True)
g5 = df.loc[df['배출가스등급'] == '5'].reset_index(drop=True)

# g4.loc[g4['차량번호'] == '31고7134', '연료']

### 코란KJ 연료 휘발유로 수정
# - 차량번호 : 31고7134
# - 연식 : 1996
# - 연료 : 휘발유
g4.loc[g4['차량번호'] == '31고7134', '연료'] = '휘발유'

# 전처리
gm4d = g4.loc[g4['연료'] == '경유'].reset_index(drop=True)
gm4r = g4.loc[g4['연료'] != '경유'].reset_index(drop=True)

### 5등급 저감장치 변환
# - 1종 -> DPF
# - 1종+SCR -> PM-NOx
g5.loc[g5['저감장치구분'] == '1종', '저감장치'] = 'DPF'
g5.loc[g5['저감장치구분'] == '1종+SCR', '저감장치'] = 'PM-NOx'

## 5등급 경유차 추출(gm5d)
gm5d = g5.loc[g5['연료'] == '경유'].reset_index(drop=True)
gm5r = g5.loc[g5['연료'] != '경유'].reset_index(drop=True)

## 필수 컬럼 추출
gm4d = gm4d.rename(columns={'차량연식':'연식', 'DPF_YN':'저감장치부착유무'})
gm4d = gm4d[[
    '차대번호', 
    '차량번호', 
    '법정동코드', 
    '시도', 
    '시군구', 
    '연식', 
    '용도', 
    '차종', 
    '차종유형', 
    '저감장치부착유무', 
    '무부하매연측정치1', 
    '일일평균주행거리',
    '최근검사경과일', 
    '운행제한건수', 
    ]]

gm5d = gm5d.rename(columns={'차량등록번호':'차량번호', '본거지법정동코드':'법정동코드', '차량연식':'연식', 'DPF_YN':'저감장치부착유무'})
gm5d = gm5d[[
    '차대번호', 
    '차량번호', 
    '법정동코드', 
    '시도', 
    '시군구', 
    '연식', 
    '용도', 
    '차종', 
    '차종유형', 
    '저감장치', 
    '저감장치부착유무',
    '무부하매연측정치1', 
    '일일평균주행거리',
    '최근검사경과일', 
    '운행제한건수', 
    ]]

# 이상값 추출
gm4d['운행제한건수'] = gm4d['운행제한건수'].fillna(0)
gm5d['운행제한건수'] = gm5d['운행제한건수'].fillna(0)
idx4 = set(gm4d.index)
idx5 = set(gm5d.index)

## A급
# - 최근검사경과일 365*3 = 1095 초과 또는 최근검사경과일 없는 경우
# - 운행제한건수 1이상
idx4a = gm4d.loc[((gm4d['최근검사경과일'] > 1095) | (gm4d['최근검사경과일'].isnull() == True)) & (gm4d['운행제한건수'] >= 1)].index
gm4da = gm4d.loc[idx4a]
gm4da['우선순위'] = 1

idx5a = gm5d.loc[((gm5d['최근검사경과일'] > 1095) | (gm5d['최근검사경과일'].isnull() == True)) & (gm5d['운행제한건수'] >= 1)].index
gm5da = gm5d.loc[idx5a]
gm5da['우선순위'] = 1

## B급
# - 최근검사경과일 365*3 = 1095 초과 또는 최근검사경과일 없는 경우
# - 운행제한건수 1미만
idx4b = gm4d.loc[((gm4d['최근검사경과일'] > 1095) | (gm4d['최근검사경과일'].isnull() == True)) & (gm4d['운행제한건수'] < 1)].index
gm4db = gm4d.loc[idx4b]
gm4db['우선순위'] = 2

idx5b = gm5d.loc[((gm5d['최근검사경과일'] > 1095) | (gm5d['최근검사경과일'].isnull() == True)) & (gm5d['운행제한건수'] < 1)].index
gm5db = gm5d.loc[idx5b]
gm5db['우선순위'] = 2

## C급
# - 최근검사경과일 365*3 = 1095 이하
idx4c = gm4d.loc[gm4d['최근검사경과일'] <= 1095].index
gm4dc = gm4d.loc[idx4c]
gm4dc['우선순위'] = 3

idx5c = gm5d.loc[gm5d['최근검사경과일'] <= 1095].index
gm5dc = gm5d.loc[idx5c]
gm5dc['우선순위'] = 3

# 분석
## 가중치 설정을 위한 상관계수 확인
# - 선별 조건(4가지)
#     - 매연
#     - 일일평균주행거리
#     - 최근검사경과일
#     - 운행제한단속건수
gm4di = gm4dc.copy()
gm5di = gm5dc.copy()

### 상관계수 계산
#### 4등급 경유 C급
# 선별조건선정
col = ['무부하매연측정치1', '일일평균주행거리', '최근검사경과일', '운행제한건수']
gm4di[col] = gm4di[col].fillna(0)
gm4di_corr = gm4di[col].corr()

##### 4등급 경유차 선별조건 상관계수
t4 = pd.concat([gm4di_corr, gm4di_corr.sum()], axis=1)
t4 = t4.rename(columns={0:'합계'})
t4 = t4.reset_index()

today_date = datetime.today().strftime("%Y%m%d")
t4['테이블생성일자'] = today_date
chc_col = {
    '테이블생성일자':'LOAD_DT', 
    'index':'LIST', 
    '무부하매연측정치1':'NOLOD_SMO_MEVLU1', 
    '일일평균주행거리':'DY_AVRG_DRVNG_DSTNC', 
    '최근검사경과일':'RCNT_INSP_ELPSD_WHL', 
    '운행제한건수':'RUN_LMT_NOCS', 
    '합계':'TOT_CRRLTN_CFFCNT',
}
STD_BD_GRD4_DS_CRRLTN_CFFCNT = t4.rename(columns=chc_col)

# STD_BD_GRD4_DS_CRRLTN_CFFCNT.columns

##### [출력] STD_BD_GRD4_DS_CRRLTN_CFFCNT

# expdf = STD_BD_GRD4_DS_CRRLTN_CFFCNT
# table_nm = 'STD_BD_GRD4_DS_CRRLTN_CFFCNT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_DS_CRRLTN_CFFCNT')

c1, c2, c3, c4 = gm4di_corr.sum()[col]
sc1 = c1 + c2 + c3 + c4 
w1, w2, w3, w4 = c1/sc1, c2/sc1, c3/sc1, c4/sc1
gm4di['선별포인트'] = np.round(w1 * gm4di['무부하매연측정치1'] + w2 * gm4di['일일평균주행거리'] + w3 * gm4di['최근검사경과일'] + w4 * gm4di['운행제한건수'] , 0)

#### 4등급 경유차 선별포인트 샘플
gm4da['선별포인트'] = np.nan
gm4db['선별포인트'] = np.nan
total4d = pd.concat([gm4da, gm4db, gm4di], ignore_index=True)

today_date = datetime.today().strftime("%Y%m%d")
total4d['테이블생성일자'] = today_date
STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT = total4d[[
    '테이블생성일자', 
    '차대번호', 
    '차량번호', 
    '법정동코드',
    '시도', 
    '시군구', 
    '연식', 
    '용도', 
    '차종', 
    '차종유형', 
    '우선순위',
    '선별포인트',
    '무부하매연측정치1', 
    '일일평균주행거리',
    '최근검사경과일', 
    '운행제한건수', 
    ]]
chc_col = {
    '테이블생성일자':'LOAD_DT', 
    '차대번호':'VIN', 
    '차량번호':'VHRNO', # 자동차등록번호
    '법정동코드':'STDG_CD', 
    '시도':'CTPV_NM', 
    '시군구':'SGG_NM', 
    '연식':'YRIDNW', 
    '용도':'PURPS_CD2', 
    '차종':'VHCTY_CD', # 차종코드
    '차종유형':'VHCTY_TY', 
    '우선순위':'PRIO_GRD',
    '선별포인트':'SELCT_PNT',
    '무부하매연측정치1':'NOLOD_SMO_MEVLU1', 
    '일일평균주행거리':'DY_AVRG_DRVNG_DSTNC',
    '최근검사경과일':'RCNT_INSP_ELPSD_WHL', 
    '운행제한건수':'RUN_LMT_NOCS', 
    '지원비용_백만원':'SPRT_CST',
    '배기량_리터':'DSPLVL',
    '총중량_톤':'TOTL_WGHT',
}
STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT = STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT.rename(columns=chc_col)

# STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT.columns

##### [출력] STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT

# expdf = STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT
# table_nm = 'STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT')

#### 5등급 경유 C급
###### 조기폐차 선별포인트
# 선별조건선정
col = ['무부하매연측정치1', '일일평균주행거리', '최근검사경과일', '운행제한건수']
# nan 값 0으로 채우기
gm5di[col] = gm5di[col].fillna(0)
gm5di_corr = gm5di[col].corr()

###### 5등급 경유차 선별조건 상관계수
t5 = pd.concat([gm5di_corr, gm5di_corr.sum()], axis=1)
t5 = t5.rename(columns={0:'합계'})
t5 = t5.reset_index()
today_date = datetime.today().strftime("%Y%m%d")
t5['테이블생성일자'] = today_date

chc_col = {
    '테이블생성일자':'LOAD_DT', 
    'index':'LIST', 
    '무부하매연측정치1':'NOLOD_SMO_MEVLU1', 
    '일일평균주행거리':'DY_AVRG_DRVNG_DSTNC', 
    '최근검사경과일':'RCNT_INSP_ELPSD_WHL', 
    '운행제한건수':'RUN_LMT_NOCS', 
    '합계':'TOT_CRRLTN_CFFCNT',
}
STD_BD_GRD5_DS_CRRLTN_CFFCNT = t5.rename(columns=chc_col)

# STD_BD_GRD5_DS_CRRLTN_CFFCNT.columns

##### [출력] STD_BD_GRD5_DS_CRRLTN_CFFCNT

# expdf = STD_BD_GRD5_DS_CRRLTN_CFFCNT
# table_nm = 'STD_BD_GRD5_DS_CRRLTN_CFFCNT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD5_DS_CRRLTN_CFFCNT')

c1, c2, c3, c4 = gm5di_corr.sum()[col]
sc1 = c1 + c2 + c3 + c4
w1, w2, w3, w4 = c1/sc1, c2/sc1, c3/sc1, c4/sc1
gm5di['선별포인트'] = np.round(w1 * gm5di['무부하매연측정치1'] + w2 * gm5di['일일평균주행거리'] + w3 * gm5di['최근검사경과일'] + w4 * gm5di['운행제한건수'], 0)

#### 5등급 경유차 선별포인트 샘플

gm5da['선별포인트'] = np.nan
gm5db['선별포인트'] = np.nan

total5d = pd.concat([gm5da, gm5db, gm5di], ignore_index=True)

today_date = datetime.today().strftime("%Y%m%d")
total5d['테이블생성일자'] = today_date
STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT = total5d[[
    '차대번호',
    '차량번호',
    '법정동코드',
    '시도',
    '시군구',
    '연식',
    '용도',
    '차종',
    '차종유형',
    '저감장치',
    '우선순위',
    '선별포인트',
    '무부하매연측정치1',
    '일일평균주행거리',
    '최근검사경과일',
    '운행제한건수',
    '테이블생성일자', 
]]
chc_col = {
    '테이블생성일자':'LOAD_DT', 
    '차대번호':'VIN', 
    '차량번호':'VHRNO', # 자동차등록번호
    '법정동코드':'STDG_CD', 
    '시도':'CTPV_NM', 
    '시군구':'SGG_NM', 
    '연식':'YRIDNW', # 연식
    '용도':'PURPS_CD2', 
    '차종':'VHCTY_CD', # 차종코드
    '차종유형':'VHCTY_TY', 
    '저감장치':'RDCDVC',
    '우선순위':'PRIO_GRD',
    '선별포인트':'SELCT_PNT',
    '무부하매연측정치1':'NOLOD_SMO_MEVLU1', 
    '일일평균주행거리':'DY_AVRG_DRVNG_DSTNC',
    '최근검사경과일':'RCNT_INSP_ELPSD_WHL', 
    '운행제한건수':'RUN_LMT_NOCS', 
}
STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT = STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT.rename(columns=chc_col)

# STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT.columns

##### [출력] STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT

# expdf = STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT
# table_nm = 'STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT')

## 저공해조치선별
# lem4d = total4d.merge(coder[['법정동코드', '시도']], on='법정동코드', how='left')
# lem5d = total5d.merge(coder[['법정동코드', '시도']], on='법정동코드', how='left')
lem4d = total4d.copy()
lem5d = total5d.copy()

# lem4d['시도'].isnull().sum(), lem5d['시도'].isnull().sum()

# lem4d.loc[lem4d['법정동코드'] == 5180031023, '시도'] = '강원특별자치도' # 강원특별자치도 양구군 남면 야촌리
# lem4d.loc[lem4d['법정동코드'] == 5172035030, '시도'] = '강원특별자치도' # 강원특별자치도 홍천군 동면 노천리
# lem4d.loc[lem4d['법정동코드'] == 5172035021, '시도'] = '강원특별자치도' # 강원특별자치도 홍천군 동면 속초리

# # lem5d.loc[lem5d['법정동코드'] == 4165052000, '시도'] = '경기도' # 경기도 포천시 선단동
# lem5d.loc[lem5d['법정동코드'] == 5180031023, '시도'] = '강원특별자치도' # 강원특별자치도 양구군 남면 야촌리
# lem5d.loc[lem5d['법정동코드'] == 5180031027, '시도'] = '강원특별자치도' # 강원특별자치도 양구군 남면 황강리
# lem5d.loc[lem5d['법정동코드'] == 5180031025, '시도'] = '강원특별자치도' # 강원특별자치도 양구군 남면 청리
# lem5d.loc[lem5d['법정동코드'] == 5180031028, '시도'] = '강원특별자치도' # 강원특별자치도 양구군 남면 창리
# lem5d.loc[lem5d['법정동코드'] == 5172035023, '시도'] = '강원특별자치도' # 강원특별자치도 홍천군 동면 덕치리
# lem5d.loc[lem5d['법정동코드'] == 5180031031, '시도'] = '강원특별자치도' # 강원특별자치도 양구군 남면 죽리
# lem5d.loc[lem5d['법정동코드'] == 5175037022, '시도'] = '강원특별자치도' # 강원특별자치도 홍천군 서면 어유포리
# lem5d.loc[lem5d['법정동코드'] == 5180031033, '시도'] = '강원특별자치도' # 강원특별자치도 양구군 남면 원리

lem4d['배출가스등급'] = '4'
lem5d['배출가스등급'] = '5'
lem = pd.concat([lem4d, lem5d], ignore_index=True)

grp1 = lem.groupby(['시도', '배출가스등급', '차종', '차종유형', '우선순위']).agg({'차대번호':'count'}).unstack('우선순위').reset_index()
grp1.columns = ['시도', '배출가스등급', '차종', '차종유형', '1순위(대수)', '2순위(대수)', '3순위(대수)']
grp1[['1순위(대수)', '2순위(대수)', '3순위(대수)']] = grp1[['1순위(대수)', '2순위(대수)', '3순위(대수)']].fillna(0)
grp1['합계'] = grp1.iloc[:, -3:].sum(axis=1)
grp1['1순위(비율)'] = grp1['1순위(대수)'] / grp1['합계']
grp1['2순위(비율)'] = grp1['2순위(대수)'] / grp1['합계']
grp1['3순위(비율)'] = grp1['3순위(대수)'] / grp1['합계']
grp1 = grp1[['시도', '배출가스등급', '차종', '차종유형', '합계', '1순위(대수)', '1순위(비율)', '2순위(대수)', '2순위(비율)', '3순위(대수)', '3순위(비율)']]
grp1 = grp1.rename(columns={'합계':'선별대수'})

grp2 = lem.groupby(['시도', '배출가스등급', '차종', '차종유형']).agg({'저감장치':'count'}).reset_index()
grp2 = grp2.rename(columns={'저감장치':'저감장치부착대수'})

grp = grp1.merge(grp2, on=['시도', '배출가스등급', '차종', '차종유형'], how='left')
today_date = datetime.today().strftime("%Y%m%d")
grp['테이블생성일자'] = today_date
cdict = {
    '시도':'CTPV',
    '배출가스등급':'EXHST_GAS_GRD_CD',
    '차종':'VHCTY_CD',
    '차종유형':'VHCTY_TY',
    '선별대수':'SELCT_MKCNT',
    '1순위(대수)':'SENO1_MKCNT',
    '1순위(비율)':'SENO1_RT',
    '2순위(대수)':'SENO2_MKCNT',
    '2순위(비율)':'SENO2_RT',
    '3순위(대수)':'SENO3_MKCNT',
    '3순위(비율)':'SENO3_RT',
    '저감장치부착대수':'RDCDVC_EXTRNS_MKCNT',
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_LEM_SELCT = grp.rename(columns=cdict)

# STD_BD_DAT_LEM_SELCT.columns

### [출력] STD_BD_DAT_LEM_SELCT

# expdf = STD_BD_DAT_LEM_SELCT
# table_nm = 'STD_BD_DAT_LEM_SELCT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_LEM_SELCT')

## 선별포인트현황
for n in range(0, 350, 50):
    if n <= 300:
        lem.loc[(lem['선별포인트'] >= n) & (lem['선별포인트'] < n + 50), '선별포인트구간'] = f'{n} ~ {n+49}'
    else:
        lem.loc[lem['선별포인트'] >= n, '선별포인트구간'] = f'{n}이상'

stat = lem.groupby(['배출가스등급', '선별포인트구간', '차종', '차종유형']).agg({'차대번호':'count', '무부하매연측정치1':'mean', '일일평균주행거리':'mean', '최근검사경과일':'mean', '운행제한건수':'mean'}).reset_index()
stat = stat.rename(columns={'차대번호':'차량대수', '무부하매연측정치1':'매연측정값'})
today_date = datetime.today().strftime("%Y%m%d")
stat['테이블생성일자'] = today_date
cdict = {
    '배출가스등급':'EXHST_GAS_GRD_CD',
    '선별포인트구간':'SELCT_PNT_RNG',
    '차종':'VHCTY_CD',
    '차종유형':'VHCTY_TY',
    '차량대수':'VHCL_MKCNT',
    '매연측정값':'SMO_MSRMT_VAL',
    '일일평균주행거리':'DY_AVRG_DRVNG_DSTNC',
    '최근검사경과일':'RCNT_INSP_ELPSD_WHL',
    '운행제한건수':'RUN_LMT_NOCS',
    '테이블생성일자':'LOAD_DT', 
}
STD_BD_DAT_SELCT_PNT_CURSTT = stat.rename(columns=cdict)

# STD_BD_DAT_SELCT_PNT_CURSTT.columns

### [출력] STD_BD_DAT_SELCT_PNT_CURSTT

# expdf = STD_BD_DAT_SELCT_PNT_CURSTT
# table_nm = 'STD_BD_DAT_SELCT_PNT_CURSTT'.upper()

# # 테이블 생성
# sql = 'create or replace table ' + table_nm + '( \n'

# for idx,column in enumerate(expdf.columns):
#     # if 'float' in expdf[column].dtype.name:
#     #     sql += column + ' float'
#     # elif 'int' in expdf[column].dtype.name:
#     #     sql += column + ' number'
#     # else:
#     sql += column + ' varchar(255)'

#     if len(expdf.columns) - 1 != idx:
#         sql += ','
#     sql += '\n'
# sql += ')'    
# we.execute(sql)

# # 데이터 추가
# # 5s
# we.import_from_pandas(expdf, table_nm)

# print(f'data export : {table_nm}')
print('data export : STD_BD_DAT_SELCT_PNT_CURSTT')

## 3-3 code end ##################################################################

print('code end')
print(time.time() - start_time)
# code end ##################################################################