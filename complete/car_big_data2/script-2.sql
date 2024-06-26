-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_CAR_CURSTT_MOD(
	CRTR_YM VARCHAR2(200), 
	RGN VARCHAR2(200),
	CTPV_NM VARCHAR2(200),
	EXHST_GAS_GRD_CD VARCHAR2(200),
	VHCL_MKCNT VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_CAR_CURSTT_MOD FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_CAR_CURSTT_MOD.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_CAR_CURSTT_MOD2(
	CRTR_YM VARCHAR2(200), 
	FUEL_CD VARCHAR2(200),
	EXHST_GAS_GRD_CD VARCHAR2(200),
	VHCL_MKCNT VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_CAR_CURSTT_MOD2 FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_CAR_CURSTT_MOD2.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;



-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_CAR_PRET(
	YMD VARCHAR2(200),
	GSL VARCHAR2(200),
	DSL VARCHAR2(200),
	LPG VARCHAR2(200),
	GSL_PRET VARCHAR2(200),
	DSL_PRET VARCHAR2(200),
	LPG_PRET VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_CAR_PRET FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_CAR_PRET.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_ECO_CAR_PRET(
	YMD VARCHAR2(200),
	BTYCR VARCHAR2(200),
	HY VARCHAR2(200),
	BTYCR_PRET VARCHAR2(200),
	HY_PRET VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_ECO_CAR_PRET FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_ECO_CAR_PRET.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;



-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_ENLF_VHC_ELPDSRC_MNG_INFO(
	VIN VARCHAR2(200),
	ELPDSRC_LAST_APRV_YN VARCHAR2(200),
	BSPL_STDG_CD VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_ENLF_VHC_ELPDSRC_MNG_INFO FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_ENLF_VHC_ELPDSRC_MNG_INFO.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_ERSR_RSLT(
	BSPL_STDG_CD VARCHAR2(200), 
	VIN VARCHAR2(200),
	CHG_YMD VARCHAR2(200),
	EXHST_GAS_GRD_CD VARCHAR2(200),
	FUEL_CD VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_ERSR_RSLT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_ERSR_RSLT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_GRD5_ELPDSRC_CURSTT(
	RGN VARCHAR2(200),
	YR VARCHAR2(200),
	VHCL_MKCNT VARCHAR2(200),
	VHCL_REDE VARCHAR2(200),
	ELPDSRC VARCHAR2(200),
	NTRL_DCLN VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_GRD5_ELPDSRC_CURSTT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_GRD5_ELPDSRC_CURSTT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_GRD5_LEM_N_MOD(
	RGN VARCHAR2(200),
	SEASON VARCHAR2(200),
	VHCL_MKCNT VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_GRD5_LEM_N_MOD FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_GRD5_LEM_N_MOD.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_LEM_ND_RUN_LMT_CURSTT(
	BSPL_STDG_CD VARCHAR2(200),
	VIN VARCHAR2(200),
	MANG_MNG_NO VARCHAR2(200),
	EXHST_GAS_CERT_NO VARCHAR2(200),
	FUEL_CD VARCHAR2(200),
	VHCL_ERSR_YN VARCHAR2(200),
	EXHST_GAS_GRD_CD VARCHAR2(200),
	CTPV VARCHAR2(200),
	SGG VARCHAR2(200),
	ELPDSRC_LAST_APRV_YN VARCHAR2(200),
	RDCDVC VARCHAR2(200),
	RUN_LMT_CRDN_YN VARCHAR2(200),
	SEASON1_CRDN_YN VARCHAR2(200),
	SEASON2_CRDN_YN VARCHAR2(200),
	SEASON3_CRDN_YN VARCHAR2(200),
	EMGN_CRDN_YN VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_LEM_ND_RUN_LMT_CURSTT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_LEM_ND_RUN_LMT_CURSTT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_CAR_REG_MKCNT(
	YR VARCHAR2(200),
	FUEL_CD VARCHAR2(200),
	CTPV VARCHAR2(200),
	VHCL_MKCNT VARCHAR2(200),
	FUEL_RT VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_CAR_REG_MKCNT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_CAR_REG_MKCNT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_GRD4_DS_INSP_WHL_ELPSD(
	VIN VARCHAR2(200),
	VHRNO VARCHAR2(200),
	STDG_CD VARCHAR2(200),
	PURPS_CD2 VARCHAR2(200),
	VHCTY_CD VARCHAR2(200),
	VHCTY_TY VARCHAR2(200),
	INSP_YMD VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_GRD4_DS_INSP_WHL_ELPSD FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_GRD4_DS_INSP_WHL_ELPSD.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_GRD5_DS_INSP_WHL_ELPSD(
	VIN VARCHAR2(200),
	VHRNO VARCHAR2(200),
	STDG_CD VARCHAR2(200),
	PURPS_CD2 VARCHAR2(200),
	VHCTY_CD VARCHAR2(200),
	VHCTY_TY VARCHAR2(200),
	INSP_YMD VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_GRD5_DS_INSP_WHL_ELPSD FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_GRD5_DS_INSP_WHL_ELPSD.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;

-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_SEASON_CRDN_NOCS_CURSTT(
	VIN VARCHAR2(200), 
	SEASON_1ODR_CRDN_NOCS VARCHAR2(200), 
	SEASON_2ODR_CRDN_NOCS VARCHAR2(200), 
	SEASON_3ODR_CRDN_NOCS VARCHAR2(200), 
	SEASON_4ODR_CRDN_NOCS VARCHAR2(200), 
	RGN VARCHAR2(200), 
	CTPV VARCHAR2(200), 
	DPF_EXTRNS_YN VARCHAR2(200), 
	VHCTY_CD VARCHAR2(200), 
	VHCTY_TY VARCHAR2(200)

);

IMPORT INTO	"vsyse".STD_BD_SEASON_CRDN_NOCS_CURSTT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_SEASON_CRDN_NOCS_CURSTT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_ORDITM_DSCL_CURSTT(
	VIN VARCHAR2(200), 
	DSCL_YR VARCHAR2(200), 
	DSCL_NOCS VARCHAR2(200), 
	RGN VARCHAR2(200), 
	CTPV VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_ORDITM_DSCL_CURSTT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_ORDITM_DSCL_CURSTT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_SEASON_DSCL_RGN_CURSTT(
	DSCL_RGN VARCHAR2(200), 
	DSCL_CTPV VARCHAR2(200), 
	VIN VARCHAR2(200), 
	SEASON_1ODR_CRDN_NOCS VARCHAR2(200), 
	SEASON_2ODR_CRDN_NOCS VARCHAR2(200), 
	SEASON_3ODR_CRDN_NOCS VARCHAR2(200), 
	SEASON_4ODR_CRDN_NOCS VARCHAR2(200), 
	VHCTY_CD VARCHAR2(200), 
	VHCTY_TY VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_SEASON_DSCL_RGN_CURSTT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_SEASON_DSCL_RGN_CURSTT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_FUEL_GRD_VHCL_CURSTT_PRET(
	YR VARCHAR2(200),
	FUEL_CD VARCHAR2(200),
	GRD VARCHAR2(200),
	VHCL_MKCNT VARCHAR2(200),
	VHCL_PRET VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_FUEL_GRD_VHCL_CURSTT_PRET FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_FUEL_GRD_VHCL_CURSTT_PRET.csv'
ROW	SEPARATOR = 'CRLF'
COLUMN SEPARATOR  = ','
SKIP = 1
ENCODING = 'UTF-8'
;


























-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT(
	VIN VARCHAR2(200),
	VHRNO VARCHAR2(200),
	STDG_CD VARCHAR2(200),
	CTPV_NM VARCHAR2(200),
	SGG_NM VARCHAR2(200),
	YRIDNW VARCHAR2(200),
	PURPS_CD2 VARCHAR2(200),
	VHCTY_CD VARCHAR2(200),
	VHCTY_TY VARCHAR2(200),
	PRIO_GRD VARCHAR2(200),
	SELCT_PNT VARCHAR2(200),
	NOLOD_SMO_MEVLU1 VARCHAR2(200),
	DY_AVRG_DRVNG_DSTNC VARCHAR2(200),
	RCNT_INSP_ELPSD_WHL VARCHAR2(200),
	RUN_LMT_NOCS VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_GRD4_LEM_PRIO_ORD_SELCT_CURSTT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT(
	VIN VARCHAR2(200),
	VHRNO VARCHAR2(200),
	STDG_CD VARCHAR2(200),
	CTPV_NM VARCHAR2(200),
	SGG_NM VARCHAR2(200),
	YRIDNW VARCHAR2(200),
	PURPS_CD2 VARCHAR2(200),
	VHCTY_CD VARCHAR2(200),
	VHCTY_TY VARCHAR2(200),
	RDCDVC VARCHAR2(200),
	PRIO_GRD VARCHAR2(200),
	SELCT_PNT VARCHAR2(200),
	NOLOD_SMO_MEVLU1 VARCHAR2(200),
	DY_AVRG_DRVNG_DSTNC VARCHAR2(200),
	RCNT_INSP_ELPSD_WHL VARCHAR2(200),
	RUN_LMT_NOCS VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_GRD5_LEM_PRIO_ORD_SELCT_CURSTT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;


-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_GRD4_DS_CRRLTN_CFFCNT(
	LIST VARCHAR2(200),
	NOLOD_SMO_MEVLU1 VARCHAR2(200),
	DY_AVRG_DRVNG_DSTNC VARCHAR2(200),
	RCNT_INSP_ELPSD_WHL VARCHAR2(200),
	RUN_LMT_NOCS VARCHAR2(200),
	TOT_CRRLTN_CFFCNT VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_GRD4_DS_CRRLTN_CFFCNT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_GRD4_DS_CRRLTN_CFFCNT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;

-- 차량 ㅇ
CREATE TABLE "vsyse".STD_BD_GRD5_DS_CRRLTN_CFFCNT(
	LIST VARCHAR2(200),
	NOLOD_SMO_MEVLU1 VARCHAR2(200),
	DY_AVRG_DRVNG_DSTNC VARCHAR2(200),
	RCNT_INSP_ELPSD_WHL VARCHAR2(200),
	RUN_LMT_NOCS VARCHAR2(200),
	TOT_CRRLTN_CFFCNT VARCHAR2(200)
);

IMPORT INTO	"vsyse".STD_BD_GRD5_DS_CRRLTN_CFFCNT FROM LOCAL CSV FILE 'D:/data/big2/BD3/df/STD_BD_GRD5_DS_CRRLTN_CFFCNT.csv'

ROW	SEPARATOR = 'CRLF'

COLUMN SEPARATOR  = ','

SKIP = 1

ENCODING = 'UTF-8'

;
