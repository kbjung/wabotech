## 	[KEITI] 운행 자동차 배출가스 측정장비 현장실증 기술개발
- 역할
  - 데이터 전처리, 분석
  - 보고서 작성
  - 컨소사 커뮤니케이션, 팀 커뮤니케이션
  - 이슈 대응, 2차 A/S
- 라이브러리 : pandas, numpy, os, datetime, time, matplotlib.pyplot

### 매연RSD 측정 데이터 분석
- 데이터
  - 매연RSD 측정 데이터 : 약 41만 행
  - 차량 제원정보 데이터 : 약 19만 행
  - 차량 정기검사 데이터 : 약 14만 행
  - 차량 정밀검사 데이터 : 약 14만 행
+ 분석 [[코드(ipynb)]](https://github.com/kbjung/wabotech/blob/main/processing/multi_road/analysis2_1_3.ipynb)
  - 원격 측정 전체 차량 통계
  - 원격 측정 경유차량의 매연 및 정밀검사 분석
  - 원격 측정 경유차량의 등록정보, 제원정보 활용 분석

### 통합측정기 측정 데이터 분석
- 데이터
  - 통합측정기 측정 데이터 : 약 49만 행
  - 차량 제원정보 데이터 : 약 14만 행
  - 차량 정밀검사 데이터 : 약 11만 행
+ 분석 [[코드(ipynb)]](https://github.com/kbjung/wabotech/blob/main/processing/multi_road/analysis2_2_2.ipynb)
  - 통합 측정 전체 차량 통계
  - 통합 측정 경유차량 매연, NOx 및 정밀검사 분석
  - 통합 측정 경유차량의 등록, 제원정보 활용 분석
