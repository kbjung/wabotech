## ⏳ [한국환경공단] 자동차 환경 빅데이터 구축 3차 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/processing/car_big_data3)
- 기간 : 2023.09.08 ~ 2024.05.05(총 9개월)
- 역할
  - 데이터 전처리, 분석
  - 보고서 작성
  - 컨소사 커뮤니케이션, 팀 커뮤니케이션
  - 이슈 대응, 2차 A/S
- python 주요 라이브러리 : pandas, numpy, os, datetime
- 데이터 : 약 23개 테이블(테이블 당 약 6백 ~ 1억 행)
- 분석 내용
  - 건설기계 검사, 인증 데이터 분석 [[코드(ipynb)]](https://github.com/kbjung/wabotech/blob/main/processing/car_big_data3/an/big3_BD02.ipynb)
    - 건설기계 인증 현황 분석
    - 건설기계 인증관련 검사 분석
    - (진행 중)건설기계 등록제원 현황 분석
    - (예정)노후 건설기계 우선 조치 선별
  - 전문정비 내역, 검사 데이터 분석 [[코드(ipynb)]](https://github.com/kbjung/wabotech/blob/main/processing/car_big_data3/an/big3_BD04.ipynb)
    - 전문정비 내역 현황 분석
    - 전문정비 전후 개선사항 비교 분석
    - 고장 항목별 주요 정비내용, 정비부품 분석
    - 동일 항목 고장 비율 분석
    - (진행 중)연료별 고장내역 현황을 이용한 적정정비 예측
    - (진행 중)연식, 주행거리별 고장내역 현황을 이용한 예방정비 분석
    - (예정)노후화 주기 분석

## ⏳ [KEITI] 운행 자동차 배출가스 측정장비 현장실증 기술개발 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/processing/multi_road#readme)
- 기간 : 2023.11 ~ 2024.02(총 4개월)
- 역할
  - 데이터 전처리, 분석
  - 컨소사 커뮤니케이션, 이슈 대응
- python 주요 라이브러리 : pandas, numpy, os, datetime, matplotlib.pyplot
- 데이터 : 총 6개 테이블(테이블 당 약 14만 ~ 49만 행)
+ 분석 내용
  - 원격측정 데이터 분석 [[코드(ipynb)]](https://github.com/kbjung/wabotech/blob/main/processing/multi_road/analysis2_1_3.ipynb)
    - 원격 측정 전체 차량 통계
    - 원격 측정 경유차량의 매연 및 정밀검사 분석
    - 원격 측정 경유차량의 등록, 제원정보 활용 분석
  - 통합측정기 데이터 분석 [[코드(ipynb)]](https://github.com/kbjung/wabotech/blob/main/processing/multi_road/analysis2_2_2.ipynb)
    - 통합 측정 전체 차량 통계
    - 통합 측정 경유차량 매연, NOx 및 정밀검사 분석
    - 통합 측정 경유차량의 등록, 제원정보 활용 분석
