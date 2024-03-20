# Wabotech
- 2022.11.01 ~ 2024.02.29 (약 1년 4개월)

## ⏳ 진행 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/processing)

### [한국환경공단] 자동차 환경 빅데이터 구축 3차 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/processing/car_big_data3)
- 기간 : 2023.09.08 ~ 2024.05.05(총 9개월)
- 역할
  - 데이터 전처리, 분석
  - 보고서 작성
  - 컨소사 커뮤니케이션, 팀 커뮤니케이션
  - 이슈 대응, 2차 A/S
- python 주요 라이브러리 : pandas, numpy, os, datetime
- 데이터 : 약 23개 테이블(테이블 당 약 6백 ~ 1억 행)
- 분석 내용
  - 건설기계 검사, 인증 데이터 분석
  - 전문정비 내역, 검사 데이터 분석

### ⏳ [KEITI] 운행 자동차 배출가스 측정장비 현장실증 기술개발 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/processing#readme)
- 기간 : 2023.11 ~ 2024.01(총 3개월)
- 역할
  - 데이터 전처리, 분석
  - 컨소사 커뮤니케이션, 이슈 대응
- python 주요 라이브러리 : pandas, numpy, os, datetime, matplotlib.pyplot
- 데이터 : 총 6개 테이블(테이블 당 약 14만 ~ 49만 행)
- 분석 내용
  - 원격/통합 측정 전체 차량 통계
  - 원격/통합 측정 경유차량의 매연 및 정밀검사 분석
  - 원격/통합 측정 경유차량의 등록정보, 제원정보 활용 분석

---

## ✔ 완료 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/complete)

### [한국환경공단] 자동차 환경 빅데이터 구축 2차 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/complete/car2_exasol#readme)
- 기간 : 2022.11.18 ~ 2023.08.17(총 9개월)
- 역할
  - 데이터 전처리 및 분석
  - 분석 시나리오 논의 및 보고서 작성
  - 컨소사 커뮤니케이션, 이슈 대응
- python 주요 라이브러리 : pandas, numpy, os, glob, re, datetime
- 데이터 : 약 14개 테이블(테이블 당 60만 ~ 1억 행)
- 분석 내용
  - 분류별 차량 현황 분석, 배출가스 계산, 지역별 차량 현황 분석
  - 저공해 조치 우선지원 지표 개발 및 지원 차량 분석
  - 차량 현황 예측(Linear, Akima, B-spline)

### [서울 연구원] 배출가스 배출량 계산 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/complete/seoul_lab#readme)
- 역할 : 데이터 전처리 및 분석, 내용 설명 및 논의
- python 주요 라이브러리 : pandas, numpy, os
- 데이터
  - 자동차 등록정보 데이터 : 약 3천만 행
  - 자동차 제원정보 데이터 : 약 50만 행
- 분석 내용
  - 전 등급 차량 배출가스 배출량 계산(CAPSS식 적용)

### [환경부] 내연기관 프리존(Free-zone) 시범사업 운행제한 확대방안 연구 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/complete/low_gas_vehicle#readme)
- 역할 : 데이터 전처리 및 분석, 아이디어 논의
- python 주요 라이브러리 : pandas, numpy, os, datetime
- 분석 내용
  - 자동차 현황(연료, 지역, 배출가스등급별)

### [한국환경공단] 저공해차 보급촉진을 위한 제도운영 지원 및 개선방안 마련 [[상세내용]](https://github.com/kbjung/wabotech/tree/main/complete/low_gas_vehicle#readme)
- 역할 : 데이터 전처리 및 분석
- python 주요 라이브러리 : pandas, numpy, os
- 분석 내용
  - 법정동코드, 주소 데이터 전처리
  - 자동차 지역별 현황
  - 충전소 대수, 충전량 현황
