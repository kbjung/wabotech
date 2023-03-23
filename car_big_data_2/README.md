## [한국환경공단] 자동차 환경 빅데이터 구축 2차
- 기간 : 2022.11.18 ~ 2023.07.17(총 8개월)
+ 활용라이브러리
  - pandas, numpy, os, tqdm, datetime, glob
 
### 데이터 전처리
+ 추출
  - 등록, 제원정보 [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5B%EC%B6%94%EC%B6%9C%5D%EB%93%B1%EB%A1%9D%EC%A0%95%EB%B3%B4%26%EC%A0%9C%EC%9B%90%EC%A0%95%EB%B3%B4.ipynb)
  - 배출가스인증시험 [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5B%EC%B6%94%EC%B6%9C%5D%EB%B0%B0%EC%B6%9C%EA%B0%80%EC%8A%A4%EC%9D%B8%EC%A6%9D%EC%8B%9C%ED%97%98.ipynb)
  - 배출가스인증정보 [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5B%EC%B6%94%EC%B6%9C%5D%EB%B0%B0%EC%B6%9C%EA%B0%80%EC%8A%A4%EC%9D%B8%EC%A6%9D%EC%A0%95%EB%B3%B4.ipynb)
  - 저감장치부착이력 [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5B%EC%B6%94%EC%B6%9C%5D%EC%A0%80%EA%B0%90%EC%9E%A5%EC%B9%98%EB%B6%80%EC%B0%A9%EC%9D%B4%EB%A0%A5.ipynb)
  - 정기, 정밀 검사 [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5B%EC%B6%94%EC%B6%9C%5D%EC%A0%95%EA%B8%B0%26%EC%A0%95%EB%B0%80%EA%B2%80%EC%82%AC.ipynb)
+ 병합
  - 등록&제원정보 [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5B%EB%B3%91%ED%95%A9%5D%EB%93%B1%EB%A1%9D%EC%A0%95%EB%B3%B4%26%EC%A0%9C%EC%9B%90%EC%A0%95%EB%B3%B4.ipynb)
+ 확인 요청 파일 result 현행화
  - 배출가스인증번호 업데이트1(제작사 확인) [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5B%EC%9A%94%EC%B2%AD0%5D%5B%ED%86%B5%EA%B3%84%5D%5BG4%5D%EB%B0%B0%EC%9D%B8%EC%97%85%EB%8E%8301.ipynb)
  - 배출가스인증번호 업데이트2(확인된 파일과 교차 검증) [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5B%EC%9A%94%EC%B2%AD0%5D%5B%ED%86%B5%EA%B3%84%5D%5BG4%5D%EB%B0%B0%EC%9D%B8%EC%97%85%EB%8E%8302.ipynb)
  
### 분석
#### 과제1(4등급 경유차 중 정상 차량)
- item 1 : 배출가스 인증번호 및 등급오류 검증
  - 배출가스인증번호별 매연 통계 [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5BBD1%5D%5B%ED%86%B5%EA%B3%84%5D%5Bitem1%5D4%EB%93%B1%EA%B8%89_%EA%B2%BD%EC%9C%A0_%EB%B0%B0%EC%9D%B8%EB%B2%88%ED%98%B8%EB%B3%84.ipynb)
- item 2 : 4등급차 세분류
  - Emission Grade(이하 EG) [ipynb](https://github.com/kbjung/wabotech/blob/main/car_big_data_2/%5BBD1%5D%5B%ED%86%B5%EA%B3%84%5D%5Bitem2%5D4%EB%93%B1%EA%B8%89%EC%B0%A8_%EC%84%B8%EB%B6%84%EB%A5%98.ipynb)
- item3 : 운행제한 규제지역 설정에 유용한 데이터 발굴
  - code : 

#### [과제3] "무공해차 전환 가속화 및 내연기관차 퇴출시나리오 도출" 분석과제 개발
- item 1 : 내연기관차 감축추이 및 감소량 예측 분석
  - code : 
- item 2 : 무공해차 전환 및 저공해조치 우선차량 선별
  - code : 
- item 3 : 내연기관차 운행제한 현황 분석
  - code : 
