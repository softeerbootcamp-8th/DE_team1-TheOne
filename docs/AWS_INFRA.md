# AWS 인프라 구성

각 서비스가 어떤 역할을 맡고, 왜 그 위치(VPC 안/밖, public/private subnet)에 있는지 정리.

![AWS 인프라 아키텍처](../assets/AWS_architecture.png)

## 노드별 설명

### 1. 게이트웨이 서버 (`theone-gateway`)
- SSH로 private subnet 내 다른 서버에 접속하기 위한 게이트웨이
- 웹에서 차량 추천 대시보드에 접근하기 위한 Nginx 리버스 프록시 서버로도 사용 — 외부 사용자는 이 서버를 거쳐서만 대시보드에 접근

### 2. 모니터링 서버 (`theone-monitoring`)
- 다른 EC2 호스트의 CPU·메모리·디스크 사용률을 모니터링하는 Prometheus + Grafana 서버
- 외부망에 노출하지 않음 — Grafana/Prometheus UI는 `127.0.0.1` 바인딩 + SSH 터널로만 접근

### 3. 차량 추천 대시보드 서버 (`theone-dashboard-server`)
- 실제 웹사이트로 보여지는 Streamlit 화면
- 게이트웨이 서버를 거쳐 외부에 노출

### 4. 원천 API 서버 (`theone-source-server`)
- 실제 리스 업체의 API 서버라고 가정
- 원천 데이터 서브 파이프라인의 결과물이 이 서버에 오게 됨

### 5. Airflow 서버 (`theone-airflow`)
- Airflow가 띄워지는 서버

### 6. EMR Serverless
- Silver/Gold 계층의 대용량 Spark 처리 실행
- 자원 사용량은 AWS가 발행하는 `AWS/EMRServerless` 지표를 CloudWatch로 모니터링

### 7. RDS
- 집계된 Gold 데이터가 이곳에 모이게 됨
- private subnet에 위치, 외부에 공개하지 않음(`--no-publicly-accessible`)

### 8. S3
- Bronze, Silver 데이터가 이곳에 적재됨
- 서브 파이프라인의 데이터도 이곳에 적재됨

### 9. Lambda
- 원천 소스 API 서버와 상호작용하는 람다만 VPC 내부에 위치
- 그 외 람다는 VPC 밖에 두고, IAM 실행 권한을 최소 범위로 좁혀 보안을 확보
