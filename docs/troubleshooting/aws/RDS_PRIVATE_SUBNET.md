# 콘솔 화면에서 서브넷 선택 단계가 사라져 데이터베이스를 만들 수 없던 문제

- 요약
  - AWS 콘솔의 표준 데이터베이스 생성 화면에 네트워크(서브넷) 선택 단계가 보이지 않음
  - 예전에는 있던 단계가 현재 콘솔 버전에서는 빠졌다는 것을 확인
  - 콘솔 대신 명령줄 도구로 네트워크 그룹을 먼저 만들고 지정해 생성

## 문제

데이터베이스(PostgreSQL RDS)를 새로 만들려는데, AWS 콘솔의 생성 화면에 어느 네트워크(서브넷)에 배치할지 고르는 단계 자체가 보이지 않았다. 이 설정 없이는 데이터베이스를 원하는 네트워크 안에 넣을 수 없었다.

## 접근

찾아보니 예전 콘솔 버전에는 "연결" 단계에서 네트워크 보안 그룹과 함께 서브넷 그룹을 고르는 화면이 있었는데, 지금 쓰는 콘솔 버전에서는 이 단계가 빠져 있다는 것을 확인했다.

## 해결

콘솔 화면을 포기하고, 네트워크 그룹(서브넷 그룹)을 명령줄 도구로 먼저 만든 뒤 그 그룹을 지정해서 데이터베이스를 생성했다.

```bash
# 1. 외부에 노출되지 않는 네트워크만 담은 서브넷 그룹을 먼저 생성
aws rds create-db-subnet-group \
  --db-subnet-group-name theone-rds-subnet-group \
  --db-subnet-group-description "private only" \
  --subnet-ids subnet-<private-data-a> subnet-<private-data-c> \
  --region ap-northeast-2

# 2. 그 서브넷 그룹을 지정해 명령줄로 데이터베이스 인스턴스 생성
#    (콘솔에는 이 옵션 자체가 없어서 CLI로 우회)
aws rds create-db-instance \
  --db-instance-identifier theone-database \
  --db-instance-class db.t4g.micro \
  --engine postgres \
  --engine-version 18.3 \
  --master-username postgres \
  --master-user-password <password> \
  --allocated-storage 20 \
  --db-subnet-group-name theone-rds-subnet-group \
  --vpc-security-group-ids <sg-id> \
  --availability-zone ap-northeast-2a \
  --no-publicly-accessible \
  --region ap-northeast-2
```
