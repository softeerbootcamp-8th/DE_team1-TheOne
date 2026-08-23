# RDS 생성 마법사에 서브넷(VPC) 선택 화면이 안 나옴

## 증상

Gold 3종 테이블을 담을 RDS를 처음부터 private 서브넷에만 놓고 싶었음. AWS 콘솔에서
"데이터베이스 생성"을 눌러 엔진으로 **PostgreSQL**을 선택하고 마법사를 따라갔는데,
어디에도 VPC/서브넷 그룹을 고르는 화면이 나오지 않음 — "연결" 섹션에 VPC 표시만
있고, 어떤 서브넷(그룹)에 넣을지 사용자가 지정할 수 있는 UI 자체가 없음.

## 원인

검색해보니 예전 RDS 생성 마법사에는 "연결" 단계에 VPC 보안 그룹과 나란히 서브넷
그룹을 선택하는 화면이 있었는데, 현재 콘솔 버전에서는 이 단계가 빠져 있음(
Aurora 계열에만 노출되고 표준 PostgreSQL 생성 흐름에서는 생략됨). 즉 콘솔 마법사만으로는 새로 만드는 RDS를 원하는 서브넷 그룹에 붙일 방법이 없음.

## 해결

콘솔 마법사를 포기하고, 서브넷 그룹을 먼저 손으로 만든 뒤 CLI로 그 그룹을 지정해
인스턴스를 생성.

```bash
# 1. private 서브넷만 담은 DB 서브넷 그룹을 먼저 생성
aws rds create-db-subnet-group \
  --db-subnet-group-name theone-rds-subnet-group \
  --db-subnet-group-description "private only" \
  --subnet-ids subnet-<private-data-a> subnet-<private-data-c> \
  --region ap-northeast-2

# 2. 그 서브넷 그룹을 지정해 CLI로 RDS 인스턴스 생성
#    (콘솔 마법사에는 이 옵션 자체가 없어서 CLI로 우회)
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

## 참고

- 서브넷 그룹은 **인스턴스 생성 시점에 한 번 정해지면 Modify 화면에서 바꿀 수 없는
  항목**이다. 나중에 다른 서브넷 그룹으로 옮기려면 인스턴스를 새로 만들어야 한다 —
  그러니 처음 만들 때 이렇게 CLI로 원하는 서브넷 그룹을 정확히 지정해두는 편이,
  나중에 마이그레이션하는 것보다 훨씬 싸다.