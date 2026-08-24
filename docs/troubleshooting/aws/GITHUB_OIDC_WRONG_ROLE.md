# GitHub Actions OIDC가 "Not authorized"로 계속 실패함 — 원인은 트러스트 정책이 아니었음

> GitHub 레포 Variable이 EC2용 IAM role을 가리키고 있어 OIDC assume이 거부됨.
> GitHub Actions 배포 전용 role ARN으로 교체해 해결.

## 증상

`deploy-dashboard.yml`의 "AWS 임시 자격증명 취득 (OIDC)" 단계가 `Assuming role with
OIDC`를 여러 번 반복하다 결국 실패.

```
Error: Could not assume role with OIDC: Not authorized to perform sts:AssumeRoleWithWebIdentity
```

## 원인

두 개의 이름이 비슷한 IAM 역할이 있었다.

- `theone-dashboard-role` — 대시보드 **EC2 인스턴스**에 붙인 역할(`ec2.amazonaws.com`
  신뢰, ECR pull용)
- `theone-github-actions-dashboard-deploy` — **GitHub Actions**가 assume해야 하는
  역할(`token.actions.githubusercontent.com` OIDC 신뢰)

GitHub 레포 Variable `AWS_ROLE_ARN_DASHBOARD`가 앞의 것(EC2용)을 가리키고 있었다.
EC2용 역할은 GitHub의 OIDC 자격증명을 신뢰하지 않으니 당연히 assume이 거부된다.
두 역할 다 "대시보드" 배포 작업 중 거의 동시에 만들다 보니 변수에 엉뚱한 ARN이
들어갔다.

## 잘못 짚었던 곳들 (트러스트 정책이 범인이 아니었다)

`Not authorized to perform sts:AssumeRoleWithWebIdentity`는 트러스트 정책 문제처럼
보이지만, 아래를 다 확인해도 전부 정상이었다.

1. **`sub` 클레임 형식** — 이 저장소는 조직/레포 이름이 과거 한 번 바뀐 적이 있어서,
   GitHub이 발급하는 `sub` 클레임이 `repo:org/repo:ref:...`가 아니라
   `repo:org@<org-id>/repo@<repo-id>:ref:refs/heads/develop`처럼 불변 ID가 붙는
   형태다. 이것도 확인했지만 이미 정확히 반영돼 있었다(GitHub API로 재확인 가능):

   ```bash
   gh api repos/<org>/<repo>/actions/oidc/customization/sub
   # {"use_default":true,"use_immutable_subject":false,"sub_claim_prefix":"repo:org@<id>/repo@<id>"}
   ```

2. **워크플로 실행 브랜치** — `develop`가 맞는지 직접 확인:

   ```bash
   gh run list --workflow=deploy-dashboard.yml --limit 5 \
     --json databaseId,headBranch,event,conclusion,createdAt
   ```

3. **트러스트 정책 내용 자체** — 실제로 잘 되는 역할(`theone-github-actions-airflow-deploy`)의
   트러스트 정책과 콘솔이 아니라 **CLI로 원본을 뽑아서** 바이트 단위로 비교했다.
   콘솔 붙여넣기 과정에서 스마트따옴표 같은 눈에 안 보이는 문자가 섞였을 가능성까지
   배제하기 위해서다.

   ```bash
   aws iam get-role --role-name theone-github-actions-dashboard-deploy \
     --query 'Role.AssumeRolePolicyDocument' --output json > /tmp/dashboard-trust.json
   aws iam get-role --role-name theone-github-actions-airflow-deploy \
     --query 'Role.AssumeRolePolicyDocument' --output json > /tmp/airflow-trust.json

   # macOS(BSD) cat은 -A가 없다. GNU의 -A와 같은 효과는 -vet.
   diff <(cat -vet /tmp/dashboard-trust.json) <(cat -vet /tmp/airflow-trust.json)
   ```

   결과는 키 순서만 다르고 완전히 동일(JSON은 순서가 의미 없어 문제 아님).

4. **역할 자체의 다른 설정** — `PermissionsBoundary`, `Path`, `Tags`도 `get-role`
   전체 출력을 비교해 동일함을 확인.

여기까지 확인하고 나서야 "역할 자체는 다 정상인데 왜 안 되지" → "그럼 애초에
**어느 역할을 assume하려 한 건지**부터 확인하자"로 관점을 바꿔 `gh variable list`를
찍어봤고, 거기서 바로 드러났다.

## 해결

```bash
gh variable set AWS_ROLE_ARN_DASHBOARD \
  --body "arn:aws:iam::572660899671:role/theone-github-actions-dashboard-deploy"
```
