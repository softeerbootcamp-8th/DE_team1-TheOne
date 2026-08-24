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

## 해결

```bash
gh variable set AWS_ROLE_ARN_DASHBOARD \
  --body "arn:aws:iam::572660899671:role/theone-github-actions-dashboard-deploy"
```
