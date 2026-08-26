# 자동 배포가 엉뚱한 권한 역할을 가리켜 인증에 실패한 문제

- 요약
  - 대시보드 자동 배포가 AWS 인증 단계에서 계속 실패
  - 배포 설정값이 실제로는 서버(EC2)용 권한 역할을 가리키고 있었음
  - GitHub Actions 배포 전용 역할 주소로 바꿔 해결

## 문제

대시보드를 자동으로 배포하는 스크립트가 AWS에 접속하기 위한 임시 인증(OIDC) 단계에서 계속 실패했다.

```
Error: Could not assume role with OIDC: Not authorized to perform sts:AssumeRoleWithWebIdentity
```

## 접근과 해결

AWS에는 "누가 어떤 권한을 쓸 수 있는지" 정하는 권한 역할이 여러 개 있는데, 이름이 비슷한 역할 두 개가 있었다.

- 하나는 대시보드를 실행하는 서버(EC2)에 붙여둔 역할
- 하나는 GitHub의 자동 배포가 사용해야 하는 역할

배포 설정값(`AWS_ROLE_ARN_DASHBOARD`)이 앞의 것(서버용 역할)을 가리키고 있었다. 서버용 역할은 GitHub의 인증 요청을 받아들이도록 설정돼 있지 않으니 인증이 거부된 것이었다. 두 역할을 같은 작업 중에 거의 동시에 만들다 보니 설정값에 다른 역할 주소가 들어갔다.

배포 설정값을 GitHub Actions 전용 역할 주소로 교체했다.

```bash
gh variable set AWS_ROLE_ARN_DASHBOARD \
  --body "arn:aws:iam::572660899671:role/theone-github-actions-dashboard-deploy"
```
