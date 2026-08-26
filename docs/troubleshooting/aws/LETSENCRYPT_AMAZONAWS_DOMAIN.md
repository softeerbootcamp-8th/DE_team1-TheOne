# AWS 기본 제공 도메인에는 무료 인증서를 발급받을 수 없던 문제

- 요약
  - 별도 도메인 없이 AWS가 기본 제공하는 주소로 HTTPS 인증서를 받으려다 계속 실패
  - Let's Encrypt가 AWS 공유 도메인에는 정책적으로 인증서 발급을 막는다는 것을 확인
  - 무료 와일드카드 DNS 서비스로 주소를 바꿔 정상 발급

## 문제

서버 접속 주소로 별도 도메인을 구매하지 않고, AWS가 기본으로 붙여주는 퍼블릭 주소로 HTTPS 인증서를 받으려 했다. 그런데 인증서 발급 도구(certbot)를 실행할 때마다 다음 에러로 계속 실패했다.

```
sudo certbot --nginx -d ec2-43-200-202-72.ap-northeast-2.compute.amazonaws.com
...
Error creating new order :: Cannot issue for
"ec2-43-200-202-72.ap-northeast-2.compute.amazonaws.com": The ACME server
refuses to issue a certificate for this domain name, because it is forbidden
by policy
```

## 접근

에러 메시지의 "정책상 금지"라는 문구를 보고 찾아보니, 무료 인증서 발급 서비스인 Let's Encrypt는 `*.amazonaws.com`처럼 클라우드 업체가 소유한 공유 도메인에는 정책적으로 인증서를 내주지 않는다는 것을 확인했다. 여러 사용자가 나눠 쓰는 도메인이라 악용을 막기 위해 아예 차단 목록에 올려둔 것이다.

## 해결

도메인을 사지 않고도 쓸 수 있는 무료 와일드카드 DNS 서비스(`sslip.io`)로 접속 주소를 바꿨다. 이 서비스는 호스트 이름에 IP 주소를 그대로 넣으면 별도 가입이나 설정 없이 그 IP로 연결해준다. AWS 소유 도메인이 아니라서 Let's Encrypt의 차단 대상도 아니다.

```bash
# 그 이름이 실제로 원하는 IP로 연결되는지 먼저 확인
dig +short 43-200-202-72.sslip.io
# 43.200.202.72

# 서버 설정의 접속 주소를 교체
sudo sed -i 's/ec2-43-200-202-72\.ap-northeast-2\.compute\.amazonaws\.com/43-200-202-72.sslip.io/' \
  /etc/nginx/conf.d/dashboard.conf
sudo nginx -t
sudo systemctl reload nginx

# 인증서 재발급
sudo certbot --nginx -d 43-200-202-72.sslip.io
```

## 검증

바뀐 주소로 인증서 발급을 다시 시도해 정상적으로 발급되는 것을 확인했다. 최종 접속 주소는 `https://43-200-202-72.sslip.io`가 됐다.
