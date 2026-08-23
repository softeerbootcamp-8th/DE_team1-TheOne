# Let's Encrypt가 AWS 기본 제공 도메인엔 인증서를 안 줌

## 증상

도메인을 따로 사지 않고, gateway 인스턴스의 Elastic IP가 기본으로 갖고 있는 AWS
퍼블릭 DNS(`ec2-43-200-202-72.ap-northeast-2.compute.amazonaws.com`)로 인증서를
받으려 했다. 이 도메인은 실제로 정상 resolve된다(`dig`로 확인 완료). 그런데
`certbot`을 돌리면 다음 에러로 계속 실패했다.

```
sudo certbot --nginx -d ec2-43-200-202-72.ap-northeast-2.compute.amazonaws.com
...
Error creating new order :: Cannot issue for
"ec2-43-200-202-72.ap-northeast-2.compute.amazonaws.com": The ACME server
refuses to issue a certificate for this domain name, because it is forbidden
by policy
```

## 원인

Let's Encrypt는 `*.amazonaws.com`처럼 클라우드 제공업체가 소유한 공유 도메인에는
정책적으로 인증서 발급을 거부한다(악용 방지 목적의 차단 목록). 도메인이 실제로
안 뜨거나 소유권을 증명 못 해서가 아니라 — DNS도 정상이고 서버 설정도 문제없다 —
그 도메인 자체가 애초에 발급 대상에서 제외되어 있는 것이라, nginx나 방화벽 쪽을
아무리 고쳐도 해결되지 않는다.

## 해결

도메인을 사지 않고도 되는 무료 와일드카드 DNS 서비스(`sslip.io`)로 전환했다. IP를
호스트네임에 그대로 인코딩하면 그 IP로 자동 resolve되고, 가입이 필요 없으며
Let's Encrypt 차단 목록에도 걸리지 않는다.

```bash
# 실제로 그 IP로 resolve되는지 먼저 확인
dig +short 43-200-202-72.sslip.io
# 43.200.202.72

# nginx 설정의 server_name을 교체
sudo sed -i 's/ec2-43-200-202-72\.ap-northeast-2\.compute\.amazonaws\.com/43-200-202-72.sslip.io/' \
  /etc/nginx/conf.d/dashboard.conf
sudo nginx -t
sudo systemctl reload nginx

# 인증서 재발급
sudo certbot --nginx -d 43-200-202-72.sslip.io
```

이번엔 정상 발급됐고, 최종 접속 주소는 `https://43-200-202-72.sslip.io`가 됐다.
