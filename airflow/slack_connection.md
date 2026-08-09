# airflow - slack 실패 알림 연결

Airflow Task가 실패하면 지정된 Slack 채널로 알림을 보냅니다.

## 1. Slack Incoming Webhook 발급

1. <https://api.slack.com/apps>에서 Slack App을 생성하거나 기존 App을 선택합니다.
2. 왼쪽 메뉴에서 `Incoming Webhooks`를 선택합니다.
3. `Activate Incoming Webhooks`를 활성화합니다.
4. `Add New Webhook to Workspace`를 누릅니다.
5. 알림을 받을 채널을 선택하고 권한을 허용합니다.
6. `Webhook URLs for Your Workspace`에서 Webhook URL을 복사합니다.

Webhook URL은 반드시 다음 형태여야 합니다.

```text
https://hooks.slack.com/services/...
```

`Bot User OAuth Token`, `User OAuth Token`, `Refresh Token`은 Webhook URL이
아니므로 사용하지 않습니다.

## 2. Airflow Connection 설정

```bash
docker compose up -d
```

브라우저에서 <http://localhost:8080>에 접속한 뒤 `Admin` → `Connections` →
`Add Connection`으로 이동합니다.

| 항목 | 값 |
|---|---|
| Connection ID | `slack_webhook` |
| Connection Type | `Slack Incoming Webhook` |
| Webhook Token | 앞에서 복사한 전체 Webhook URL |
| Slack Webhook Endpoint | 비워두기 |
| Schema | 비워두기 |

저장한 Connection은 로컬 PostgreSQL에만 존재하며 Git으로 공유되지 않습니다.
새로 환경을 구성한 팀원은 각자 Connection을 만들어야 합니다.
