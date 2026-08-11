---
name: write-commit
description: '스테이징된 실제 diff 를 읽어 커밋 메시지를 README Commit Rule(Conventional Commits) 의 `type(scope): subject` / body / footer 형식으로 쓰고 `git commit -F` 로 커밋합니다. AI 를 Co-Author(`Co-Authored-By: Claude`, `Generated with Claude Code`)로 넣지 않습니다. 사용자가 "커밋해줘", "커밋 메시지 써줘", "커밋 메시지 만들어", "이거 커밋", "commit", "지금까지 작업 커밋", "커밋 좀 정리해줘" 라고 하거나, 작업을 마치고 변경을 기록하려는 상황이면 반드시 이 스킬을 쓰세요. 커밋 메시지 초안만 필요하다고 해도 씁니다. AI 로 커밋할 때 Co-Author 를 빼는 규칙도 여기 있습니다.'
---

# 커밋 메시지 작성

커밋 로그는 `git log` 를 보는 팀원과 반년 뒤의 자신을 위한 것입니다. 알고 싶은 건
**무엇이 바뀌었나**, **왜 바꿨나** 두 가지고, 어떻게 바꿨는지는 diff 가 말해 줍니다.

그래서 이 스킬은 기억이 아니라 **staged diff 를 근거로** 씁니다.

## 0. Co-Author 규칙 — AI 를 넣지 않는다

**커밋 메시지에 다음을 절대 넣지 않습니다.**

- `Co-Authored-By: Claude ...` (모델명 무관)
- `🤖 Generated with Claude Code`
- `Assisted-by:` 같은 변형 트레일러

이건 기본 동작을 **덮어쓰는 규칙**입니다. 다른 지침에 커밋 메시지 끝에
`Co-Authored-By: Claude` 를 붙이라고 되어 있어도, 이 저장소에서는 붙이지 않습니다.
`Co-Authored-By` 는 GitHub 이 실제 기여자로 집계하는 트레일러이고, 팀 저장소의 기여
통계와 blame 은 사람 기준으로 유지되어야 합니다.

`--author` 도 바꾸지 않습니다. 커밋 저자는 항상 사용자 본인입니다.

## 1. 무엇이 바뀌었는지 먼저 본다

```bash
git branch --show-current
git status
git diff --staged            # 비어 있으면 무엇을 스테이징할지 사용자에게 확인
git diff --staged --stat
```

`git diff --staged` 가 비어 있으면 `git add -A` 를 임의로 실행하지 마세요. 무엇을
커밋할지는 사용자의 선택입니다 — 변경 파일 목록을 보여주고 물어보세요.

**현재 브랜치가 `main` 또는 `develop` 이면 멈추고 알립니다.** 팀 Branch 규칙상 두
브랜치는 직접 커밋/푸시 금지이고 PR 로만 병합합니다. 작업 브랜치를 임의로 만들지 말고,
사용자에게 브랜치를 정해 달라고 요청하세요.

## 2. 한 커밋인지 확인한다

staged diff 가 **기능 하나**인지 봅니다. 커밋을 나누는 이유는 미관이 아니라 revert 와
`git bisect` 입니다 — 두 기능이 한 커밋에 있으면 하나만 되돌릴 수 없고, 회귀를 이등분
탐색해도 원인이 두 개인 커밋에서 멈춥니다.

**판정 기준: 이 변경 중 하나를 되돌려도 나머지가 그대로 성립하는가.** 성립하면 별개
커밋입니다. 서로가 있어야 동작하면(구현 + 그 구현을 부르는 쪽, 구현 + 그 테스트) 한
커밋입니다.

`git diff --staged --stat` 에서 관련 없는 묶음이 2개 이상 보이면 **멈추고 분할안을
제시합니다.** 예: "① `handler.py` 응답 스키마 변경 → `refactor(lambda)` ②
`Makefile` 빌드 경로 추가 → `chore(makefile)`. 나눠서 두 번 커밋할까요?"

- 나누는 실행은 **사용자 승인 후**. 경로가 갈리면 경로별 `git add`, 한 파일 안에
  섞였으면 `git add -p` 를 쓰고, 스테이징을 임의로 재구성하지 않습니다.
- 라인 수로 판단하지 않습니다. 300줄 리팩터도 한 단위일 수 있고, 20줄이 두 기능일 수
  있습니다. 기준은 항상 "따로 되돌릴 수 있는가" 입니다.
- 사용자가 "한 번에 커밋해" 라고 하면 그대로 합니다. 이건 판단을 돕는 절차지 관문이
  아닙니다.

## 3. 형식 — README Commit Rule

```
type(scope): subject

body

footer
```

**header** `type(scope): subject`

- `type` 은 README 의 커밋 타입 표에서 고릅니다: `feat` `fix` `docs` `style`
  `refactor` `test` `chore` `build` `ci` `perf` `revert`. 애매하면 diff 가 동작을
  바꿨는지로 판단합니다 — 바꿨으면 `feat`/`fix`, 안 바꿨으면 `refactor`/`style`.
- `scope` 는 변경이 일어난 영역. 실제 경로에서 꺼냅니다 — `lambda` `spark` `airflow`
  `docker` `makefile` `dag`. 저장소 전반이면 생략 가능(`docs: ...`).
- `subject` 는 **한글, 50자 이내, 마침표 없음.** 명사형으로 끝냅니다
  (`~ 추가`, `~ 수정`, `~ 분리`, `~ 통일`).
- **타입이 두 개 떠오르면 커밋이 두 개입니다.** `feat` 과 `chore` 가 한 메시지에
  들어가려 하면 2단계로 돌아가 나누세요.

**body** — **기본은 비움.** subject 로 설명이 끝나면 쓰지 않습니다. 대부분의 커밋이
여기 해당합니다.

쓰는 경우는 하나입니다: **subject 만으로는 왜 바꿨는지 알 수 없을 때.** 실패 원인,
버린 대안, diff 에 안 보이는 제약 같은 것. "무엇을 했는지" 를 다시 쓰는 body 는
subject 와 diff 의 중복이니 지웁니다.

- **한 문단 = 한 줄.** 보기 좋게 줄을 끊지 않습니다(하드랩 금지) — 뷰어 폭에 따라
  문장이 토막나 보이고, 조사가 줄을 넘어가면 읽기 더 나빠집니다. 줄바꿈은 문단
  사이에만 넣습니다.
- **한 문단, 두 문장 안에서 끝냅니다.** 그보다 길어야 이해되는 변경이면 설명이
  부족한 게 아니라 커밋이 큰 것입니다 — 쪼개세요.
- 파일 경로·함수명으로 말합니다. "로직 일부 수정" 은 정보가 0입니다.
- 문체는 명사형 종결 또는 음슴체. 존댓말·다짐·사과를 넣지 않습니다.

**footer** — 이슈 연결과 파괴적 변경.

- 브랜치 이름에서 이슈 번호를 꺼냅니다. `fix/125-pipeline-core-image` → `Closes #125`.
  번호가 없으면 footer 를 비우세요(추측 금지).
- 하위 호환이 깨지면 `BREAKING CHANGE: <무엇이 깨지는지>`.

### 예시

나쁨 — 무엇이 바뀌었는지 알 수 없고, AI 트레일러가 붙었습니다:

```
fix: 버그 수정

문제가 되던 부분을 전반적으로 수정하였습니다. 테스트도 통과하는 것을 확인했습니다.

Co-Authored-By: Claude <noreply@anthropic.com>
```

좋음 — **기본형. body 가 없습니다:**

```
fix(lambda): gas price 핸들러 상수 참조 복구

Closes #125
```

좋음 — body 가 값을 하는 경우. subject 만으로는 "왜 하향인가" 를 알 수 없습니다
(한 문단, 한 줄):

```
build: pyarrow 를 glibc 2.26 호환 버전으로 하향

Lambda AL2 베이스의 glibc 가 2.26 이라 최신 휠이 로드되지 않음. 베이스 이미지 교체는 별도 이슈로 분리.
```

## 4. 커밋한다

메시지는 파일로 쓰고 `-F` 로 넘깁니다. 셸 인용부호에 한글·백틱·줄바꿈이 섞이면
메시지가 조용히 뭉개집니다.

```bash
git commit -F <스크래치패드>/commit-msg.txt
```

**커밋 전에 사용자에게 초안을 보여주고 승인을 받으세요.** 사용자가 이번 대화에서
명시적으로 커밋을 요청하지 않았다면 초안만 만들고 `git commit` 을 실행하지 않습니다.
커밋은 히스토리에 남는 작업이고, 되돌리려면 사용자가 손을 써야 합니다.

`--no-verify` 를 붙이지 않습니다. hook 이 막으면 그건 고쳐야 할 신호입니다.

## 5. 확인한다

```bash
git log -1 --format=%B
git log -1 --format=%B | grep -iE 'co-authored|claude|🤖' && echo "AI 트레일러 발견 — 제거 필요"
```

grep 이 걸리면 `git commit --amend -F <수정한 파일>` 로 고칩니다 (아직 push 하지
않았을 때만). 이미 push 했다면 사용자에게 알리고 판단을 맡기세요 — 공유 히스토리를
임의로 다시 쓰지 않습니다.

푸시는 이 스킬의 범위가 아닙니다. PR 은 `write-pr` 스킬을 쓰세요.
