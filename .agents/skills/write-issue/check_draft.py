#!/usr/bin/env python3
"""이슈/PR 초안을 등록 전에 검사합니다. 두 가지를 봅니다.

1. 소제목(`## `) 본문이 200자를 넘는가 — 사람이 눈으로 세면 틀립니다.
2. 문단을 중간에서 하드랩했는가 — GitHub 이슈/PR 렌더러는 문단 안의 단일 줄바꿈도
   그대로 줄바꿈으로 표시합니다. 80자에서 끊어 쓰면 조사가 줄을 넘어가
   "`docker compose up -d`" / "가 실패했고" 처럼 문장이 토막나 보입니다.
   에디터에서 예뻐 보이는 것과 GitHub 에서 읽히는 것이 다릅니다.

    python3 check_draft.py draft.md
    python3 check_draft.py draft.md --limit 200

길이 세는 기준: 공백 포함, HTML 주석과 체크박스 기호는 제외.
군더더기를 재려는 것이지 마크업을 재려는 게 아니라서입니다.
"""

import argparse
import re
import sys
from pathlib import Path


def sections(text: str) -> list[tuple[str, str]]:
    # 이슈 템플릿 맨 앞의 YAML frontmatter 를 먼저 떼어냅니다.
    # 안 떼면 닫는 `---` 이 아래 구분선 처리에 걸려 본문이 통째로 날아갑니다.
    text = re.sub(r"\A---\n.*?\n---\n", "", text, flags=re.DOTALL)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    # 남은 `---` 아래는 PR 템플릿의 리뷰/머지 규칙 안내라 작성자가 쓴 글이 아닙니다.
    text = text.split("\n---\n")[0]

    out, title, buf = [], None, []
    for line in text.splitlines():
        if line.startswith("## "):
            if title is not None:
                out.append((title, "\n".join(buf)))
            title, buf = line[3:].strip(), []
        elif title is not None:
            buf.append(line)
    if title is not None:
        out.append((title, "\n".join(buf)))
    return out


def body_length(body: str) -> int:
    """산문 길이만 잽니다.

    200자 제한은 "말이 장황하다"를 잡으려는 것이지 "명령어가 길다"를 잡으려는 게
    아닙니다. 코드블록·인라인코드까지 세면 정확한 경로와 복붙 가능한 명령을 지우는
    쪽으로 사람을 몰게 되는데, 그건 스킬이 시키는 것과 정반대입니다.
    """
    body = re.sub(r"```.*?```", "", body, flags=re.DOTALL)  # 명령·로그 블록
    body = re.sub(r"`[^`\n]+`", "", body)  # 경로·함수명·에러명
    body = re.sub(r"^\s*-\s*\[[ xX]\]\s*", "", body, flags=re.MULTILINE)
    return len(body.strip())


# 새 블록을 여는 줄들 — 이 줄로 시작하면 앞줄의 이어쓰기가 아닙니다.
# 목록 기호 뒤는 공백이거나 줄끝입니다 (템플릿의 빈 항목 `2.` 를 오탐하지 않도록).
BLOCK_START = re.compile(r"\s*(#{1,6}\s|[-*+](\s|$)|\d+[.)](\s|$)|\||>|```)")


def hard_wraps(text: str) -> list[tuple[int, str]]:
    """문단 중간에서 끊긴 줄(= 앞줄에 이어붙어야 할 줄)을 찾습니다."""
    # 이슈 템플릿의 YAML frontmatter 와 HTML 주석은 템플릿이 준 메타·작성 안내라
    # 사람이 쓴 본문이 아닙니다. 행 번호를 유지하려고 줄 수만큼 빈 줄로 바꿉니다.
    blank = lambda m: "\n" * m.group(0).count("\n")  # noqa: E731
    text = re.sub(r"\A---\n.*?\n---\n", blank, text, flags=re.DOTALL)
    text = re.sub(r"<!--.*?-->", blank, text, flags=re.DOTALL)

    out, in_code, prev = [], False, ""
    for i, line in enumerate(text.splitlines(), 1):
        if line.lstrip().startswith("```"):
            in_code = not in_code
            prev = line
            continue
        if in_code or not line.strip():
            prev = line
            continue
        # 앞줄이 내용이 있고, 이 줄이 새 블록을 열지 않으면 이어쓰기 = 하드랩.
        if prev.strip() and not BLOCK_START.match(line):
            out.append((i, line.strip()))
        prev = line
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()

    found = sections(args.path.read_text(encoding="utf-8"))
    if not found:
        print("!! `## ` 소제목을 못 찾았습니다. 템플릿 소제목을 그대로 쓰세요.")
        return 2

    over = 0
    for title, body in found:
        n = body_length(body)
        flag = "OVER" if n > args.limit else "ok  "
        if n > args.limit:
            over += 1
        print(f"{flag} {n:>4}자  ## {title}")

    if over:
        print(f"\n{over}개 소제목이 {args.limit}자를 넘습니다. 배경 설명과 다짐부터 지우세요.")

    wraps = hard_wraps(args.path.read_text(encoding="utf-8"))
    if wraps:
        print(f"\n문단 중간에서 끊긴 줄 {len(wraps)}개 — 앞줄에 이어 붙이세요:")
        for lineno, line in wraps:
            print(f"  {lineno}행: {line[:60]}")
        print("  (한 문단 = 한 줄. 줄바꿈은 문단·목록 항목 사이에만.)")

    return 1 if (over or wraps) else 0


if __name__ == "__main__":
    sys.exit(main())
