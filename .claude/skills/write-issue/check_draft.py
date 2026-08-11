#!/usr/bin/env python3
"""이슈/PR 초안을 등록 전에 검사합니다. 세 가지를 봅니다.

1. `작업 목적`(이슈) / `변경 사항 요약`(PR) 본문이 200자를 넘는가 — 사람이 눈으로
   세면 틀립니다. **다른 소제목은 길이를 재지 않습니다.** 체크리스트·재현 절차는
   작업이 늘면 같이 늘어나는 게 정상이라, 길이로 막으면 군더더기가 아니라 해야 할
   일을 지우게 됩니다.
2. 체크박스가 10개를 넘는가 (이슈만) — 넘으면 이슈 하나로 300줄·1~2일 기준을
   지킬 수 없습니다. 항목을 줄이지 말고 상위/하위 이슈로 나눠야 합니다.
3. 문단을 중간에서 하드랩했는가 — GitHub 이슈/PR 렌더러는 문단 안의 단일 줄바꿈도
   그대로 줄바꿈으로 표시합니다. 80자에서 끊어 쓰면 조사가 줄을 넘어가
   "`docker compose up -d`" / "가 실패했고" 처럼 문장이 토막나 보입니다.
   에디터에서 예뻐 보이는 것과 GitHub 에서 읽히는 것이 다릅니다.

    python3 check_draft.py draft.md
    python3 check_draft.py draft.md --kind pr        # 체크박스 개수 검사 생략
    python3 check_draft.py draft.md --limit 200 --max-checkbox 10

길이 세는 기준: 공백 포함, HTML 주석과 체크박스 기호는 제외.
군더더기를 재려는 것이지 마크업을 재려는 게 아니라서입니다.
"""

import argparse
import re
import sys
from pathlib import Path


def strip_meta(text: str) -> str:
    """작성자가 쓴 본문만 남깁니다."""
    # 이슈 템플릿 맨 앞의 YAML frontmatter 를 먼저 떼어냅니다.
    # 안 떼면 닫는 `---` 이 아래 구분선 처리에 걸려 본문이 통째로 날아갑니다.
    text = re.sub(r"\A---\n.*?\n---\n", "", text, flags=re.DOTALL)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    # 남은 `---` 아래는 PR 템플릿의 리뷰/머지 규칙 안내라 작성자가 쓴 글이 아닙니다.
    return text.split("\n---\n")[0]


def sections(text: str) -> list[tuple[str, str]]:
    text = strip_meta(text)

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


# 200자 제한을 적용하는 소제목. 나머지는 면제입니다 — 체크리스트가 늘어나는 건
# 이슈가 장황해진 게 아니라 할 일이 많아진 것이고, 그건 길이가 아니라 분할로 잡습니다.
LENGTH_LIMITED = {"작업목적", "변경사항요약"}

# 상한이 없는 소제목도 글자 수는 세서 보여줍니다. 상한 해제는 "길게 써도 된다" 가
# 아니라 "항목이 늘어나는 걸 막지 않는다" 는 뜻이라서입니다. 기준의 3배를 넘으면
# 막지는 않되 압축을 권합니다 — 대개 항목이 늘어난 게 아니라 설명이 늘어난 경우입니다.
SOFT_RATIO = 3


def is_limited(title: str) -> bool:
    return title.replace(" ", "") in LENGTH_LIMITED


# 공백류에 `\s` 를 쓰면 줄바꿈까지 먹어서 빈 항목(`- [ ]`)이 다음 줄을 삼킵니다.
CHECKBOX = re.compile(r"^[ \t]*[-*][ \t]*\[[ xX]\][ \t]*(.*)$", re.MULTILINE)


def checkboxes(text: str) -> list[str]:
    """작업량으로 세는 체크박스만 돌려줍니다.

    빈 항목(템플릿 그대로 남은 `- [ ]`)과 하위 이슈 참조(`- [ ] #211`)는 제외합니다.
    상위 이슈는 하위를 11개 나열해도 그 자체로 300줄을 넘기지 않기 때문입니다.
    """
    text = strip_meta(text)
    items = [m.group(1).strip() for m in CHECKBOX.finditer(text)]
    return [i for i in items if i and not re.match(r"#\d+", i)]


def body_length(body: str) -> int:
    """산문 길이만 잽니다.

    200자 제한은 "말이 장황하다"를 잡으려는 것이지 "명령어가 길다"를 잡으려는 게
    아닙니다. 코드블록·인라인코드까지 세면 정확한 경로와 복붙 가능한 명령을 지우는
    쪽으로 사람을 몰게 되는데, 그건 스킬이 시키는 것과 정반대입니다.
    """
    body = re.sub(r"```.*?```", "", body, flags=re.DOTALL)  # 명령·로그 블록
    body = re.sub(r"`[^`\n]+`", "", body)  # 경로·함수명·에러명
    body = re.sub(r"^[ \t]*-[ \t]*\[[ xX]\][ \t]*", "", body, flags=re.MULTILINE)
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
    parser.add_argument(
        "--kind",
        choices=("issue", "pr"),
        default="issue",
        help="issue: 체크박스 개수도 검사 (기본). pr: 길이만 검사",
    )
    parser.add_argument("--max-checkbox", type=int, default=10)
    args = parser.parse_args()

    text = args.path.read_text(encoding="utf-8")
    found = sections(text)
    if not found:
        print("!! `## ` 소제목을 못 찾았습니다. 템플릿 소제목을 그대로 쓰세요.")
        return 2

    over = 0
    for title, body in found:
        n = body_length(body)
        if not is_limited(title):
            hint = "  ← 상한은 없지만 압축 검토" if n > args.limit * SOFT_RATIO else ""
            print(f"info {n:>4}자  ## {title} (상한 없음){hint}")
            continue
        if n > args.limit:
            over += 1
        print(f"{'OVER' if n > args.limit else 'ok  '} {n:>4}자  ## {title}")

    if over:
        print(f"\n{over}개 소제목이 {args.limit}자를 넘습니다. 배경 설명과 다짐부터 지우세요.")

    split = False
    if args.kind == "issue":
        boxes = checkboxes(text)
        split = len(boxes) > args.max_checkbox
        print(f"{'OVER' if split else 'ok  '} {len(boxes):>4}개  체크박스")
        if split:
            print(
                f"\n체크박스가 {len(boxes)}개입니다 (상한 {args.max_checkbox}). "
                "항목을 지우지 말고 상위/하위 이슈로 분할하세요 — SKILL.md 3절 참고."
            )

    wraps = hard_wraps(text)
    if wraps:
        print(f"\n문단 중간에서 끊긴 줄 {len(wraps)}개 — 앞줄에 이어 붙이세요:")
        for lineno, line in wraps:
            print(f"  {lineno}행: {line[:60]}")
        print("  (한 문단 = 한 줄. 줄바꿈은 문단·목록 항목 사이에만.)")

    return 1 if (over or split or wraps) else 0


def self_check() -> int:
    """규칙을 바꾼 뒤 돌려보는 자체 검사 — `python3 check_draft.py --self-check`."""
    assert is_limited("작업 목적") and is_limited("변경사항 요약")
    assert not is_limited("완료 조건 (Definition of Done)")
    assert not is_limited("작업 체크리스트")

    draft = "## 완료 조건\n\n- [ ] 첫 항목\n- [x] 끝난 항목\n- [ ]\n- [ ] #211 하위 이슈\n"
    assert checkboxes(draft) == ["첫 항목", "끝난 항목"], checkboxes(draft)

    long_body = "가" * 300
    assert body_length(f"`{long_body}`") == 0  # 백틱 안은 세지 않음
    assert body_length(long_body) == 300
    print("self-check ok")
    return 0


if __name__ == "__main__":
    if "--self-check" in sys.argv:
        sys.exit(self_check())
    sys.exit(main())
