#!/usr/bin/env python3
"""코드 변경이 있는 커밋·PR 전에 `review-engineering` 검토를 거치게 만드는 훅입니다.

Claude Code 의 PreToolUse 훅으로 등록해 두면 Bash 도구 호출을 가로채고, 그 명령이
`git commit` 또는 `gh pr create` 일 때만 개입합니다. 검토 기록이 없으면 exit 2 로
막고, 무엇을 하라는 안내를 stderr 로 돌려줍니다 (Claude 가 그 메시지를 읽습니다).

    등록:  .claude/settings.json 의 hooks.PreToolUse
    통과:  python3 .claude/hooks/review_gate.py --pass commit|pr

문서·스킬·설정·이미지만 바뀌면 자동 통과합니다. 검토 기록은 **코드 변경 내용의 해시**로
남습니다. 검토 후 코드를 더 고치면 해시가 달라져 다시 검토를 요구합니다 — 검토한 것과
커밋하는 것이 같은 물건이어야 의미가 있습니다.

기록은 `.git/review-cache/` 에 두고 git 에는 올리지 않습니다. 이 게이트는
암호학적 강제가 아니라 **검토 단계를 흐름에 끼워 넣는 장치**입니다. 사람이 판단해서
건너뛰어야 할 때는 `--pass` 를 직접 실행하면 됩니다.
"""

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

CACHE_DIR_NAME = "review-cache"
CODE_SUFFIXES = {
    ".py",
    ".pyi",
    ".sql",
    ".sh",
    ".bash",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".java",
    ".scala",
}
CODE_FILENAMES = {"Dockerfile"}


class Stage(NamedTuple):
    """단계 하나의 전부. 트리거·검토 범위·안내 문구를 따로 둔 dict 로 흩으면
    단계를 추가할 때 세 곳을 고쳐야 하고, 한 곳을 잊으면 KeyError 로 훅이 죽습니다."""

    # 셸 구분자로 쪼갠 조각의 맨 앞에서 찾습니다 (`segments()` 참고).
    needles: tuple[str, ...]
    diff: list[str]
    label: str
    # PR 범위는 원격 기준이라, 로컬 원격 추적이 오래됐으면 조용히 잘못된 범위를 검토합니다.
    refresh: list[str] | None = None


class DiffState(NamedTuple):
    """검토 대상 diff의 획득 여부와 지문.

    빈 diff는 정상 상태이므로 `digest=None`만으로는 원격 ref 누락·git 오류와
    구분할 수 없습니다. Git 훅은 후자를 통과시키면 안 되므로 별도로 보존합니다.
    """

    available: bool
    digest: str | None


STAGES = {
    "commit": Stage(
        needles=("git commit",),
        diff=["git", "diff", "--cached"],
        label="커밋 대상(staged diff)",
    ),
    "pr": Stage(
        needles=("gh pr create",),
        diff=["git", "diff", "origin/develop...HEAD"],
        label="PR 대상(origin/develop...HEAD)",
        refresh=["git", "fetch", "origin", "develop", "-q"],
    ),
}


def run(cmd: list[str]) -> str:
    code, stdout = run_result(cmd)
    return stdout if code == 0 else ""


def run_result(cmd: list[str]) -> tuple[int, str]:
    try:
        done = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return 1, ""
    return done.returncode, done.stdout


def is_code_path(path: str) -> bool:
    """review-engineering을 강제할 실행 코드 경로인지 판정합니다."""
    name = Path(path).name
    return name in CODE_FILENAMES or Path(name).suffix.lower() in CODE_SUFFIXES


def diff_state(stage: str, *, refresh: bool = True) -> DiffState:
    """코드 diff를 읽고 비코드 변경과 읽기 실패를 구분합니다."""
    spec = STAGES[stage]
    if refresh and spec.refresh:
        run(spec.refresh)  # Claude 경로와 `--pass`는 기존처럼 최신 원격을 시도합니다.
    code, names = run_result([*spec.diff, "--name-only", "-z"])
    if code != 0:
        return DiffState(available=False, digest=None)
    code_paths = [path for path in names.split("\0") if path and is_code_path(path)]
    if not code_paths:
        return DiffState(available=True, digest=None)
    code, diff = run_result([*spec.diff, "--", *code_paths])
    if code != 0:
        return DiffState(available=False, digest=None)
    if not diff.strip():
        return DiffState(available=True, digest=None)
    return DiffState(available=True, digest=hashlib.sha256(diff.encode("utf-8")).hexdigest()[:16])


def diff_hash(stage: str, *, refresh: bool = True) -> str | None:
    """코드 변경의 지문. 코드 변경이 없으면 None (검토할 게 없음)."""
    return diff_state(stage, refresh=refresh).digest


def cache_dir() -> Path:
    """기록 위치는 저장소의 실제 git 디렉터리 기준입니다.

    훅이 어느 디렉터리에서 불릴지 보장되지 않아서, cwd 를 믿으면 서브디렉터리에서
    커밋할 때 기록을 못 찾고 계속 막습니다. `--show-toplevel` + `.git` 은 worktree에서
    깨집니다 — worktree의 `.git`은 디렉터리가 아니라 실제 git 디렉터리를 가리키는
    포인터 파일이라 그 아래에 mkdir 할 수 없습니다. `--absolute-git-dir`은 일반
    checkout과 worktree 모두에서 실제 git 디렉터리를 절대경로로 돌려줍니다.
    """
    git_dir = run(["git", "rev-parse", "--absolute-git-dir"]).strip()
    return (Path(git_dir) if git_dir else Path(".git")) / CACHE_DIR_NAME


def marker(stage: str, digest: str) -> Path:
    return cache_dir() / f"{stage}-{digest}"


def record(stage: str) -> int:
    digest = diff_hash(stage)
    if digest is None:
        print(f"검토할 변경이 없습니다 ({STAGES[stage].label} 가 비어 있음).")
        return 0
    target = marker(stage, digest)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("reviewed\n", encoding="utf-8")
    print(f"검토 기록 완료: {stage} {digest}")
    return 0


def block(stage: str) -> int:
    """누락된 검토 기록의 공통 안내를 출력합니다."""
    what = STAGES[stage].label
    sys.stderr.write(
        f"[review-engineering] {what} 에 대한 검토 기록이 없습니다.\n\n"
        f"1. `review-engineering` 스킬로 {what} 를 평가하세요 "
        "(과잉/부족/적정 3분류 + 근거 명령과 출력).\n"
        "2. 판정 결과를 사용자에게 보여주세요. 차단 사유가 없으면 그렇게 쓰면 됩니다.\n"
        f"3. 그다음 `python3 .claude/hooks/review_gate.py --pass {stage}` 로 기록하고 "
        "명령을 다시 실행하세요.\n\n"
        "검토를 건너뛰고 기록만 남기지 마세요. 그러면 이 게이트가 존재할 이유가 없습니다.\n"
    )
    return 2


def check(stage: str) -> int:
    """Git 훅이 호출하는 오프라인 검토 기록 검사.

    이 함수는 `git fetch`를 하지 않습니다. PR 훅은 이미 로컬에 있는
    `origin/develop`을 기준으로 검토 기록을 대조하므로, 훅 실행이 네트워크 상태에
    따라 느려지거나 멈추지 않습니다.
    """
    state = diff_state(stage, refresh=False)
    if not state.available:
        if stage == "pr":
            sys.stderr.write(
                "[review-engineering] PR 기준 `origin/develop...HEAD` diff를 확인할 수 없습니다.\n"
                "`git fetch origin develop`로 기준 ref를 준비한 뒤 다시 푸시하세요.\n"
            )
        else:
            sys.stderr.write("[review-engineering] 커밋 대상 diff를 확인할 수 없습니다.\n")
        return 2

    digest = state.digest
    if digest is None:
        return 0
    if marker(stage, digest).exists():
        return 0
    return block(stage)


# 셸 구분자. `cd x && git commit` 을 잡으려면 부분 문자열로 봐야 하는데, 그러면
# `echo "git commit"` 이나 `rg "git commit"` 처럼 **언급만 한 명령까지** 걸립니다.
# 그래서 구분자로 쪼갠 뒤 각 조각의 **맨 앞**에서만 트리거를 찾습니다.
SEPARATORS = re.compile(r"&&|\|\||[;|\n]")

# 인용부호 안의 내용은 셸에 넘길 데이터지 실행할 명령이 아닙니다. 먼저 지워야
# `... "cd x && git commit" ...` 같은 문자열이 조각을 가르지 않습니다.
QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")


def segments(command: str) -> list[str]:
    stripped = QUOTED.sub(" ", command)
    return [seg.strip() for seg in SEPARATORS.split(stripped) if seg.strip()]


def stage_of(command: str) -> str | None:
    for stage, spec in STAGES.items():
        for seg in segments(command):
            if any(seg.startswith(n) for n in spec.needles):
                return stage
    return None


def target_stage(event: dict) -> str | None:
    """이 훅 이벤트가 개입할 단계. 개입하지 않으면 None.

    stdin 파싱과 분리해 둔 이유는 자체 검사에서 이 판단만 따로 돌려보기 위함입니다 —
    여기서 실수하면 훅이 모든 Bash 호출을 막아 세션이 통째로 멈춥니다.
    """
    if event.get("tool_name") != "Bash":
        return None
    return stage_of((event.get("tool_input") or {}).get("command", ""))


def gate() -> int:
    try:
        event = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0  # 훅 입력을 못 읽으면 개발을 막지 않습니다.

    stage = target_stage(event)
    if stage is None:
        return 0

    # Claude PreToolUse는 기존처럼 PR 직전에 origin/develop 갱신을 시도합니다.
    # Git 훅 전용 `--check`만 오프라인 검사로 분리합니다.
    digest = diff_hash(stage)
    if digest is None:
        return 0
    if marker(stage, digest).exists():
        return 0
    return block(stage)  # PreToolUse 에서 2 = 도구 호출 차단 + stderr 를 Claude 에게 전달


def main() -> int:
    if "--pass" in sys.argv:
        i = sys.argv.index("--pass")
        stage = sys.argv[i + 1] if len(sys.argv) > i + 1 else ""
        if stage not in STAGES:
            print(f"사용법: --pass {'|'.join(STAGES)}", file=sys.stderr)
            return 1
        return record(stage)
    if "--check" in sys.argv:
        i = sys.argv.index("--check")
        stage = sys.argv[i + 1] if len(sys.argv) > i + 1 else ""
        if stage not in STAGES:
            print(f"사용법: --check {'|'.join(STAGES)}", file=sys.stderr)
            return 1
        return check(stage)
    if "--self-check" in sys.argv:
        return self_check()
    return gate()


def self_check() -> int:
    """훅을 고친 뒤 돌려보는 자체 검사."""
    assert stage_of("git add -A && git commit -F msg.txt") == "commit"
    assert stage_of("gh pr create --base develop") == "pr"
    assert stage_of("git status") is None
    assert stage_of("gh pr view 195") is None
    # 언급만 한 명령은 잡지 않습니다 (오탐이면 팀원이 훅을 끕니다).
    assert stage_of("""echo '{"command":"git commit"}' | python3 x.py""") is None
    assert stage_of('rg -n "git commit" docs/') is None
    # 인용부호 안의 `&&` 가 조각을 가르면 안 됩니다 (여기서 실제로 오탐이 났습니다).
    assert stage_of('python3 -c \'x = "cd a && git commit -m y"\'') is None
    assert stage_of("cd airflow && git commit -m x") == "commit"
    assert stage_of("git push -u origin HEAD && gh pr create --base develop") == "pr"

    # 훅이 개입하지 않아야 하는 입력들. 여기가 틀리면 모든 Bash 호출이 막힙니다.
    assert target_stage({"tool_name": "Read", "tool_input": {"file_path": "x"}}) is None
    assert target_stage({"tool_name": "Bash", "tool_input": {"command": "ls -la"}}) is None
    assert target_stage({}) is None
    assert target_stage({"tool_name": "Bash"}) is None
    assert target_stage({"tool_name": "Bash", "tool_input": {"command": "git commit"}}) == "commit"

    # 단계 정의가 한 곳에 모여 있는지 — 흩어지면 단계 추가 때 KeyError 로 죽습니다.
    for name, spec in STAGES.items():
        assert spec.needles and spec.diff and spec.label, name

    assert marker("commit", "abc").name == "commit-abc"
    assert marker("commit", "abc").is_absolute(), "기록 위치는 저장소 루트 기준이어야 함"
    assert is_code_path("main/airflow/dags/example.py")
    assert is_code_path("queries/monthly.sql")
    assert is_code_path("shared/airflow/Dockerfile")
    assert not is_code_path("docs/design.md")
    assert not is_code_path(".agents/skills/write-pr/SKILL.md")
    assert not is_code_path("config/generation.json")
    print("self-check ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
