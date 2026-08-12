"""`review_gate.py`의 Git 훅용 검토 기록 검사를 검증합니다."""

import importlib.util
import io
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from contextlib import redirect_stderr
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
GATE_PATH = ROOT / ".claude/hooks/review_gate.py"

spec = importlib.util.spec_from_file_location("review_gate", GATE_PATH)
assert spec and spec.loader
review_gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review_gate)


class ReviewGateCheckTest(unittest.TestCase):
    def test_check_requires_matching_marker_without_refreshing_remote(self) -> None:
        """Git 훅 검사는 네트워크 fetch 없이 같은 해시의 기록만 허용한다."""
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp)
            with (
                patch.object(review_gate, "cache_dir", return_value=cache),
                patch.object(
                    review_gate,
                    "diff_state",
                    return_value=review_gate.DiffState(available=True, digest="same-diff"),
                ) as diff_state,
            ):
                self.assertEqual(review_gate.check("commit"), 2)
                diff_state.assert_called_once_with("commit", refresh=False)

                (cache / "commit-same-diff").parent.mkdir(parents=True, exist_ok=True)
                (cache / "commit-same-diff").write_text("reviewed\n", encoding="utf-8")

                self.assertEqual(review_gate.check("commit"), 0)

    def test_check_pr_fails_closed_when_its_diff_cannot_be_resolved(self) -> None:
        """오프라인 pre-push는 origin/develop이 없으면 통과시키지 않는다."""
        with patch.object(
            review_gate,
            "diff_state",
            return_value=review_gate.DiffState(available=False, digest=None),
        ), redirect_stderr(io.StringIO()) as stderr:
            self.assertEqual(review_gate.check("pr"), 2)
        self.assertIn("origin/develop", stderr.getvalue())

    def test_check_allows_an_empty_diff_after_a_successful_read(self) -> None:
        with patch.object(
            review_gate,
            "diff_state",
            return_value=review_gate.DiffState(available=True, digest=None),
        ):
            self.assertEqual(review_gate.check("pr"), 0)

    def test_claude_gate_refreshes_before_comparing_its_marker(self) -> None:
        """Claude PreToolUse 경로는 기존처럼 PR 기준 ref를 갱신한다."""
        event = {"tool_name": "Bash", "tool_input": {"command": "gh pr create"}}
        with (
            patch.object(review_gate, "json") as json_module,
            patch.object(
                review_gate,
                "diff_state",
                return_value=review_gate.DiffState(available=True, digest="fresh-diff"),
            ) as diff_state,
            patch.object(review_gate, "marker", return_value=Path("missing-marker")),
            patch.object(review_gate.sys, "stdin", io.StringIO("{}")),
            redirect_stderr(io.StringIO()),
        ):
            json_module.load.return_value = event
            self.assertEqual(review_gate.gate(), 2)
        diff_state.assert_called_once_with("pr", refresh=True)

    def test_git_hooks_call_check_from_repository_root(self) -> None:
        expected = {
            ROOT / ".githooks/pre-commit": "--check commit",
            ROOT / ".githooks/pre-push": "--check pr",
        }
        for hook, check_arg in expected.items():
            content = hook.read_text(encoding="utf-8")
            self.assertIn('git rev-parse --show-toplevel', content)
            self.assertIn(".claude/hooks/review_gate.py", content)
            self.assertIn(check_arg, content)

    def test_pre_push_checks_only_non_deletion_branch_pushes(self) -> None:
        hook = ROOT / ".githooks/pre-push"
        zero = "0" * 40
        branch_push = f"refs/heads/chore/x {'1' * 40} refs/heads/chore/x {zero}\n"
        branch_delete = f"(delete) {zero} refs/heads/chore/x {'1' * 40}\n"
        tag_push = f"refs/tags/v1 {'1' * 40} refs/tags/v1 {zero}\n"

        self.assertEqual(self.run_pre_push(hook, branch_push, python_exit=17), (17, "--check pr"))
        self.assertEqual(self.run_pre_push(hook, branch_delete), (0, ""))
        self.assertEqual(self.run_pre_push(hook, tag_push), (0, ""))

    def run_pre_push(self, hook: Path, stdin: str, python_exit: int = 0) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as tmp:
            tool_dir = Path(tmp)
            capture = tool_dir / "python3-args"
            fake_python = tool_dir / "python3"
            fake_python.write_text(
                "#!/usr/bin/env sh\nprintf '%s %s' \"$2\" \"$3\" > \"$HOOK_CAPTURE\"\nexit \"${HOOK_PYTHON_EXIT:-0}\"\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)
            env = os.environ | {
                "PATH": f"{tool_dir}:{os.environ['PATH']}",
                "HOOK_CAPTURE": str(capture),
                "HOOK_PYTHON_EXIT": str(python_exit),
            }
            done = subprocess.run([str(hook)], input=stdin, text=True, capture_output=True, cwd=ROOT, env=env)
            return done.returncode, capture.read_text(encoding="utf-8") if capture.exists() else ""


if __name__ == "__main__":
    unittest.main()
