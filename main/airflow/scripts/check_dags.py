"""DAG 파일이 실제로 import 되는지 검증합니다.

Airflow 는 import 에 실패한 DAG 를 조용히 건너뜁니다. 에러를 띄우는 게 아니라
목록에서 사라지기 때문에, 보통 배포한 뒤 "그 DAG 가 화면에 안 보인다" 로 발견됩니다.
그 전에 걸러내려고 둡니다.

    로컬:  cd main/airflow && .venv/bin/python scripts/check_dags.py
    CI:    airflow uv 환경에서 같은 파일을 실행합니다
           (.github/workflows/ci.yml 의 dag-import job)

DAG 를 정의하는 부수효과(스케줄 등록 등)는 없습니다. import 만 해봅니다.
"""

import importlib.util
import pathlib
import sys
import traceback

DEFAULT_DAGS_DIR = pathlib.Path(__file__).resolve().parent.parent / "dags"


def main(argv: list[str]) -> int:
    dags_dir = pathlib.Path(argv[1]) if len(argv) > 1 else DEFAULT_DAGS_DIR
    if not dags_dir.is_dir():
        print(f"DAG 폴더가 없습니다: {dags_dir}", file=sys.stderr)
        return 1

    # 실제 Airflow 와 같은 경로 구성. airflow/ 가 먼저 있어야 데이터셋별 scripts 를
    # 저장소 루트의 동명 scripts 패키지로 잘못 불러오지 않습니다.
    sys.path.insert(0, str(dags_dir))
    sys.path.insert(0, str(dags_dir.parent))
    # DAG 가 import 하는 shared/ 는 저장소 루트에 있습니다. 컨테이너는 PYTHONPATH,
    # 테스트는 pyproject 의 pythonpath 로 넣어주는 그 경로입니다 (#485).
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[3]))

    files = sorted(dags_dir.glob("*_dag.py"))
    if not files:
        # 0건을 성공으로 처리하면 파일명 규칙이 바뀐 걸 눈치채지 못합니다.
        print(f"검사할 DAG 파일이 없습니다: {dags_dir}", file=sys.stderr)
        return 1

    failed = 0
    for path in files:
        spec = importlib.util.spec_from_file_location(path.stem, path)
        try:
            spec.loader.exec_module(importlib.util.module_from_spec(spec))
            print(f"OK   {path.name}")
        except Exception:
            failed += 1
            print(f"FAIL {path.name}")
            traceback.print_exc()

    print(f"\n{len(files)}개 중 {failed}개 실패")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
