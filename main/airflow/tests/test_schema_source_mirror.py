"""`schema/source` 가 `schema/bronze` 의 정확한 복제본인지 확인합니다.

sub/ 는 schema/bronze·silver·gold 를 참조하지 않는다는 경계(#529·#540) 때문에
같은 스키마가 두 곳에 존재합니다. 그 복제를 **손으로** 유지하는 동안 실제로 갈렸습니다 —
`weekly_price_usd` -> `weekly_lease_fee` 통일 때 한쪽만 따라갔고, 기사 데이터는 bronze 가
15컬럼 월별 스냅샷으로 바뀐 뒤에도 source 는 9컬럼 리스 계약으로 남아 있었습니다.

경계를 유지하려면 동등성을 테스트가 붙들어야 합니다. 이 파일은 **검사만** 하므로
sub/ 프로덕션 코드가 schema.bronze 를 import 하는 것과 다릅니다.

1. 거울 대상 세 스키마가 컬럼명·타입·순서까지 같다
2. 거울 목록에 적힌 이름이 양쪽에 실재한다 — 이름이 바뀌면 검사에서 조용히 빠짐
"""

import pytest

from schema import bronze, source

# (schema/bronze 의 이름, schema/source 의 이름)
MIRRORED = [
    ("MONTHLY_TAXI_TRIP_SCHEMA", "MONTHLY_TAXI_TRIP_SCHEMA"),
    ("DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA", "DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA"),
    ("LEASE_VEHICLE_INVENTORY_SCHEMA", "LEASE_VEHICLE_INVENTORY_SCHEMA"),
]


@pytest.mark.parametrize(("bronze_name", "source_name"), MIRRORED)
def test_거울_스키마는_컬럼명_타입_순서까지_같다(bronze_name, source_name):
    expected = getattr(bronze, bronze_name)
    actual = getattr(source, source_name)

    # pa.Schema 의 == 는 필드 순서와 타입까지 봅니다.
    assert actual == expected, (
        f"schema/source.{source_name} 이 schema/bronze.{bronze_name} 과 다릅니다.\n"
        f"  bronze: {list(zip(expected.names, map(str, expected.types)))}\n"
        f"  source: {list(zip(actual.names, map(str, actual.types)))}"
    )


@pytest.mark.parametrize(("bronze_name", "source_name"), MIRRORED)
def test_거울_목록의_이름이_양쪽에_실재한다(bronze_name, source_name):
    """이름이 바뀌면 `getattr` 이 죽어야 합니다.

    목록에서 조용히 빠지면 그 스키마는 검사 대상이 아닌 채로 갈라집니다.
    """
    assert hasattr(bronze, bronze_name), f"schema/bronze 에 {bronze_name} 이 없습니다"
    assert hasattr(source, source_name), f"schema/source 에 {source_name} 이 없습니다"


def test_기사_스냅샷의_nullable_은_exit_date_뿐이다():
    """`exit_date` 는 재직 중이면 NULL 입니다. 나머지가 비면 하류가 조용히 틀립니다."""
    names = set(source.DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA.names)

    assert source.DRIVER_VEHICLE_MONTHLY_SNAPSHOT_REQUIRED_NON_NULL == names - {"exit_date"}
