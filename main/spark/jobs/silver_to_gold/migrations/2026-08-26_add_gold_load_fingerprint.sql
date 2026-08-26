-- 같은 입력 버전과 추천 설정을 다시 적재해 중복 Gold 버전을 만드는 일을 막습니다. (#1054)
--
-- 기존 버전이 어떤 입력과 설정으로 만들어졌는지는 복원할 수 없습니다. 따라서 기존
-- 행에는 지역·월 안에서 겹치지 않는 legacy key를 넣습니다. 이 마이그레이션 이후에
-- 만들어지는 행부터 SHA-256 fingerprint를 기록합니다.

BEGIN;

ALTER TABLE gold_load_versions
    ADD COLUMN IF NOT EXISTS load_fingerprint TEXT;

UPDATE gold_load_versions
SET load_fingerprint = 'legacy-version:' || version::TEXT
WHERE load_fingerprint IS NULL;

ALTER TABLE gold_load_versions
    ALTER COLUMN load_fingerprint SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'gold_load_versions_load_fingerprint_key'
          AND conrelid = 'gold_load_versions'::regclass
    ) THEN
        ALTER TABLE gold_load_versions
            ADD CONSTRAINT gold_load_versions_load_fingerprint_key
            UNIQUE (service_area, year_month, load_fingerprint);
    END IF;
END
$$;

COMMIT;
