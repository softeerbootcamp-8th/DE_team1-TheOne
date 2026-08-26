-- silver_lineage에 실행·코드·설정 식별자를 추가합니다.
-- 기존 Gold 버전은 당시 Airflow run/code/config를 복원할 근거가 없으므로 legacy
-- sentinel로 백필합니다. 새 코드 배포 뒤 생성되는 행부터 실제 식별자가 기록됩니다.

BEGIN;

ALTER TABLE silver_lineage
    ADD COLUMN IF NOT EXISTS airflow_run_id TEXT,
    ADD COLUMN IF NOT EXISTS code_sha TEXT,
    ADD COLUMN IF NOT EXISTS config_hash TEXT;

UPDATE silver_lineage
SET airflow_run_id = 'legacy__' || service_area || '__' || year_month || '__v' || version,
    code_sha = 'legacy-unknown',
    config_hash = 'legacy-config:' || service_area || ':' || year_month || ':v' || version
WHERE airflow_run_id IS NULL
   OR code_sha IS NULL
   OR config_hash IS NULL;

ALTER TABLE silver_lineage
    ALTER COLUMN airflow_run_id SET NOT NULL,
    ALTER COLUMN code_sha SET NOT NULL,
    ALTER COLUMN config_hash SET NOT NULL;

COMMIT;
