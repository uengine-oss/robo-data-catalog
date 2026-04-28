-- =====================================================================
-- public.infer_fk_candidates() — sample 값 overlap 기반 FK 추론 함수
--
-- 호출 예:
--   SELECT * FROM public.infer_fk_candidates('RWIS', 0.8, 2);
--
-- 알고리즘은 PostgreSQL 내부에서 완결 (클라이언트 round-trip 0).
-- =====================================================================

CREATE OR REPLACE FUNCTION public.infer_fk_candidates(
  p_schema           TEXT    DEFAULT 'RWIS',
  p_threshold        NUMERIC DEFAULT 0.8,
  p_min_src_distinct INT     DEFAULT 2
)
RETURNS TABLE (
  src_schema    TEXT,
  src_table     TEXT,
  src_column    TEXT,
  tgt_schema    TEXT,
  tgt_table     TEXT,
  tgt_column    TEXT,
  src_distinct  INT,
  overlap_count INT,
  overlap_ratio NUMERIC,
  dtype_family  TEXT
) AS $$
DECLARE
  v_excluded_cols CONSTANT TEXT[] := ARRAY[
    'LOG_TIME', 'SYSDT', 'SYS_TIME', 'REG_TIME', 'CREATE_DT', 'UPDATE_DT',
    'USEYN', 'USE_YN',
    'TAG_DESC', 'BNB_NAME', 'BNB_DESC', 'TAG_FULL', 'TAG_BASE',
    'DSP_BASE', 'DSP_FULL', 'ON_MSG', 'OFF_MSG'
  ];

  v_pair         RECORD;
  v_src_distinct INT;
  v_overlap      INT;
  v_ratio        NUMERIC;
BEGIN
  -- ANALYZE는 함수 안에서 수행 불가 (자동 commit 필요) → 호출자 측에서 사전 수행 가정.

  -- candidate column pool (TEMP — 함수 종료 시 자동 정리)
  CREATE TEMP TABLE _fk_pool ON COMMIT DROP AS
  WITH populated AS (
    SELECT relname AS table_name
    FROM pg_stat_user_tables
    WHERE schemaname = p_schema AND n_live_tup > 0
  )
  SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    CASE
      WHEN c.data_type IN ('integer','bigint','smallint','numeric','real','double precision') THEN 'NUMERIC'
      WHEN c.data_type IN ('character varying','character','text') THEN 'STRING'
      ELSE 'OTHER'
    END AS dtype_family
  FROM information_schema.columns c
  JOIN populated p ON p.table_name = c.table_name
  WHERE c.table_schema = p_schema
    AND c.column_name <> ALL (v_excluded_cols);

  CREATE INDEX ON _fk_pool (dtype_family, table_name);

  FOR v_pair IN
    SELECT
      a.table_name AS src_table, a.column_name AS src_column,
      b.table_name AS tgt_table, b.column_name AS tgt_column,
      a.dtype_family
    FROM _fk_pool a
    JOIN _fk_pool b
      ON a.dtype_family = b.dtype_family
     AND a.table_name <> b.table_name
  LOOP
    BEGIN
      EXECUTE format(
        'SELECT
           COUNT(DISTINCT a.%I)::INT,
           COUNT(DISTINCT a.%I) FILTER (
             WHERE EXISTS (
               SELECT 1 FROM %I.%I b
               WHERE b.%I = a.%I AND b.%I IS NOT NULL
             )
           )::INT
         FROM %I.%I a
         WHERE a.%I IS NOT NULL',
         v_pair.src_column, v_pair.src_column,
         p_schema, v_pair.tgt_table,
         v_pair.tgt_column, v_pair.src_column, v_pair.tgt_column,
         p_schema, v_pair.src_table,
         v_pair.src_column
      ) INTO v_src_distinct, v_overlap;
    EXCEPTION WHEN OTHERS THEN
      CONTINUE; -- dtype mismatch 등 런타임 에러는 skip
    END;

    IF v_src_distinct < p_min_src_distinct THEN
      CONTINUE;
    END IF;

    v_ratio := v_overlap::NUMERIC / v_src_distinct;
    IF v_ratio < p_threshold THEN
      CONTINUE;
    END IF;

    -- 결과 행 반환
    src_schema    := p_schema;
    src_table     := v_pair.src_table;
    src_column    := v_pair.src_column;
    tgt_schema    := p_schema;
    tgt_table     := v_pair.tgt_table;
    tgt_column    := v_pair.tgt_column;
    src_distinct  := v_src_distinct;
    overlap_count := v_overlap;
    overlap_ratio := v_ratio;
    dtype_family  := v_pair.dtype_family;
    RETURN NEXT;
  END LOOP;

  DROP TABLE IF EXISTS _fk_pool;
  RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION public.infer_fk_candidates(TEXT, NUMERIC, INT) IS
  'sample 값 overlap 기반 FK 후보 추론. PostgreSQL 내부에서 완결.';
