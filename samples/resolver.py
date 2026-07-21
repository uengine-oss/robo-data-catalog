"""Table-name normalization and fuzzy resolution."""
from __future__ import annotations

import re
from typing import List, Optional, Tuple

from rapidfuzz import fuzz, process

_CAMEL_SPLIT = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_SEP = re.compile(r"[\s\-]+")
_MULTI_UNDER = re.compile(r"_+")


def _normalize(name: str) -> str:
    """범용 스타일 통일: 스키마 strip + camelCase 분해 + 구분자 통일 + 소문자.

    언어·도메인 중립. 토큰 의미(접두어·접미어·약어)는 건드리지 않음.
    'PUBLIC.ZngmCommCdDtl' / 'zngm-comm cd dtl' / 'zngm_comm_cd_dtl' → 'zngm_comm_cd_dtl'
    """
    name = name.split(".")[-1]
    name = _CAMEL_SPLIT.sub("_", name)
    name = _SEP.sub("_", name)
    name = _MULTI_UNDER.sub("_", name).strip("_")
    return name.lower()


def resolve_name(
    query: str,
    candidates: List[str],
    threshold: float = 85.0,
) -> Optional[Tuple[str, float]]:
    """query 를 candidates 중 best match 로 해소.

    normalize 를 query·candidates 양쪽에 적용 후 비교.
    1) exact match → (원본 candidate, 100.0)
    2) rapidfuzz.process.extractOne (score_cutoff=threshold)
    3) 실패 시 None
    """
    if not candidates:
        return None

    norm_map = {_normalize(c): c for c in candidates}
    norm_q = _normalize(query)
    if norm_q in norm_map:
        return (norm_map[norm_q], 100.0)

    result = process.extractOne(
        norm_q,
        list(norm_map.keys()),
        scorer=fuzz.WRatio,
        score_cutoff=threshold,
    )
    if result is None:
        return None
    matched_norm, score, _ = result
    return (norm_map[matched_norm], float(score))
