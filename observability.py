"""ROBO Data Catalog 운영 로그 구성과 구조화 작업 로그.

사용법:
    from observability import setup_catalog_logging
"""

import logging
import sys
from contextvars import ContextVar
from typing import Any

# 컨텍스트 변수
_log_context: ContextVar[dict[str, Any]] = ContextVar("log_context", default={})


class CatalogLogContextFilter(logging.Filter):
    """로그에 컨텍스트 정보를 자동 추가하는 필터"""
    
    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _log_context.get()
        for key, value in ctx.items():
            setattr(record, key, value)
        return True


class CatalogLogFormatter(logging.Formatter):
    """ROBO Catalog 로그 포맷터
    
    출력 형식:
        2024-01-01 12:00:00 [INFO] module_name: 메시지 | key=value
    """
    
    def format(self, record: logging.LogRecord) -> str:
        # 기본 메시지
        msg = super().format(record)
        
        # 추가 키워드 인자가 있으면 붙이기
        extra_parts = []
        skip_attrs = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs",
            "pathname", "process", "processName", "relativeCreated",
            "stack_info", "exc_info", "exc_text", "thread", "threadName",
            "message", "asctime",
        }
        
        for key, value in record.__dict__.items():
            if key not in skip_attrs and not key.startswith("_"):
                extra_parts.append(f"{key}={value}")
        
        if extra_parts:
            msg = f"{msg} | {', '.join(extra_parts)}"
        
        return msg


def setup_catalog_logging(level: int = logging.INFO) -> None:
    """애플리케이션 로깅 설정
    
    Args:
        level: 로그 레벨 (기본: INFO)
    """
    # UTF-8 인코딩 설정
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except (AttributeError, OSError, ValueError) as exc:
            # Console encoding is a best-effort compatibility aid, not a reason
            # to prevent service startup. Keep the fallback observable.
            logging.getLogger(__name__).debug(
                "UTF-8 console reconfiguration unavailable | error_type=%s",
                type(exc).__name__,
            )
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 새 핸들러 추가
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(CatalogLogFormatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    handler.addFilter(CatalogLogContextFilter())
    root_logger.addHandler(handler)
    
    # 서드파티 로거 레벨 조정
    for name in ["neo4j", "httpx", "httpcore", "urllib3"]:
        logging.getLogger(name).setLevel(logging.WARNING)


def get_catalog_logger(name: str) -> logging.Logger:
    """로거 인스턴스 반환
    
    Args:
        name: 로거 이름 (보통 __name__)
        
    Returns:
        logging.Logger 인스턴스
    """
    return logging.getLogger(name)


def log_catalog_operation(
    operation_domain: str,
    operation_stage: str,
    message: str,
    level: int | str = logging.INFO,
    exc: Exception | None = None,
    exc_info: bool = False,
) -> None:
    """Catalog 작업 경계의 구조화 로그를 출력한다.

    - operation_domain: metadata, lineage 등 Catalog 기능 영역
    - operation_stage: 논리적 단계 이름
    - message: 사용자 친화적 설명
    - level: logging 모듈 레벨 또는 문자열("DEBUG", "INFO" 등)
    - exc: 예외 객체 전달 시 스택 트레이스까지 출력
    - exc_info: 기존 코드 호환용 플래그 (True면 스택 트레이스 출력)
    """
    domain_text = (operation_domain or "CATALOG").upper()
    stage_text = (operation_stage or "OPERATION").upper()

    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    if exc is not None:
        real_exc_info: Exception | bool | None = exc
    elif exc_info:
        real_exc_info = True
    else:
        real_exc_info = None

    logging.log(level, f"[{domain_text}:{stage_text}] {message}", exc_info=real_exc_info)


