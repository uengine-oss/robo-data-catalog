# ROBO 카탈로그 — 컨테이너 이미지.
#
# 분석기와 같은 이유로 옮겼다(`robo-data-analyzer/Dockerfile` 머리말 참조): 호스트 런타임을
# 빌드할 때마다 의존성을 다시 풀어야 했고 그 해결이 실제 환경과 갈라져 실패했다.
#
# 이 서비스는 파일을 공유하지 않는다 — Neo4j 와 HTTP 만 쓴다. 그래서 바인드 마운트가 없다.
# 분석기와 달리 작업 디렉터리를 붙이지 않는 이유다.
#
# 의존성은 `requirements.lock.txt` — 돌아가는 venv 에서 뽑은 조합이라 재현이 보장된다.
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.lock.txt /app/requirements.lock.txt
RUN pip install --no-cache-dir -r /app/requirements.lock.txt \
 && apt-get purge -y build-essential && apt-get autoremove -y

COPY . /app

# `DOCKER_COMPOSE_CONTEXT` 가 가리키는 작업 디렉터리. 마운트 지점이 실재해야 한다 —
# 이 서비스는 파일을 공유하지 않지만, 그 값이 비면 삭제 경로가 엉뚱한 곳을 기준으로
# 삼는다(compose.yml 의 robo-catalog 절 참조).
RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 5503

HEALTHCHECK --interval=5s --timeout=5s --retries=60 --start-period=20s \
  CMD curl --fail --silent http://127.0.0.1:5503/docs >/dev/null || exit 1

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5503"]
