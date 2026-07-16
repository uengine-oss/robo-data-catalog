# ROBO Data Catalog — 그래프 조회·샘플·리니지 API (FastAPI / uvicorn :5503)
#
# 게이트웨이가 /robo/{schema,graph,lineage,glossary} 등을 robo-data-catalog:5503 으로 라우팅.
# Neo4j 접속은 런타임 env(또는 Electron 의 X-Neo4j-* override 헤더)로 주입 — 이미지에 비밀값 없음.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5503

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5503"]
