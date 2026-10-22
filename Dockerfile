# ROBO Data Catalog — 그래프 조회·샘플·리니지 API (FastAPI / uvicorn :5503)
#
# 게이트웨이가 /robo/{schema,graph,lineage,glossary} 등을 robo-data-catalog:5503 으로 라우팅.
# Neo4j 접속은 런타임 env(또는 Electron 의 X-Neo4j-* override 헤더)로 주입 — 이미지에 비밀값 없음.
# ⚠ **베이스를 `python:3.12-slim` 에서 Ubuntu 24.04 로 옮겼다** (2026-10-22).
#
# `python:*-slim` 은 Debian trixie 위에 있고, trixie 의 `zlib` 에
# CVE-2026-85091 이 걸려 있는데 **데비안이 고친 판을 안 냈다**(trixie·sid 모두
# vulnerable, 버그 #1146895). 이 이미지에 남아 있던 마지막 1건이 그것이었다.
#
# 베이스를 나란히 재 보니 갈렸다:
#     python:3.12-slim                        0C 3H   (zlib 포함)
#     ubuntu:24.04 + python3 + python3-venv   **0C 0H**
#
# noble 의 기본 파이썬이 **3.12** 라 쓰던 판과 같고, glibc 계열도 그대로다.
#
# ⚠ **apt 의 `python3-pip`·`python3-setuptools` 는 안 깐다** — 그것들이
#   setuptools 68 · wheel 0.42 를 끌고 와 **3건이 도로 생긴다**(실측).
#   venv 를 만들고 **그 안에서** 최신으로 올린다.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

# ⚠ **venv 를 만든 뒤 `*-whl` 꾸러미를 걷어낸다** (2026-10-22 실측).
#
# `python3-venv` 가 `python3-pip-whl`·`python3-setuptools-whl` 을 끌고 오는데,
# 이것들은 `/usr/share/python-wheels/` 에 **pip 24.0 · setuptools 68.1.2 의
# 휠 파일**을 남긴다. 검사기는 그 휠 **안까지** 읽어서
# `urllib3 2.7.0` 2건 · `msgpack 1.1.2` 2건 · `setuptools` 1건을 올린다 —
# 정작 venv 안에는 setuptools 84 · urllib3 2.8 이 들어 있는데도 그렇다.
# 베이스를 옮기고 1 → 5 로 **늘어나서** 알았다.
#
# venv 를 다 만들었으면 그 휠들은 **더 쓸 일이 없다.**
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-venv ca-certificates \
    && python3 -m venv "$VIRTUAL_ENV" \
    && pip install --no-cache-dir --upgrade pip setuptools wheel \
    && apt-get purge -y python3-pip-whl python3-setuptools-whl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /usr/share/python-wheels

WORKDIR /app

COPY requirements.txt .
# ⚠ **다 깔았으면 `pip` 자신을 걷어낸다** (2026-10-22 실측).
#
# pip 는 `pip/_vendor/` 에 `urllib3`·`msgpack`·`setuptools` 사본을 들고 다니고,
# 검사기는 그 사본까지 센다 — `urllib3 2.7.0` 2건 · `msgpack 1.1.2` 2건 ·
# `setuptools 70.3.0` 1건이 전부 여기서 나왔다. 정작 venv 에 **설치된** 것은
# urllib3 2.8.0 · setuptools 84.2.1 로 멀쩡하다.
#
# 이 컨테이너는 uvicorn 만 띄운다 — **런타임에 pip 를 쓸 일이 없다.**
# 없는 것을 안 싣는 것이 가장 확실하다(10-20 의 streamlit 과 같은 수).
RUN pip install --no-cache-dir -r requirements.txt \
    && pip uninstall -y pip wheel \
    && rm -rf /root/.cache

COPY . .

EXPOSE 5503

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5503"]
