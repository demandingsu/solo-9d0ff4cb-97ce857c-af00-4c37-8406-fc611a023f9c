# 通用镜像：两个服务与 demo 共用，通过 APP_MODULE 选择入口
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY collector /app/collector
COPY relay /app/relay
COPY scripts /app/scripts

ARG APP_MODULE=collector.app:app
ENV APP_MODULE=${APP_MODULE}

EXPOSE 8000

# 容器内默认数据目录（compose 会挂卷到这里）
ENV COLLECTOR_DB=/data/collector/collector.db \
    RELAY_DATA=/data/relay

CMD ["sh", "-c", "uvicorn ${APP_MODULE} --host 0.0.0.0 --port 8000 --proxy-headers"]
