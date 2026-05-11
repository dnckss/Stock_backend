FROM python:3.11-slim

# HF Spaces 권장 — 기본 사용자(uid=1000)
ENV HOME=/home/user \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HOME=/home/user/.cache/huggingface

# yfinance/lxml 등에서 필요한 빌드/런타임 라이브러리
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
        && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 user
USER user
WORKDIR /home/user/app

COPY --chown=user:user requirements.txt .
RUN pip install --user -r requirements.txt

COPY --chown=user:user . .

ENV PATH="/home/user/.local/bin:${PATH}"

# HF Spaces 는 PORT=7860 을 주입한다. api.py 가 os.environ.get("PORT") 로 읽음.
EXPOSE 7860

CMD ["python", "api.py"]
