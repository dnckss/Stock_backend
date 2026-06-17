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

# HF Spaces 는 app_port(README)=7860 으로 라우팅한다. HF 는 PORT env 를 자동 주입하지
# 않으므로 여기서 명시 고정 — api.py 가 os.environ["PORT"] 를 읽어 반드시 7860 에서 listen.
ENV PORT=7860
EXPOSE 7860

CMD ["python", "api.py"]
