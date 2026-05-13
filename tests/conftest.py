"""pytest fixtures + 환경 sandbox.

외부 의존(Supabase·OpenAI·yfinance) 없이 단위 테스트가 돌아가도록
필수 env 를 더미값으로 채우고 STRICT_ENV=false 로 둔다.
"""
import os
import sys

# tests/ 위 폴더(프로젝트 루트) 를 path 에 추가 — `from services.x` 같은 import 를 위해.
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# 테스트는 외부 시스템과 통신하지 않으므로 더미값으로 충분.
os.environ.setdefault("STRICT_ENV", "false")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("OPENAI_API_KEY", "test-key")
