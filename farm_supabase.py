"""
FARM → BATBOT 연동용 Supabase 저장 모듈
이 파일을 FARM HF Space의 src/ 폴더에 복사하세요.

사용법 (Prophet.py, ARIMA.py, LSTM.py의 save_to_chatbot_db 함수 안에 추가):
    from farm_supabase import save_prediction
    save_prediction(item_name, "Prophet", target_date, predicted_price, current_price, change_rate, trend)
"""

import requests
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kgeqtguypsfrhrxwbrfu.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtnZXF0Z3V5cHNmcmhyeXhicmZ1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAzNjk0NTIsImV4cCI6MjA5NTk0NTQ1Mn0"
    ".oIPcHvCu9PQKbd1VFXH5jP8Zkr85IzDKi6sEWWyHNFg"
))


def _headers(prefer: str = "") -> dict:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def save_prediction(
    item: str,
    model: str,
    target_date: str,
    predicted_price: float,
    current_price: float,
    change_rate: float,
    trend: str,
) -> bool:
    """
    시계열 예측 결과를 Supabase에 저장 (upsert).

    Args:
        item: 품목명 (예: "배추", "무")
        model: 모델명 (예: "ARIMA", "Prophet", "LSTM")
        target_date: 예측 기준일 (예: "2026-07-01")
        predicted_price: 예측 가격
        current_price: 현재 가격
        change_rate: 변화율 (%)
        trend: 추세 ("상승세", "하락세", "보합세")
    """
    url = f"{SUPABASE_URL}/rest/v1/predictions"
    try:
        # 기존 데이터 삭제
        requests.delete(
            url,
            headers=_headers(),
            params={"item": f"eq.{item}", "model": f"eq.{model}"},
            timeout=10,
        )
        # 새 데이터 삽입
        r = requests.post(
            url,
            headers=_headers("return=minimal"),
            json={
                "item": item,
                "model": model,
                "target_date": target_date,
                "predicted_price": float(predicted_price),
                "current_price": float(current_price),
                "change_rate": round(float(change_rate), 2),
                "trend": trend,
            },
            timeout=10,
        )
        success = r.status_code in (200, 201, 204)
        if success:
            print(f"[Supabase] ✅ {item} {model} 예측 저장 완료")
        else:
            print(f"[Supabase] ❌ 저장 실패: {r.status_code} {r.text[:100]}")
        return success
    except Exception as e:
        print(f"[Supabase] ❌ 오류: {e}")
        return False
