"""
api_client.py
한국농수산식품유통공사 — 전국 공영도매시장 실시간 경매정보
서비스 URL: https://apis.data.go.kr/B552845/katRealTime2/trades2
필수 파라미터: serviceKey, cond[trd_clcln_ymd::EQ] (YYYY-MM-DD)
"""

import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# OilPriceClient — 한국석유공사 오피넷 API
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------

BASE_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

# 월별 제철 품목
SEASONAL_ITEMS: dict[int, list[str]] = {
    1:  ["배추", "무", "대파", "사과"],
    2:  ["딸기", "배추", "무", "대파"],
    3:  ["딸기", "대파", "양파"],
    4:  ["딸기", "양파", "감자"],
    5:  ["딸기", "양파", "감자"],
    6:  ["감자", "양파", "고추"],
    7:  ["고추", "감자", "양파"],
    8:  ["고추", "배추", "대파"],
    9:  ["고추", "배추", "무", "사과"],
    10: ["배추", "무", "사과", "고추"],
    11: ["배추", "무", "사과", "대파"],
    12: ["배추", "무", "딸기", "사과"],
}


class OilPriceClient:
    """한국석유공사 오피넷 — 국내 주유소 가격 및 국제 원유 가격"""

    DOMESTIC_URL = "https://www.opinet.co.kr/api/avgAllPrice.do"
    INTL_URL     = "https://www.opinet.co.kr/api/internationalOilPrice.do"

    _PRODUCT_LABELS = {
        "B027": "휘발유",
        "D047": "경유",
        "K015": "LPG(부탄)",
        "C004": "등유",
    }

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = _build_session()

    def get_domestic_price(self) -> list[dict]:
        """국내 주유소 전국 평균 가격 (원/L)"""
        resp = self.session.get(
            self.DOMESTIC_URL,
            params={"code": self.api_key, "out": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        oils = resp.json().get("RESULT", {}).get("OIL", [])
        result = []
        for oil in oils:
            cd    = oil.get("PRODUCT_CD", "")
            label = self._PRODUCT_LABELS.get(cd, oil.get("PRODUCT_NM", cd))
            try:
                price = round(float(oil.get("PRICE", 0)), 1)
                diff  = round(float(oil.get("DIFF",  0)), 1)
            except (ValueError, TypeError):
                continue
            result.append({"품목": label, "가격": price, "전일대비": diff})
        return result

    def get_international_price(self) -> list[dict]:
        """국제 원유 가격 (WTI·두바이·브렌트, USD/배럴)"""
        resp = self.session.get(
            self.INTL_URL,
            params={"code": self.api_key, "out": "json"},
            timeout=10,
        )
        resp.raise_for_status()
        oils = resp.json().get("RESULT", {}).get("OIL", [])
        result = []
        for oil in oils:
            try:
                price = round(float(oil.get("PRICE", 0)), 2)
                diff  = round(float(oil.get("DIFF",  0)), 2)
            except (ValueError, TypeError):
                continue
            result.append({
                "품목":   oil.get("PRODUCT_NM", ""),
                "가격":   price,
                "전일대비": diff,
                "기준일": oil.get("TRADE_DT", ""),
            })
        return result


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


# ---------------------------------------------------------------------------
# AgroMarketClient
# ---------------------------------------------------------------------------

class AgroMarketClient:
    """전국 공영도매시장 실시간 경매정보 API 클라이언트"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = _build_session()

    def _get(self, extra_params: dict) -> dict:
        """
        serviceKey 이중인코딩 방지: serviceKey는 URL에 직접, 나머지는 urlencode
        """
        query = urllib.parse.urlencode(extra_params)
        url = f"{BASE_URL}?serviceKey={self.api_key}&{query}"
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_price_by_date(self, item: str, target_date: str, market: str = None) -> pd.DataFrame:
        """
        특정 날짜의 품목 경매 데이터 반환.

        Parameters
        ----------
        item : str  품목명 (예: '배추', '대파')
        target_date : str  YYYY-MM-DD
        market : str  도매시장명 필터 (선택, 예: '가락')

        Returns
        -------
        DataFrame — 해당 품목 행. 없으면 빈 DataFrame.
        """
        params = {
            "returnType": "json",
            "pageNo": 1,
            "numOfRows": 1000,
            "cond[trd_clcln_ymd::EQ]": target_date,
        }

        try:
            data = self._get(params)
        except Exception as e:
            return pd.DataFrame()

        items = (data.get("response", {})
                     .get("body", {})
                     .get("items", {}) or {})
        row_list = items.get("item", [])
        if not row_list:
            return pd.DataFrame()
        if isinstance(row_list, dict):
            row_list = [row_list]

        df = pd.DataFrame(row_list)

        # 품목 필터: corp_gds_item_nm(법인상품품목명) 또는 gds_sclsf_nm(소분류명)
        mask = pd.Series([False] * len(df))
        for col in ["corp_gds_item_nm", "gds_sclsf_nm", "gds_mclsf_nm"]:
            if col in df.columns:
                mask |= df[col].astype(str).str.contains(item, na=False)
        df = df[mask].copy()

        # 시장 필터 (선택)
        if market and "whsl_mrkt_nm" in df.columns:
            df = df[df["whsl_mrkt_nm"].str.contains(market, na=False)]

        # 낙찰가 숫자 변환
        if "scsbd_prc" in df.columns:
            df["scsbd_prc"] = pd.to_numeric(df["scsbd_prc"], errors="coerce")

        return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# PriceAnalyzer
# ---------------------------------------------------------------------------

class PriceAnalyzer:
    """경매 데이터 기반 가격 변동성 분석"""

    def __init__(self, client: AgroMarketClient):
        self.client = client

    def get_seasonal_items(self, month: int) -> list[str]:
        return SEASONAL_ITEMS.get(month, [])

    def _get_price_series(self, item: str, days: int = 14) -> list[tuple[str, float]]:
        """최근 N일 평일 평균낙찰가 시계열 (오래된 순)"""
        today = date.today()
        results = []

        def fetch(d: date):
            df = self.client.get_price_by_date(item, d.strftime("%Y-%m-%d"))
            if df.empty or "scsbd_prc" not in df.columns:
                return None
            avg = df["scsbd_prc"].dropna().mean()
            if pd.isna(avg):
                return None
            return (d.strftime("%m/%d"), round(avg))

        date_list = [
            today - timedelta(days=i)
            for i in range(days, 0, -1)
            if (today - timedelta(days=i)).weekday() < 5
        ]

        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(fetch, d): d for d in date_list}
            raw = []
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append((futures[f], r))

        raw.sort(key=lambda x: x[0])
        return [(label, price) for _, (label, price) in raw]

    def get_volatility_summary(self, item: str) -> dict:
        """
        최근 2주 가격 변동성 분석.

        Returns
        -------
        dict: 현재가(평균낙찰가), 2주_평균가, 전일_대비(%), z_score, 신호, 조언, 시계열
        에러 시: {'error': str}
        """
        series = self._get_price_series(item, days=14)

        if len(series) < 2:
            return {"error": f"{item} 최근 데이터가 부족합니다. (수집: {len(series)}일)"}

        prices = [p for _, p in series]
        current = prices[-1]
        prev = prices[-2]
        avg = sum(prices) / len(prices)
        std = (sum((p - avg) ** 2 for p in prices) / len(prices)) ** 0.5

        daily_chg = (current - prev) / prev * 100 if prev else 0.0
        z = (current - avg) / std if std > 0 else 0.0

        if z > 1.5:
            signal, advice = "고가경보", "가격이 최근 평균보다 크게 높습니다. 지금 출하가 유리합니다!"
        elif z > 0.5:
            signal, advice = "출하 적기", "가격이 평균보다 높습니다. 출하를 서두르세요."
        elif z > -0.5:
            signal, advice = "보통", "가격이 평균 수준입니다. 시장 동향을 조금 더 지켜보세요."
        elif z > -1.5:
            signal, advice = "관망", "가격이 다소 낮습니다. 반등을 기다려보세요."
        else:
            signal, advice = "출하 보류", "가격이 매우 낮습니다. 저장 여건이 된다면 출하를 늦추세요."

        return {
            "현재가(평균낙찰가)": current,
            "2주_평균가":         round(avg),
            "전일_대비(%)":       round(daily_chg, 1),
            "z_score":            round(z, 2),
            "신호":               signal,
            "조언":               advice,
            "시계열":             series,
        }
