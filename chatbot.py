"""
밭봇 — 전국 공영도매시장 실시간 경매정보 챗봇
농가(생산자) 대상: 품목별 가격 조회 + 출하 타이밍 추천
데이터 출처: 한국농수산식품유통공사(aT) 전국 공영도매시장 실시간 경매정보
             KAMIS 농산물유통정보 (과거 가격 이력)
"""

from openai import OpenAI
from api_client import AgroMarketClient, PriceAnalyzer, OilPriceClient, KamisClient, MafraHistoryClient
from datetime import datetime, timedelta
import re
import requests

# Supabase 예측 DB (선택적 import — 서버 환경에서도 동작)
try:
    from database import get_predictions as _db_get_predictions
    _HAS_PRED_DB = True
except Exception:
    _HAS_PRED_DB = False

import os
from dotenv import load_dotenv
from cryptography.fernet import Fernet

load_dotenv()

def _decrypt(enc_value: str) -> str:
    if not enc_value:
        return ""
    try:
        key_path = os.path.join(os.path.dirname(__file__), "secret.key")
        with open(key_path, "rb") as kf:
            f = Fernet(kf.read())
        return f.decrypt(enc_value.encode()).decode()
    except Exception:
        return ""

def _get_key(enc_env: str, plain_secret: str) -> str:
    # 평문 환경변수 먼저 확인 (FastAPI/uvicorn 환경)
    plain_val = os.environ.get(plain_secret, "")
    if plain_val:
        return plain_val
    try:
        import streamlit as st
        val = st.secrets.get(plain_secret, "")
        if val:
            return val
    except Exception:
        pass
    return _decrypt(os.environ.get(enc_env, ""))

API_KEY          = _get_key("AGRO_API_KEY_ENC",   "AGRO_API_KEY")
GROQ_API_KEY     = _get_key("GROQ_API_KEY_ENC",   "GROQ_API_KEY")
OIL_API_KEY      = _get_key("OIL_API_KEY_ENC",    "OIL_API_KEY")
KAMIS_CERT_KEY   = _get_key("KAMIS_CERT_KEY_ENC",  "KAMIS_CERT_KEY")
KAMIS_CERT_ID    = _get_key("KAMIS_CERT_ID_ENC",   "KAMIS_CERT_ID")
MAFRA_API_KEY    = _get_key("MAFRA_API_KEY_ENC",   "MAFRA_API_KEY")
groq_client = OpenAI(
    api_key=GROQ_API_KEY,
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
)

SUPPORTED_ITEMS = ["배추", "무", "고추", "대파", "양파", "감자", "딸기", "사과", "배"]
FARM_ITEMS = ["옥수수", "소맥", "대두", "설탕", "커피"]  # FARM 시계열 전용 품목

_OIL_DOMESTIC_KW = {"경유", "휘발유", "기름값", "주유", "LPG", "등유", "기름"}
_OIL_INTL_KW     = {"국제유가", "WTI", "브렌트", "두바이유", "원유", "국제 유가"}
_OIL_GENERAL_KW  = {"유가"}

# ── 기상청 날씨 API ──────────────────────────────────────────────────────────
_KMA_API_KEY  = "YCj_RwjOTDCo_0cIzvwwyw"
_KMA_NCST_URL = "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getUltraSrtNcst"

_LOCATION_GRID = {
    "서울": (60, 127), "부산": (98, 76),  "대구": (89, 90),
    "인천": (55, 124), "광주": (58, 74),  "대전": (67, 100),
    "울산": (102, 84), "수원": (60, 121), "춘천": (73, 134),
    "강릉": (92, 131), "청주": (69, 107), "전주": (63, 89),
    "목포": (50, 67),  "여수": (73, 66),  "제주": (52, 38),
    "포항": (102, 94), "경주": (100, 91), "천안": (63, 110),
}

_PTY_MAP = {
    "0": "맑음", "1": "비", "2": "비/눈",
    "3": "눈",   "5": "빗방울", "6": "빗방울·눈날림", "7": "눈날림",
}

_WEATHER_KW = [
    "날씨", "기온", "온도", "비 와", "눈 와", "비오", "눈오",
    "흐려", "맑아", "바람", "강수", "습도", "우산", "더워", "추워",
    "덥다", "춥다", "날씨야", "날씨어",
]

def _is_weather_query(text: str) -> bool:
    return any(kw in text for kw in _WEATHER_KW)

def _detect_location(text: str):
    for loc, coords in _LOCATION_GRID.items():
        if loc in text:
            return loc, coords
    return "서울", (60, 127)

def _kma_base_time():
    """초단기실황은 매시 40분 이후 정상 조회 → 40분 미만이면 1시간 전 사용"""
    now = datetime.now()
    if now.minute < 40:
        now -= timedelta(hours=1)
    return now.strftime("%Y%m%d"), now.strftime("%H00")

def _get_current_weather(nx: int = 60, ny: int = 127) -> str:
    base_date, base_time = _kma_base_time()
    params = {
        "pageNo": 1, "numOfRows": 50, "dataType": "JSON",
        "base_date": base_date, "base_time": base_time,
        "nx": nx, "ny": ny, "authKey": _KMA_API_KEY,
    }
    resp = requests.get(_KMA_NCST_URL, params=params, timeout=8)
    data = resp.json()
    items = data["response"]["body"]["items"]["item"]
    obs   = {it["category"]: it["obsrValue"] for it in items}

    temp   = obs.get("T1H", "?")
    humid  = obs.get("REH", "?")
    wind   = obs.get("WSD", "?")
    rain   = obs.get("RN1", "0")
    pty_cd = str(int(float(obs.get("PTY", 0))))
    sky    = _PTY_MAP.get(pty_cd, "맑음")

    summary = f"기온 {temp}°C · {sky} · 습도 {humid}% · 풍속 {wind}m/s"
    if rain not in ("0", "강수없음"):
        summary += f" · 1시간 강수량 {rain}mm"
    return summary

# ── 기상청 단기예보 (오늘~3일) ──────────────────────────────────────────────
_KMA_FCST_URL    = "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getVilageFcst"
_FCST_BASE_HOURS = [2, 5, 8, 11, 14, 17, 20, 23]

_SKY_MAP = {"1": "맑음", "3": "구름많음", "4": "흐림"}
_PTY_FCST_MAP = {"0": "", "1": "비", "2": "비/눈", "3": "눈", "4": "소나기"}

_FORECAST_KW = [
    "내일", "모레", "주간", "이번주", "주말", "예보", "전망",
    "올까", "올까요", "올거야", "며칠",
]

def _is_forecast_query(text: str) -> bool:
    return any(kw in text for kw in _FORECAST_KW)

def _fcst_base_time():
    """단기예보 발표시각: 0200·0500·0800·1100·1400·1700·2000·2300 (10분 후 제공)"""
    now = datetime.now() - timedelta(minutes=10)
    valid = [h for h in _FCST_BASE_HOURS if h <= now.hour]
    if not valid:                              # 자정~02:10 → 전날 2300 사용
        yesterday = now - timedelta(days=1)
        return yesterday.strftime("%Y%m%d"), "2300"
    return now.strftime("%Y%m%d"), f"{max(valid):02d}00"

def _get_forecast_weather(nx: int = 60, ny: int = 127) -> str:
    from collections import defaultdict
    base_date, base_time = _fcst_base_time()
    params = {
        "pageNo": 1, "numOfRows": 1000, "dataType": "JSON",
        "base_date": base_date, "base_time": base_time,
        "nx": nx, "ny": ny, "authKey": _KMA_API_KEY,
    }
    resp  = requests.get(_KMA_FCST_URL, params=params, timeout=10)
    items = resp.json()["response"]["body"]["items"]["item"]

    # 날짜별 데이터 수집
    daily = defaultdict(lambda: {"temps": [], "pops": [], "sky": "1", "pty": "0"})
    for it in items:
        date, cat, val, ftime = it["fcstDate"], it["category"], it["fcstValue"], it["fcstTime"]
        if   cat == "TMP":  daily[date]["temps"].append(float(val))
        elif cat == "TMX":  daily[date]["tmax"] = float(val)
        elif cat == "TMN":  daily[date]["tmin"] = float(val)
        elif cat == "POP":  daily[date]["pops"].append(float(val))
        elif cat == "SKY" and ftime in ("1200", "1500"):
            daily[date]["sky"] = val
        elif cat == "PTY" and ftime in ("1200", "1500") and val != "0":
            daily[date]["pty"] = val

    today = datetime.now()
    result_lines = []
    for i in range(3):
        d_obj  = today + timedelta(days=i)
        dkey   = d_obj.strftime("%Y%m%d")
        label  = ["오늘", "내일", "모레"][i]
        dfmt   = f"{d_obj.month}/{d_obj.day}"
        if dkey not in daily:
            continue
        d = daily[dkey]

        tmax = d.get("tmax", max(d["temps"]) if d["temps"] else None)
        tmin = d.get("tmin", min(d["temps"]) if d["temps"] else None)
        pop  = f"{max(d['pops']):.0f}" if d["pops"] else "0"

        pty = d["pty"]
        sky_txt = _PTY_FCST_MAP.get(pty, "") or _SKY_MAP.get(d["sky"], "맑음")

        tmax_s = f"{tmax:.0f}" if tmax is not None else "?"
        tmin_s = f"{tmin:.0f}" if tmin is not None else "?"
        result_lines.append(
            f"{label}({dfmt}): 최고 {tmax_s}°C / 최저 {tmin_s}°C · {sky_txt} · 강수확률 {pop}%"
        )

    return "\n".join(result_lines) if result_lines else "예보 데이터를 가져올 수 없어요."


class AgroChatBot:
    def __init__(self):
        self.client      = AgroMarketClient(API_KEY)
        self.analyzer    = PriceAnalyzer(self.client)
        self.oil_client  = OilPriceClient(OIL_API_KEY)
        self.kamis       = KamisClient(KAMIS_CERT_KEY, KAMIS_CERT_ID) if KAMIS_CERT_KEY else None
        self.mafra       = MafraHistoryClient(MAFRA_API_KEY) if MAFRA_API_KEY else None

    # ------------------------------------------------------------------
    # 추출 헬퍼
    # ------------------------------------------------------------------

    def _extract_item(self, text: str) -> str | None:
        for item in SUPPORTED_ITEMS + FARM_ITEMS:
            if item in text:
                return item
        return None

    def _extract_date(self, text: str) -> str:
        today = datetime.today()
        if "오늘" in text:
            return today.strftime("%Y-%m-%d")
        if "어제" in text:
            return (today - timedelta(days=1)).strftime("%Y-%m-%d")
        m = re.search(r"(\d{4})[년\-]?\s*(\d{1,2})[월\-]?\s*(\d{1,2})[일]?", text)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")

    def _extract_market(self, text: str) -> str | None:
        for kw in ["가락", "강서", "노량진", "구리", "수원", "인천", "부산", "대구", "광주", "대전"]:
            if kw in text:
                return kw
        return None

    def _extract_history_range(self, text: str) -> tuple[str, str] | None:
        """과거 기간 표현 추출 → (start_date, end_date) 또는 None"""
        today = datetime.today()
        current_year = today.year

        if "작년" in text:
            y = current_year - 1
            return f"{y}-01-01", f"{y}-12-31"
        if "재작년" in text:
            y = current_year - 2
            return f"{y}-01-01", f"{y}-12-31"

        # "N년 전"
        m = re.search(r"(\d+)\s*년\s*전", text)
        if m:
            y = current_year - int(m.group(1))
            return f"{y}-01-01", f"{y}-12-31"

        # 특정 연도 단독 ("2020년", "2019")
        m = re.search(r"(20\d{2})\s*년?", text)
        if m:
            y = int(m.group(1))
            if y < current_year:
                # 연도+월 패턴
                mm = re.search(r"(20\d{2})\s*년?\s*(\d{1,2})\s*월", text)
                if mm:
                    y2  = int(mm.group(1))
                    mo  = int(mm.group(2))
                    last_day = (datetime(y2, mo % 12 + 1, 1) - timedelta(days=1)).day if mo < 12 else 31
                    return f"{y2}-{mo:02d}-01", f"{y2}-{mo:02d}-{last_day:02d}"
                return f"{y}-01-01", f"{y}-12-31"

        return None

    # ------------------------------------------------------------------
    # 실시간 데이터 → Groq 컨텍스트 구성
    # ------------------------------------------------------------------

    def _build_context(self, text: str) -> str:
        parts = []
        oil_kws  = {"유가", "기름값", "원유", "WTI", "브렌트", "경유"}
        agri_kws = {"품목", "채소", "과일", "농산물", "가격", "시세", "경락가",
                    "연관", "영향", "같이", "오를", "내릴", "올라", "내려"}

        has_oil  = any(k in text for k in oil_kws)
        has_agri = any(k in text for k in agri_kws)

        if has_oil and has_agri:
            try:
                corr = self.analyzer.get_oil_correlation(SUPPORTED_ITEMS, days=30)
                if corr:
                    lines = [f"{r['품목']} {r['상관계수']:+.2f}" for r in corr]
                    parts.append("최근 30일 유가(WTI)-농산물 상관계수: " + ", ".join(lines))
            except Exception:
                pass

        if has_oil:
            try:
                intl = self.oil_client.get_international_price()
                if intl:
                    lines = [f"{i['품목']} ${i['가격']}/배럴" for i in intl]
                    parts.append("현재 국제유가: " + ", ".join(lines))
            except Exception:
                pass

        if has_agri:
            try:
                prices = []
                for item in SUPPORTED_ITEMS[:6]:
                    s = self.analyzer.get_volatility_summary(item)
                    if "error" not in s:
                        prices.append(
                            f"{item} {s['현재가(평균낙찰가)']:,}원({s['전일_대비(%)']:+.1f}%)"
                        )
                if prices:
                    parts.append("현재 주요 품목 경락가: " + ", ".join(prices))
            except Exception:
                pass

        return "\n".join(parts)

    def get_ai_answer(self, user_input: str, context: str = "") -> str:
        system_msg = (
            "너는 유통학 전문가이자 농산물 마케팅 전략가인 '밭봇'이야. "
            "반드시 순수한 한국어로만 답해. 한자, 일본어, 한문은 절대 쓰지 마. "
            "'거래량', '출하량', '가격' 같은 한국어 단어를 써. "
            "농민들에게 경매 시세를 분석해주고, 유통 흐름이나 경제 상황에 대해 전문적으로 상담해줘."
        )
        if context:
            system_msg += (
                f"\n\n[실시간 데이터]\n{context}\n\n"
                "위 데이터를 바탕으로 구체적이고 자연스럽게 답변해줘."
            )
        try:
            safe_input = user_input.encode("utf-8", errors="ignore").decode("utf-8")
            response = groq_client.chat.completions.create(
                model="gemini-2.5-flash",
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": safe_input},
                ]
            )
            text = response.choices[0].message.content
            text = re.sub(r'[一-鿿぀-ゟ゠-ヿ]+', '', text)
            return text
        except Exception as e:
            return f"AI 답변 중 오류가 발생했어요: {e}"

    # ------------------------------------------------------------------
    # 포맷 헬퍼
    # ------------------------------------------------------------------

    def _format_domestic_oil(self, data: list[dict]) -> str:
        lines = []
        for d in data:
            arrow = "▲" if d["전일대비"] > 0 else ("▼" if d["전일대비"] < 0 else "─")
            lines.append(
                f"  {d['품목']:8s}: {d['가격']:,.1f}원/L  "
                f"{arrow} {abs(d['전일대비']):.1f}원"
            )
        return "\n".join(lines)

    def _format_intl_oil(self, data: list[dict]) -> str:
        lines = []
        for d in data:
            arrow = "▲" if d["전일대비"] > 0 else ("▼" if d["전일대비"] < 0 else "─")
            date_str = d["기준일"]
            if len(date_str) == 8:
                date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            lines.append(
                f"  {d['품목']:10s}: ${d['가격']:.2f}/배럴  "
                f"{arrow} ${abs(d['전일대비']):.2f}  ({date_str})"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 응답 생성
    # ------------------------------------------------------------------

    def _oil_response(self, want_domestic: bool, want_intl: bool) -> str:
        parts = []
        try:
            if want_domestic or (not want_intl):
                dom = self.oil_client.get_domestic_price()
                if dom:
                    parts.append("[국내 주유소 전국 평균 가격]\n" + self._format_domestic_oil(dom))
            if want_intl or (not want_domestic):
                intl = self.oil_client.get_international_price()
                if intl:
                    parts.append("[국제 원유 가격 (USD/배럴)]\n" + self._format_intl_oil(intl))
        except Exception as e:
            return f"유가 데이터 조회 중 오류가 발생했어요: {e}"
        return "\n\n".join(parts) if parts else "유가 데이터를 가져올 수 없어요. 잠시 후 다시 시도해주세요."

    def _oil_correlation_response(self) -> str:
        try:
            results = self.analyzer.get_oil_correlation(SUPPORTED_ITEMS, days=30)
        except Exception as e:
            return f"유가 연동 분석 중 오류가 발생했어요: {e}"
        if not results:
            return "데이터가 부족해서 분석할 수 없어요. 잠시 후 다시 시도해주세요."

        lines = []
        for r in results:
            c = r["상관계수"]
            if c >= 0.6:
                tag = "강한 연동 ▲▲"
            elif c >= 0.3:
                tag = "중간 연동 ▲"
            elif c <= -0.6:
                tag = "강한 역연동 ▼▼"
            elif c <= -0.3:
                tag = "중간 역연동 ▼"
            else:
                tag = "거의 무관  ─"
            lines.append(f"  {r['품목']:4s}: {c:+.2f}  {tag}")

        body = "\n".join(lines)
        return (
            "[유가(WTI)와 농산물 가격 상관분석 — 최근 30일]\n\n"
            f"{body}\n\n"
            "※ +1.0에 가까울수록 유가 오를 때 같이 오름\n"
            "※ 응답에 10~20초 걸릴 수 있어요"
        )

    def _history_response(self, item: str, year: int) -> str:
        if not self.mafra:
            return "과거 이력 조회를 위한 API 키가 설정되지 않았어요."

        if not (2014 <= year <= 2023):
            return f"{year}년 데이터는 지원하지 않아요. 2014~2023년 사이로 질문해 주세요."

        df = self.mafra.get_yearly_price(item, year)
        if df.empty:
            return (
                f"{year}년 {item} 데이터가 없어요.\n"
                "해당 연도 가락시장에 거래 기록이 없거나 API 오류일 수 있어요."
            )

        avg  = df["평균가"].mean()
        low  = df["평균가"].min()
        high = df["평균가"].max()
        trend = "  →  ".join(f"{r['월']} {r['평균가']:,}원" for _, r in df.iterrows())

        return (
            f"[{item} {year}년 월별 평균 경락가 — 가락시장]\n\n"
            f"  연평균: {round(avg):,}원\n"
            f"  최저월: {round(low):,}원\n"
            f"  최고월: {round(high):,}원\n\n"
            f"월별: {trend}\n\n"
            "※ 조회에 30초~1분 걸릴 수 있어요"
        )

    # ------------------------------------------------------------------
    # 메인 라우터
    # ------------------------------------------------------------------

    def respond(self, user_input: str) -> str:
        text = user_input.strip()

        # ★ 0순위-A: 날씨 예보 (단기예보 3일)
        if _is_forecast_query(text):
            try:
                loc_name, (nx, ny) = _detect_location(text)
                forecast = _get_forecast_weather(nx, ny)
                return f"📅 {loc_name} 날씨 예보\n{forecast}"
            except Exception as e:
                print(f"[단기예보 오류] {e}")

        # ★ 0순위-B: 기상청 실시간 날씨 조회
        if _is_weather_query(text):
            try:
                loc_name, (nx, ny) = _detect_location(text)
                summary = _get_current_weather(nx, ny)
                return f"🌤 {loc_name} 현재 날씨\n{summary}"
            except Exception as e:
                print(f"[기상청 오류] {e}")
                # 실패 시 AI 폴백으로 계속 진행

        # 유가 연동 상관분석 (구체적 키워드 — 일반 유가 조회보다 먼저)
        if any(kw in text for kw in ["유가 관련", "유가 영향", "기름값 영향", "유가 연동", "유가랑 관련"]):
            return self._oil_correlation_response()

        # 단순 유가 조회 (복합 질문은 AI로)
        is_domestic = any(kw in text for kw in _OIL_DOMESTIC_KW)
        is_intl     = any(kw in text for kw in _OIL_INTL_KW)
        is_simple_oil = is_domestic or is_intl or any(kw in text for kw in _OIL_GENERAL_KW)
        is_complex    = any(kw in text for kw in ["품목", "채소", "과일", "농산물", "연관",
                                                   "영향", "같이", "오를", "내릴", "올라", "내려"])
        if is_simple_oil and not is_complex:
            return self._oil_response(want_domestic=is_domestic, want_intl=is_intl)

        # 제철 품목
        if any(kw in text for kw in ["제철", "이번달", "이번 달", "계절"]):
            month = datetime.today().month
            items = self.analyzer.get_seasonal_items(month)
            return (
                f"{month}월 제철 품목: {', '.join(items)}\n"
                "각 품목의 가격이나 출하 타이밍을 물어보세요!"
            )

        # 과거 가격 이력 (aT API 연도별 조회)
        history_range = self._extract_history_range(text)
        if history_range:
            item = self._extract_item(text)
            if item:
                start, _ = history_range
                year = int(start[:4])
                return self._history_response(item, year)

        # 특정 품목 실시간 조회
        item = self._extract_item(text)
        if item:
            market = self._extract_market(text)
            market_label = f"{market}시장" if market else "전국 공영도매시장"

            # 예측/전망 질문 → AI 분석 우선
            is_prediction = any(kw in text for kw in [
                "앞으로", "전망", "예측", "어떻게 될", "어떨", "미래", "예상",
                "오를까", "내릴까", "올라갈", "내려갈", "상승", "하락",
            ])
            if is_prediction:
                context_parts = []
                is_farm_item = item in FARM_ITEMS
                # 1. 실시간 시세 (국내 경매 품목만 — FARM 전용 품목은 aT API 없음)
                if not is_farm_item:
                    try:
                        s = self.analyzer.get_volatility_summary(item)
                        if "error" not in s:
                            context_parts.append(
                                f"{item} 현재 경락가: {s['현재가(평균낙찰가)']:,}원, "
                                f"전일 대비: {s['전일_대비(%)']:+.1f}%, "
                                f"신호: {s['신호']}"
                            )
                    except Exception:
                        pass
                # 2. FARM 시계열 예측값 (Supabase)
                if _HAS_PRED_DB:
                    try:
                        preds = _db_get_predictions(item)
                        if preds:
                            unit = "달러($)" if is_farm_item else "원"
                            pred_lines = [
                                f"  - {p['model']}: {p['target_date']} "
                                f"{p['predicted_price']:,.2f}{unit} ({p['trend']}, "
                                f"변화율 {p['change_rate']:+.1f}%)"
                                for p in preds
                            ]
                            context_parts.append(
                                "[FARM AI 시계열 예측 결과]\n" + "\n".join(pred_lines)
                            )
                    except Exception:
                        pass
                if is_farm_item and not context_parts:
                    context_parts.append(
                        f"{item}은 국제 선물 품목으로 국내 경매 데이터가 없습니다. "
                        "FARM AI 예측 데이터를 기반으로 답변해주세요."
                    )
                extra = self._build_context(text)
                if extra:
                    context_parts.append(extra)
                context = "\n\n".join(context_parts)
                return self.get_ai_answer(text, context=context)

            if any(kw in text for kw in ["가격", "얼마", "시세"]):
                date_str = self._extract_date(text)
                df = self.client.get_price_by_date(item, date_str, market)
                if df.empty:
                    return (
                        f"{date_str} {market_label} {item} 데이터가 없어요.\n"
                        "주말·공휴일은 경매가 없습니다. 다른 날짜를 물어보세요."
                    )
                avg   = df["scsbd_prc"].mean()
                low   = df["scsbd_prc"].min()
                high  = df["scsbd_prc"].max()
                count = len(df)
                return (
                    f"[{date_str}] {market_label} {item} 경매 결과\n"
                    f"평균 낙찰가: {round(avg):,}원\n"
                    f"최저가: {round(low):,}원\n"
                    f"최고가: {round(high):,}원\n"
                    f"거래 건수: {count}건"
                )

            if any(kw in text for kw in ["팔", "출하", "타이밍", "될까", "언제", "변동"]):
                s = self.analyzer.get_volatility_summary(item)
                if "error" in s:
                    return s["error"]
                trend = "  →  ".join(f"{lbl} {p:,}원" for lbl, p in s["시계열"][-5:])
                z = s["z_score"]
                zone = "고가권" if z > 0.5 else ("저가권" if z < -0.5 else "평균권")
                return (
                    f"[{item} 출하 타이밍 분석 — 전국 공영도매시장, 최근 2주]\n\n"
                    f"  현재 평균낙찰가: {s['현재가(평균낙찰가)']:,}원\n"
                    f"  2주 평균가:      {s['2주_평균가']:,}원\n"
                    f"  전일 대비:       {s['전일_대비(%)']:+.1f}%\n"
                    f"  z-score:         {z:+.2f} ({zone})\n\n"
                    f"  ▶ [{s['신호']}] {s['조언']}\n\n"
                    f"최근 추이: {trend}"
                )

        # 급등/급락
        if any(kw in text for kw in ["급등", "급락", "오른", "내린", "알림", "비교"]):
            results = []
            for it in SUPPORTED_ITEMS[:6]:
                try:
                    s = self.analyzer.get_volatility_summary(it)
                    if "error" not in s:
                        results.append((it, s["전일_대비(%)"], s["신호"]))
                except Exception:
                    continue
            if not results:
                return "현재 비교할 데이터가 없어요. 잠시 후 다시 시도해주세요."
            results.sort(key=lambda x: abs(x[1]), reverse=True)
            lines = "\n".join(
                f"  {'▲' if r[1] > 0 else '▼'} {r[0]}: {r[1]:+.1f}%  [{r[2]}]"
                for r in results
            )
            return f"주요 품목 전일 대비 변동률 (전국 공영도매시장):\n\n{lines}"

        # 나머지 — 실시간 데이터 컨텍스트 + Groq AI
        context = self._build_context(text)
        return self.get_ai_answer(text, context=context)


def main():
    import sys
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stdin, "reconfigure"):
        sys.stdin.reconfigure(encoding="utf-8", errors="ignore")
    bot = AgroChatBot()
    print("=" * 45)
    print("밭봇 -- 전국 공영도매시장 실시간 경매정보")
    print("=" * 45)
    print("(종료: exit)\n")
    while True:
        try:
            user_input = input("사장님: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n밭봇: 좋은 하루 되세요!")
            break
        if user_input.lower() in ("exit", "quit", "종료"):
            print("밭봇: 좋은 하루 되세요!")
            break
        if not user_input:
            continue
        print(f"밭봇: {bot.respond(user_input)}\n")


if __name__ == "__main__":
    main()
