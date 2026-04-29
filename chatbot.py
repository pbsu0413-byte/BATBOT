"""
밭봇 — 전국 공영도매시장 실시간 경매정보 챗봇
농가(생산자) 대상: 품목별 가격 조회 + 출하 타이밍 추천
데이터 출처: 한국농수산식품유통공사(aT) 전국 공영도매시장 실시간 경매정보
"""

from groq import Groq
from api_client import AgroMarketClient, PriceAnalyzer, OilPriceClient
from datetime import datetime, timedelta
import re

import os
from dotenv import load_dotenv
from cryptography.fernet import Fernet

load_dotenv()

def _decrypt(enc_value: str) -> str:
    if not enc_value:
        return ""
    key_path = os.path.join(os.path.dirname(__file__), "secret.key")
    with open(key_path, "rb") as kf:
        f = Fernet(kf.read())
    return f.decrypt(enc_value.encode()).decode()

def _get_key(enc_env: str, plain_secret: str) -> str:
    try:
        import streamlit as st
        val = st.secrets.get(plain_secret, "")
        if val:
            return val
    except Exception:
        pass
    return _decrypt(os.environ.get(enc_env, ""))

API_KEY      = _get_key("AGRO_API_KEY_ENC", "AGRO_API_KEY")
GROQ_API_KEY = _get_key("GROQ_API_KEY_ENC", "GROQ_API_KEY")
OIL_API_KEY  = _get_key("OIL_API_KEY_ENC",  "OIL_API_KEY")
groq_client = Groq(api_key=GROQ_API_KEY)

SUPPORTED_ITEMS = ["배추", "무", "고추", "대파", "양파", "감자", "딸기", "사과", "배"]


_OIL_DOMESTIC_KW  = {"경유", "휘발유", "기름값", "주유", "LPG", "등유", "기름"}
_OIL_INTL_KW      = {"국제유가", "WTI", "브렌트", "두바이유", "원유", "국제 유가"}
_OIL_GENERAL_KW   = {"유가"}


class AgroChatBot:
    def __init__(self):
        self.client    = AgroMarketClient(API_KEY)
        self.analyzer  = PriceAnalyzer(self.client)
        self.oil_client = OilPriceClient(OIL_API_KEY)

    def _extract_item(self, text: str) -> str | None:
        for item in SUPPORTED_ITEMS:
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

    def get_ai_answer(self, user_input: str) -> str:
        """AI 두뇌: 정해진 규칙 외의 질문을 처리합니다."""
        system_msg = (
            "너는 유통학 전문가이자 농산물 마케팅 전략가인 '밭봇'이야. "
            "반드시 순수한 한국어로만 답해. 한자, 일본어, 한문은 절대 쓰지 마. '거래량', '출하량', '가격' 같은 한국어 단어를 써. "
            "농민들에게 경매 시세를 분석해주고, 유통 흐름이나 경제 상황에 대해 전문적으로 상담해줘."
        )
        try:
            safe_input = user_input.encode("utf-8", errors="ignore").decode("utf-8")
            response = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": safe_input}
                ]
            )
            text = response.choices[0].message.content
            # 한자(CJK) 및 일본어(히라가나·가타카나) 제거
            text = re.sub(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]+', '', text)
            return text
        except Exception as e:
            return f"AI 답변 중 오류가 발생했어요: {e}"

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

    def _oil_response(self, want_domestic: bool, want_intl: bool) -> str:
        parts = []
        try:
            if want_domestic or (not want_intl):
                dom = self.oil_client.get_domestic_price()
                if dom:
                    parts.append(
                        "[국내 주유소 전국 평균 가격]\n" + self._format_domestic_oil(dom)
                    )
            if want_intl or (not want_domestic):
                intl = self.oil_client.get_international_price()
                if intl:
                    parts.append(
                        "[국제 원유 가격 (USD/배럴)]\n" + self._format_intl_oil(intl)
                    )
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

    def respond(self, user_input: str) -> str:
        text = user_input.strip()

        # 유가 조회
        is_domestic = any(kw in text for kw in _OIL_DOMESTIC_KW)
        is_intl     = any(kw in text for kw in _OIL_INTL_KW)
        is_oil      = is_domestic or is_intl or any(kw in text for kw in _OIL_GENERAL_KW)
        if is_oil:
            return self._oil_response(want_domestic=is_domestic, want_intl=is_intl)

        if any(kw in text for kw in ["제철", "이번달", "이번 달", "계절"]):
            month = datetime.today().month
            items = self.analyzer.get_seasonal_items(month)
            return (
                f"{month}월 제철 품목: {', '.join(items)}\n"
                "각 품목의 가격이나 출하 타이밍을 물어보세요!"
            )

        item = self._extract_item(text)
        if item:
            market = self._extract_market(text)
            market_label = f"{market}시장" if market else "전국 공영도매시장"

            if any(kw in text for kw in ["가격", "얼마", "시세"]):
                date_str = self._extract_date(text)
                df = self.client.get_price_by_date(item, date_str, market)
                if df.empty:
                    return (
                        f"{date_str} {market_label} {item} 데이터가 없어요.\n"
                        "주말·공휴일은 경매가 없습니다. 다른 날짜를 물어보세요."
                    )
                avg = df["scsbd_prc"].mean()
                low = df["scsbd_prc"].min()
                high = df["scsbd_prc"].max()
                count = len(df)
                return (
                    f"[{date_str}] {market_label} {item} 경매 결과\n"
                    f"  평균 낙찰가: {round(avg):,}원\n"
                    f"  최저가:     {round(low):,}원\n"
                    f"  최고가:     {round(high):,}원\n"
                    f"  거래 건수:  {count}건"
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

        if any(kw in text for kw in ["유가 관련", "유가 영향", "기름값 영향", "유가 연동", "유가랑 관련"]):
            return self._oil_correlation_response()

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

        # 유가 관련 키워드가 있지만 위에서 잡히지 않은 경우 재시도
        if any(kw in text for kw in ["유가", "기름", "경유", "원유"]):
            return self._oil_response(want_domestic=True, want_intl=True)

        return self.get_ai_answer(text)


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
