"""
밭봇 — 전국 공영도매시장 실시간 경매정보 챗봇
농가(생산자) 대상: 품목별 가격 조회 + 출하 타이밍 추천
데이터 출처: 한국농수산식품유통공사(aT) 전국 공영도매시장 실시간 경매정보
"""

from groq import Groq
from api_client import AgroMarketClient, PriceAnalyzer
from datetime import datetime, timedelta
import re

import os

API_KEY = os.environ.get("AGRO_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
groq_client = Groq(api_key=GROQ_API_KEY)

SUPPORTED_ITEMS = ["배추", "무", "고추", "대파", "양파", "감자", "딸기", "사과", "배"]


class AgroChatBot:
    def __init__(self):
        self.client = AgroMarketClient(API_KEY)
        self.analyzer = PriceAnalyzer(self.client)

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
            return response.choices[0].message.content
        except Exception as e:
            return f"AI 답변 중 오류가 발생했어요: {e}"

    def respond(self, user_input: str) -> str:
        text = user_input.strip()

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
