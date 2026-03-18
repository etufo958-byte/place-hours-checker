import re
import time
from datetime import datetime
from typing import List, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Naver Place Hours Checker")

REQUEST_TIMEOUT = 20
PER_ITEM_SLEEP_SEC = 1.0

# 일단 안정화용으로 Playwright 끔
ENABLE_BROWSER_FALLBACK = False

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}


class CheckItem(BaseModel):
    mid: str
    storeName: Optional[str] = None


class BatchRequest(BaseModel):
    items: List[CheckItem]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def url_candidates(mid: str) -> List[str]:
    mid = str(mid).strip()
    return [
        f"https://m.place.naver.com/place/{mid}/home",
        f"https://map.naver.com/p/entry/place/{mid}",
    ]


def clean_text(text: str) -> str:
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{2,}", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def extract_hours_lines_from_text(text: str) -> List[str]:
    text = clean_text(text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    keywords = [
        "영업시간", "브레이크타임", "라스트오더",
        "정기휴무", "휴무", "점심시간", "휴게시간",
        "월", "화", "수", "목", "금", "토", "일", "매일",
        "24시간 영업", "24시간 운영", "24시 영업",
        "연중무휴", "매일 영업",
        "영업 중", "영업종료", "곧 영업 시작", "곧 마감",
    ]

    time_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*[-~∼]\s*([01]?\d|2[0-3]):[0-5]\d"
    )

    result = []
    for i, line in enumerate(lines):
        if any(k in line for k in keywords) or time_pattern.search(line):
            result.append(line)

            # 앞뒤 줄도 일부 함께 저장
            if i - 1 >= 0:
                prev_line = lines[i - 1]
                if prev_line not in result and (
                    any(k in prev_line for k in keywords) or time_pattern.search(prev_line)
                ):
                    result.append(prev_line)

            if i + 1 < len(lines):
                next_line = lines[i + 1]
                if next_line not in result and (
                    any(k in next_line for k in keywords) or time_pattern.search(next_line)
                ):
                    result.append(next_line)

    dedup = []
    seen = set()
    for line in result:
        if line not in seen:
            dedup.append(line)
            seen.add(line)

    return dedup[:20]


def judge_has_hours(lines: List[str]) -> bool:
    if not lines:
        return False

    text = "\n".join(lines).strip()

    # 제외 문구
    invalid_keywords = [
        "영업시간 수정",
        "정보 수정",
        "수정 제안",
        "제안하기",
        "정보 수정 제안",
    ]

    # 1. 일반 시간 범위: 09:00 - 18:00
    time_range_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*[-~∼]\s*([01]?\d|2[0-3]):[0-5]\d"
    )
    if time_range_pattern.search(text):
        return True

    # 2. 요일 + 시간
    day_time_pattern = re.compile(
        r"(월|화|수|목|금|토|일|매일).{0,15}([01]?\d|2[0-3]):[0-5]\d"
    )
    if day_time_pattern.search(text):
        return True

    # 3. 24시간/상시 운영
    always_open_keywords = [
        "24시간 영업",
        "24시간 운영",
        "24시 영업",
        "연중무휴",
        "매일 영업",
        "상시영업",
    ]
    if any(k in text for k in always_open_keywords):
        return True

    # 4. 운영 상태 + 종료/라스트오더 시간
    close_or_lastorder_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*에\s*(영업\s*종료|라스트오더|주문마감|접수마감)"
    )
    if close_or_lastorder_pattern.search(text):
        return True

    # 5. 영업 상태 문구 자체
    status_keywords = [
        "영업 중",
        "영업종료",
        "곧 영업 시작",
        "곧 마감",
        "브레이크타임",
        "라스트오더",
        "정기휴무",
        "휴게시간",
        "점심시간",
    ]
    if any(k in text for k in status_keywords):
        return True

    # 6. 수정제안류만 있고 실제 시간/상태정보가 없으면 X
    if any(k in text for k in invalid_keywords):
        return False

    return False

def fetch_text_via_requests(url: str) -> str:
    """
    requests로 HTML 받아서 본문 텍스트 추출
    인코딩 깨짐 보정 포함
    """
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    # 인코딩 보정
    if not resp.encoding or resp.encoding.lower() in ["iso-8859-1", "ascii"]:
        resp.encoding = resp.apparent_encoding or "utf-8"

    html = resp.text

    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    text = soup.get_text("\n", strip=True)
    return clean_text(text)


def check_one_mid(mid: str, store_name: Optional[str]) -> Dict[str, Any]:
    candidates = url_candidates(mid)
    errors = []

    for url in candidates:
        try:
            text = fetch_text_via_requests(url)
            lines = extract_hours_lines_from_text(text)
            has_hours = judge_has_hours(lines)

            return {
                "mid": mid,
                "storeName": store_name or "",
                "hasHours": bool(has_hours),
                "rawText": "\n".join(lines),
                "status": "완료",
                "error": "",
                "sourceUrl": url,
                "checkedAt": now_str(),
                "sourceType": "requests",
            }
        except Exception as e:
            errors.append(f"[requests] {url} -> {str(e)}")

    # 여기서도 API 전체 실패로 만들지 않고 X로 반환
    return {
        "mid": mid,
        "storeName": store_name or "",
        "hasHours": False,
        "rawText": "",
        "status": "완료",
        "error": " | ".join(errors)[:1000],
        "sourceUrl": "",
        "checkedAt": now_str(),
        "sourceType": "",
    }


@app.get("/health")
def health():
    return {"ok": True, "time": now_str()}


@app.post("/check-hours-batch")
def check_hours_batch(payload: BatchRequest):
    results = []

    try:
        for item in payload.items:
            try:
                result = check_one_mid(item.mid, item.storeName)
            except Exception as e:
                result = {
                    "mid": item.mid,
                    "storeName": item.storeName or "",
                    "hasHours": False,
                    "rawText": "",
                    "status": "실패",
                    "error": f"server item error: {str(e)}"[:1000],
                    "sourceUrl": "",
                    "checkedAt": now_str(),
                    "sourceType": "",
                }

            results.append(result)
            time.sleep(PER_ITEM_SLEEP_SEC)

        return {
            "ok": True,
            "count": len(results),
            "results": results,
        }

    except Exception as e:
        # 배치 전체가 죽어도 JSON으로 반환
        return {
            "ok": False,
            "count": 0,
            "results": [],
            "error": f"batch error: {str(e)}"[:1000],
        }
