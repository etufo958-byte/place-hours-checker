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
    """
    텍스트 전체에서 영업시간 관련 줄만 추출
    """
    text = clean_text(text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    keywords = [
        "영업시간", "브레이크타임", "라스트오더",
        "정기휴무", "휴무", "점심시간", "월", "화", "수", "목", "금", "토", "일", "매일"
    ]

    time_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*[-~∼]\s*([01]?\d|2[0-3]):[0-5]\d"
    )

    result = []
    for i, line in enumerate(lines):
        if any(k in line for k in keywords) or time_pattern.search(line):
            result.append(line)

            # 다음 줄도 이어서 가져오기
            if i + 1 < len(lines):
                nxt = lines[i + 1]
                if nxt not in result and (
                    any(k in nxt for k in keywords) or time_pattern.search(nxt)
                ):
                    result.append(nxt)

    dedup = []
    seen = set()
    for line in result:
        if line not in seen:
            dedup.append(line)
            seen.add(line)

    return dedup[:20]


def judge_has_hours(lines: List[str]) -> bool:
    """
    O / X 판정
    """
    if not lines:
        return False

    text = "\n".join(lines)

    if any(k in text for k in ["영업시간", "브레이크타임", "라스트오더", "정기휴무", "점심시간"]):
        return True

    time_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*[-~∼]\s*([01]?\d|2[0-3]):[0-5]\d"
    )
    return bool(time_pattern.search(text))


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
