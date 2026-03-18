import re
import time
from datetime import datetime
from typing import List, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from pydantic import BaseModel

# Playwright fallback
from playwright.sync_api import sync_playwright

app = FastAPI(title="Naver Place Hours Checker")

REQUEST_TIMEOUT = 20
ENABLE_BROWSER_FALLBACK = True
BROWSER_WAIT_MS = 1800

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

# 너무 공격적으로 돌리지 않도록 소폭 지연
PER_ITEM_SLEEP_SEC = 1.2


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
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def extract_hours_lines_from_text(text: str) -> List[str]:
    """
    body text에서 영업시간 관련 줄만 추출
    단순 O/X 판별이 목적이라 과도한 구조화보다
    '영업시간 정보가 존재하는지' 중심으로 판별
    """
    text = clean_text(text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    keywords = [
        "영업시간", "브레이크타임", "라스트오더",
        "정기휴무", "휴무", "매일", "월", "화", "수", "목", "금", "토", "일"
    ]

    time_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*[-~]\s*([01]?\d|2[0-3]):[0-5]\d"
    )

    result = []
    for i, line in enumerate(lines):
        if any(k in line for k in keywords) or time_pattern.search(line):
            result.append(line)

            # 다음 줄이 이어지는 시간 정보일 수 있으니 1줄 추가
            if i + 1 < len(lines):
                nxt = lines[i + 1]
                if nxt not in result and (
                    any(k in nxt for k in keywords) or time_pattern.search(nxt)
                ):
                    result.append(nxt)

    # 중복 제거
    dedup = []
    seen = set()
    for line in result:
        if line not in seen:
            dedup.append(line)
            seen.add(line)
    return dedup[:15]


def judge_has_hours(lines: List[str]) -> bool:
    """
    단순 판별:
    - 영업시간/브레이크타임/라스트오더/휴무 키워드가 있거나
    - HH:MM-HH:MM 패턴이 있으면 O
    """
    if not lines:
        return False

    text = "\n".join(lines)
    if any(k in text for k in ["영업시간", "브레이크타임", "라스트오더", "정기휴무"]):
        return True

    time_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*[-~]\s*([01]?\d|2[0-3]):[0-5]\d"
    )
    return bool(time_pattern.search(text))


def fetch_with_requests(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def fetch_text_via_requests(url: str) -> str:
    html = fetch_with_requests(url)
    soup = BeautifulSoup(html, "lxml")

    # script/style 제거
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    text = soup.get_text("\n", strip=True)
    return clean_text(text)


def fetch_text_via_browser(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page(locale="ko-KR")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(BROWSER_WAIT_MS)

            # body 전체 텍스트 우선
            text = page.locator("body").inner_text(timeout=5000)
            return clean_text(text)
        finally:
            browser.close()


def check_one_mid(mid: str, store_name: Optional[str] = None) -> Dict[str, Any]:
    candidates = url_candidates(mid)

    errors = []
    for url in candidates:
        # 1차: requests
        try:
            text = fetch_text_via_requests(url)
            lines = extract_hours_lines_from_text(text)
            has_hours = judge_has_hours(lines)

            if has_hours:
                return {
                    "mid": mid,
                    "storeName": store_name or "",
                    "hasHours": True,
                    "rawText": "\n".join(lines),
                    "status": "완료",
                    "error": "",
                    "sourceUrl": url,
                    "checkedAt": now_str(),
                    "sourceType": "requests",
                }

            # requests로 못 잡았으면 fallback 후보로 계속 진행
        except Exception as e:
            errors.append(f"[requests] {url} -> {str(e)}")

        # 2차: Playwright fallback
        if ENABLE_BROWSER_FALLBACK:
            try:
                text = fetch_text_via_browser(url)
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
                    "sourceType": "playwright",
                }
            except Exception as e:
                errors.append(f"[browser] {url} -> {str(e)}")

    return {
        "mid": mid,
        "storeName": store_name or "",
        "hasHours": False,
        "rawText": "",
        "status": "실패",
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

    for item in payload.items:
        result = check_one_mid(item.mid, item.storeName)
        results.append(result)
        time.sleep(PER_ITEM_SLEEP_SEC)

    return {
        "ok": True,
        "count": len(results),
        "results": results,
    }
