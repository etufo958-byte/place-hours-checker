import re
import time
from datetime import datetime
from typing import List, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from pydantic import BaseModel
from playwright.sync_api import sync_playwright

app = FastAPI(title="Naver Place Hours Checker")

REQUEST_TIMEOUT = 20
PER_ITEM_SLEEP_SEC = 1.0
ENABLE_BROWSER_FALLBACK = True

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
        "정기휴무", "휴무", "점심시간", "월", "화", "수", "목", "금", "토", "일", "매일"
    ]

    time_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*[-~]\s*([01]?\d|2[0-3]):[0-5]\d"
    )

    result = []
    for i, line in enumerate(lines):
        if any(k in line for k in keywords) or time_pattern.search(line):
            result.append(line)

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
    if not lines:
        return False

    text = "\n".join(lines)
    if any(k in text for k in ["영업시간", "브레이크타임", "라스트오더", "정기휴무", "점심시간"]):
        return True

    time_pattern = re.compile(
        r"([01]?\d|2[0-3]):[0-5]\d\s*[-~]\s*([01]?\d|2[0-3]):[0-5]\d"
    )
    return bool(time_pattern.search(text))


def fetch_text_via_requests(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    # 핵심: 인코딩 강제 보정
    if not resp.encoding or resp.encoding.lower() in ["iso-8859-1", "ascii"]:
        resp.encoding = resp.apparent_encoding or "utf-8"

    html = resp.text

    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    text = soup.get_text("\n", strip=True)
    return clean_text(text)


def fetch_text_via_browser(page, url: str) -> str:
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(2500)
    text = page.locator("body").inner_text(timeout=10000)
    return clean_text(text)


def make_browser():
    p = sync_playwright().start()
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-zygote",
            "--single-process",
        ],
    )
    return p, browser


def check_one_mid(mid: str, store_name: Optional[str], page=None) -> Dict[str, Any]:
    candidates = url_candidates(mid)
    errors = []

    for url in candidates:
        # 1차 requests
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
        except Exception as e:
            errors.append(f"[requests] {url} -> {str(e)}")

        # 2차 browser fallback
        if ENABLE_BROWSER_FALLBACK and page is not None:
            try:
                text = fetch_text_via_browser(page, url)
                lines = extract_hours_lines_from_text(text)
                has_hours = judge_has_hours(lines)

                return {
                    "mid": mid,
                    "storeName": store_name or "",
                    "hasHours": bool(has_hours),
                    "rawText": "\n".join(lines),
                    "status": "완료" if has_hours else "완료",
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

    p = None
    browser = None
    context = None
    page = None

    try:
        if ENABLE_BROWSER_FALLBACK:
            p, browser = make_browser()
            context = browser.new_context(locale="ko-KR")
            page = context.new_page()

        for item in payload.items:
            result = check_one_mid(item.mid, item.storeName, page=page)
            results.append(result)
            time.sleep(PER_ITEM_SLEEP_SEC)

    finally:
        try:
            if page:
                page.close()
        except:
            pass
        try:
            if context:
                context.close()
        except:
            pass
        try:
            if browser:
                browser.close()
        except:
            pass
        try:
            if p:
                p.stop()
        except:
            pass

    return {
        "ok": True,
        "count": len(results),
        "results": results,
    }
