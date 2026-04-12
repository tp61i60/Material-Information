"""
MOPS 不動產重大訊息爬蟲 v5版本 (API & CLI 雙模式修復版 )
修復項目：
  1. 雙模式啟動：可作為腳本直接產出 CSV，也可作為 FastAPI 伺服器運行。
  2. onclick 解析退回 V5 強健的字串切割法，避免正則表達式漏抓單引號格式。
  3. spoke_date 格式比對：自動偵測 7 位民國年或 8 位西元格式，雙軌比對。
  4. FastAPI async route 改用 run_in_executor 包住阻塞式爬蟲。
"""
from __future__ import annotations

import asyncio
import os
import re
import time
import logging
import socket
import argparse
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import quote
from datetime import datetime, timedelta
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from concurrent.futures import ThreadPoolExecutor
import urllib3                                                    # ← 新增
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  

# ── 設定 ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="MOPS Real Estate API")

SEARCH_KEYWORDS = ["不動產使用權", "租賃", "出租", "續租", "委建"]
SLEEP_SEC       = 2.0

MOPS_HOSTS = [
    "https://mopsc.twse.com.tw",
    "https://mopsov.twse.com.tw",
    "https://mops.twse.com.tw",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# 全域 thread pool（給 run_in_executor 用）
_executor = ThreadPoolExecutor(max_workers=1)


# ── 主機管理類別 ───────────────────────────────────────────────────────────────

class MopsSession:
    """管理 MOPS HTTP Session 與主機偵測"""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.verify = False          # ← 加這行
        self._active_host: str | None = None

    @staticmethod
    def _is_blocked(html: str) -> bool:
        return "PAGE CANNOT BE ACCESSED" in html or "安全性考量" in html

    @staticmethod
    def _is_throttled(html: str) -> bool:
        return "查詢過於頻繁" in html or "系統忙碌" in html

    @staticmethod
    def _is_dns_error(exc: Exception) -> bool:
        if isinstance(exc, socket.gaierror):
            return True
        if isinstance(exc, requests.exceptions.ConnectionError):
            cause = str(exc).lower()
            return "name or service not known" in cause or "nodename nor servname" in cause
        return False

    @staticmethod
    def _replace_host(url: str, new_host: str) -> str:
        parts = url.split("/", 3)
        if len(parts) >= 4:
            return new_host.rstrip("/") + "/" + parts[3]
        return new_host.rstrip("/") + "/" + url

    def detect_host(self, force: bool = False) -> str:
        if self._active_host and not force:
            return self._active_host

        if force:
            log.warning("主機 %s 失效，重新偵測...", self._active_host)
            self._active_host = None
        else:
            log.info("偵測可用的 MOPS 主機...")

        test_path = (
            "/mops/web/ajax_t51sb10"
            "?encodeURIComponent=1&firstin=true&Stp=4&KIND=L"
            "&keyWord=test&year=114&month1=1&begin_day=1&end_day=1"
        )

        for host in MOPS_HOSTS:
            try:
                self.session.get(host + "/mops/web/index", timeout=10)
                time.sleep(0.5)
                resp = self.session.get(host + test_path, timeout=15)
                resp.encoding = "utf-8"
                if self._is_blocked(resp.text):
                    log.warning("  ✗ %s → 被封鎖（非台灣 IP）", host)
                    continue
                if resp.status_code == 200 and len(resp.text) > 100:
                    log.info("  ✓ %s → 可用", host)
                    self._active_host = host
                    return host
            except Exception as e:
                log.warning("  ✗ %s → 連線失敗：%s", host, e)

        raise ConnectionError(
            "\n❌ 所有 MOPS 主機均無法存取！\n"
            "請確認您的 IP 在台灣境內，或使用台灣 VPN/代理伺服器。"
        )

    def fetch(
        self,
        url: str,
        method: str = "GET",
        data: dict | None = None,
        max_retries: int = 5,
        throttle_wait: int = 60,
        retry_wait:    int = 10,
    ) -> BeautifulSoup | None:
        host = self.detect_host()

        for attempt in range(max_retries):
            current_url = self._replace_host(url, host)
            self.session.headers.update({"Referer": host + "/mops/web/t51sb10"})

            try:
                if method == "POST":
                    resp = self.session.post(current_url, data=data, timeout=30)
                else:
                    resp = self.session.get(current_url, timeout=30)
                resp.encoding = "utf-8"
                html = resp.text

                if self._is_blocked(html):
                    raise ConnectionError("IP 被封鎖，請改用台灣境內 IP")

                if self._is_throttled(html):
                    log.warning("查詢過於頻繁，等待 %d 秒後重試...", throttle_wait)
                    time.sleep(throttle_wait)
                    continue

                return BeautifulSoup(html, "html.parser")

            except ConnectionError:
                raise

            except Exception as e:
                if self._is_dns_error(e):
                    log.warning("DNS 解析失敗（%s），切換主機...", host)
                    host = self.detect_host(force=True)
                    continue

                log.warning("請求失敗（第 %d 次）：%s", attempt + 1, e)
                time.sleep(retry_wait)

        log.error("超過重試次數，跳過此項目")
        return None


# ── 工具函數 ──────────────────────────────────────────────────────────────────

def clean(text: str) -> str:
    return re.sub(r"\s+", "", text).strip() if text else ""


def truncate_instructions(text: str) -> str:
    if not text:
        return ""
    pattern = r"(?m)^\s*(?:12\s*[\.．、]|（十二）|十二[、．])"
    parts = re.split(pattern, text, maxsplit=1)
    result = parts[0].strip()
    result = re.sub(r"[\r\n]{3,}", "\n\n", result)
    return result


def get_date_range(days: int = 7) -> tuple[set, set, set]:
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=days)

    dates = []
    curr  = start_dt
    while curr <= end_dt:
        dates.append(curr)
        curr += timedelta(days=1)

    query_targets   = {(d.year - 1911, d.month) for d in dates}
    valid_roc_dates = {
        f"{d.year - 1911:03d}{d.month:02d}{d.day:02d}" for d in dates
    }
    valid_ad_dates  = {d.strftime("%Y%m%d") for d in dates}

    return query_targets, valid_roc_dates, valid_ad_dates


def is_date_in_range(spoke_date: str, valid_roc_dates: set, valid_ad_dates: set) -> bool:
    if not spoke_date:
        return False
    if len(spoke_date) == 8:
        return spoke_date in valid_ad_dates
    if len(spoke_date) == 7:
        return spoke_date in valid_roc_dates
    # 寬鬆比對（取後 6 位月日部分）
    log.debug("spoke_date 格式未知：%s，嘗試寬鬆比對", spoke_date)
    for d in valid_ad_dates:
        if spoke_date.endswith(d[4:]):
            return True
    return False


# ── 列表頁解析 ────────────────────────────────────────────────────────────────

def _parse_onclick(onclick: str) -> dict:
    """退回 V5 最穩定的字串切割法，捨棄正則表達式，避免 MOPS 單雙引號混用導致錯誤"""
    # 移除前綴與所有可能的引號
    onclick = re.sub(r'document\.fm\.|\.value|["\']', "", onclick)
    onclick = re.sub(r"openWindow\(.*?\);?", "", onclick)
    parts = [p.strip() for p in onclick.split(";") if "=" in p.strip()]
    
    result = {}
    for part in parts:
        k, v = part.split("=", 1)
        k = k.strip()
        if k and k not in ("window", "location"):
            result[k] = v.strip()
    return result


def get_list_post_params(sess: MopsSession, keyword: str, year: int, month: int) -> list[dict]:
    host        = sess.detect_host()
    all_records = []

    for kind in ("L", "O"):
        encoded_kw = quote(keyword, encoding="utf-8")
        url = (
            f"{host}/mops/web/ajax_t51sb10"
            f"?encodeURIComponent=1&firstin=true&id=&key=&TYPEK=&Stp=4&go=false"
            f"&COMPANY_ID=&r1=1&KIND={kind}&CODE=&keyWord={encoded_kw}"
            f"&year={year}&month1={month}&begin_day=1&end_day=31"
        )

        soup = sess.fetch(url)
        if soup is None:
            continue

        names, dates = [], []
        for tr in soup.select("table tr"):
            tds = tr.find_all("td")
            if len(tds) >= 3:
                names.append(clean(tds[1].get_text()))
                dates.append(tds[2].get_text().strip())

        inputs = soup.select("td input[onclick]")
        log.debug("  [%s/%s] 找到 %d 個 input 按鈕", kind, keyword, len(inputs))

        for i, inp in enumerate(inputs):
            onclick    = inp.get("onclick", "")
            param_dict = _parse_onclick(onclick)

            if not param_dict.get("co_id"):
                continue

            if not param_dict.get("spoke_date") and param_dict.get("date"):
                param_dict["spoke_date"] = param_dict.pop("date")

            param_dict["_check_name"] = names[i] if i < len(names) else ""
            param_dict["_check_date"] = dates[i] if i < len(dates) else ""
            param_dict["_keyword"]    = keyword
            param_dict["_month"]      = month
            param_dict["_kind"]       = kind
            all_records.append(param_dict)

    return all_records


# ── 詳細頁解析 ────────────────────────────────────────────────────────────────

def _find_cell_by_header(soup: BeautifulSoup, header_keyword: str) -> str:
    for tr in soup.select("table tr"):
        headers = tr.find_all(
            lambda tag: tag.name in ("th", "td")
            and "odd" not in tag.get("class", [])
            and header_keyword in tag.get_text()
        )
        if headers:
            td = tr.find("td", class_="odd")
            if td:
                return clean(td.get_text())
    return ""


def get_detail(sess: MopsSession, param_dict: dict, year: int) -> dict | None:
    host = sess.detect_host()
    url  = f"{host}/mops/web/ajax_t05st01"

    co_id      = param_dict.get("co_id", "")
    spoke_date = param_dict.get("spoke_date", "")
    seq_no     = param_dict.get("seq_no", "")
    spoke_time = param_dict.get("spoke_time", "")

    post_data = {
        "step": "2", "colorchg": "1", "co_id": co_id,
        "spoke_date": spoke_date, "seq_no": seq_no,
        "spoke_time": spoke_time, "off": "1",
        "firstin": "1", "year": str(year),
        "t51sb10": "t51sb10", "b_date": "1", "e_date": "1",
    }

    soup = sess.fetch(url, method="POST", data=post_data)
    if soup is None:
        return None

    page_text = soup.get_text()
    if "查無所需資料" in page_text or "資料庫查無所需資料" in page_text:
        return None

    instr_el = soup.select_one("tr:nth-of-type(5) td.odd pre")
    if instr_el:
        instructions_raw = instr_el.get_text(separator="\n").strip()
    else:
        tds = soup.select("table tr:nth-of-type(5) td.odd")
        instructions_raw = clean(tds[0].get_text()) if tds else ""

    is_real_estate  = "1.標的物之名稱" in instructions_raw
    is_construction = "委建" in instructions_raw
    if not (is_real_estate or is_construction):
        return None

    instructions_cleaned = truncate_instructions(instructions_raw)

    subject_el = (
        soup.select_one("tr:nth-of-type(3) td.odd pre font")
        or soup.select_one("tr:nth-of-type(3) td.odd")
    )
    subject = clean(subject_el.get_text()) if subject_el else ""

    terms           = _find_cell_by_header(soup, "條款")
    occurrence_date = _find_cell_by_header(soup, "發生日期")

    if not terms or not occurrence_date:
        trs = soup.select("table tr")
        if len(trs) >= 4:
            tds = trs[3].select("td.odd")
            if len(tds) >= 2:
                terms           = terms           or clean(tds[0].get_text())
                occurrence_date = occurrence_date or clean(tds[1].get_text())

    if param_dict.get("_keyword") == "委建":
        if "委建" not in subject or "委建" not in instructions_cleaned:
            return None

    return {
        "stock_code":      co_id,
        "subject":         subject,
        "terms":           terms,
        "occurrence_date": occurrence_date,
        "instructions":    instructions_cleaned,
        "search_keyword":  param_dict.get("_keyword", ""),
        "search_month":    param_dict.get("_month", ""),
        "announce_date":   spoke_date,
    }


# ── 共用爬取邏輯 ───────────────────────────────────────────────────────────────

def run_crawl(days: int = 7) -> list[dict]:
    sess = MopsSession()
    sess.detect_host()

    all_results: list[dict] = []
    seen_keys:   set        = set()

    query_targets, valid_roc_dates, valid_ad_dates = get_date_range(days=days)
    log.info("查詢範圍：過去 %d 天，涵蓋 %d 個年月組合", days, len(query_targets))

    for keyword in SEARCH_KEYWORDS:
        log.info("─── 關鍵字：%s ───", keyword)

        for year, month in sorted(query_targets):
            log.info("  搜尋民國 %d 年 %02d 月...", year, month)
            params_list = get_list_post_params(sess, keyword, year, month)

            if not params_list:
                log.info("    (無結果)")
                continue

            for param_dict in params_list:
                spoke_date = param_dict.get("spoke_date", "")

                if not is_date_in_range(spoke_date, valid_roc_dates, valid_ad_dates):
                    continue

                dedup_key = (
                    param_dict.get("co_id", ""),
                    spoke_date,
                    param_dict.get("seq_no", ""),
                )
                if dedup_key in seen_keys:
                    continue

                log.info(
                    "  → 抓取 %s (%s) spoke_date=%s...",
                    param_dict.get("co_id"), param_dict.get("_check_name", ""), spoke_date,
                )
                time.sleep(SLEEP_SEC)

                detail = get_detail(sess, param_dict, year)
                if detail:
                    seen_keys.add(dedup_key)
                    all_results.append(detail)
                    log.info("    ✓ 符合，累計 %d 筆", len(all_results))
                else:
                    log.debug("    跳過（不符篩選條件）")

    return all_results


# ── 儲存 CSV ──────────────────────────────────────────────────────────────────

def _save(results: list, filepath: str) -> None:
    if results:
        df   = pd.DataFrame(results)
        cols = ["announce_date", "stock_code", "subject",
                "occurrence_date", "terms", "search_keyword",
                "search_month", "instructions"]
        cols = [c for c in cols if c in df.columns]
        df[cols].to_csv(filepath, index=False, encoding="utf-8-sig")
        log.info("已儲存至 %s（%d 列）", filepath, len(df))
    else:
        log.warning("無結果，未產生 CSV 檔案")


def main(days: int = 7) -> None:
    output_file = f"mops_real_estate_{datetime.now().strftime('%Y%m%d')}.csv"
    results = run_crawl(days=days)
    _save(results, output_file)
    log.info("✅ 完成！共抓取 %d 筆 → %s", len(results), output_file)


# ── FastAPI 路由 ───────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "time": datetime.now().isoformat()}

@app.get("/fetch-news")
async def fetch_news(days: int = Query(default=7, ge=1, le=30)) -> JSONResponse:
    try:
        loop    = asyncio.get_event_loop()
        results = await loop.run_in_executor(_executor, run_crawl, days)
        return JSONResponse(content={"count": len(results), "data": results})
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        log.exception("爬取失敗")
        raise HTTPException(status_code=500, detail=str(e))


# ── 啟動邏輯 (支援 CLI 直接產檔 或 啟動 API) ──────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MOPS 爬蟲 (雙模式)")
    parser.add_argument("--api", action="store_true", help="啟動 FastAPI 伺服器 (供 n8n / 外部呼叫)")
    parser.add_argument("--days", type=int, default=7, help="查詢過去幾天的公告 (預設: 7)")
    args = parser.parse_args()

    if args.api:
        # 模式 1：啟動伺服器模式 (不會馬上跑，需要去戳 endpoint)
        import uvicorn
        port = int(os.environ.get("PORT", 8080))
        log.info(f"啟動 FastAPI 伺服器... 請用瀏覽器或工具呼叫 http://localhost:{port}/fetch-news")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        # 模式 2：腳本模式 (直接執行並產出 CSV)
        log.info(f"以腳本模式啟動... 將直接爬取過去 {args.days} 天的資料並儲存 CSV")
        main(days=args.days)