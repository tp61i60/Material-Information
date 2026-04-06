"""
MOPS 不動產重大訊息爬蟲 v3
修復項目：
  1. 加上 `from __future__ import annotations` → 相容 Python 3.8/3.9
  2. spoke_date 日期比對：統一轉成民國年格式 YYYMMDD（7 位）再比對
  3. get_date_range() 同時回傳西元與民國格式日期集合
  4. 移除遺留的 argparse 說明，改以 API 模式為主，保留 main() 供本機測試
  5. get_detail() 硬編碼欄位改用標題文字比對，降低位置偏移風險
  6. fetch() 中 throttle_wait / retry_wait 拆成兩個獨立參數
  7. OUTPUT_FILE 移入 main()，避免模組載入時產生無用常數
"""
from __future__ import annotations  # ← Fix 1：相容 Python 3.8/3.9

import os
import re
import time
import logging
import socket
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import quote
from datetime import datetime, timedelta
from fastapi import FastAPI, Query, HTTPException

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


# ── 主機管理類別 ───────────────────────────────────────────────────────────────

class MopsSession:
    """管理 MOPS HTTP Session 與主機偵測"""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._active_host: str | None = None  # Fix 1 讓此語法在 3.8 也合法

    # ── 工具方法 ──────────────────────────────────────────────────────────────

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

    # ── 主機偵測 ──────────────────────────────────────────────────────────────

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

    # ── HTTP 請求（含重試） ────────────────────────────────────────────────────
    # Fix 6：throttle_wait（限速等待）與 retry_wait（一般錯誤等待）拆開

    def fetch(
        self,
        url: str,
        method: str = "GET",
        data: dict | None = None,
        max_retries: int = 5,
        throttle_wait: int = 60,   # 被限速時等待秒數
        retry_wait:    int = 10,   # 一般錯誤重試間隔秒數
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
                time.sleep(retry_wait)  # Fix 6：使用獨立的 retry_wait

        log.error("超過重試次數，跳過此項目")
        return None


# ── 工具函數 ──────────────────────────────────────────────────────────────────

def clean(text: str) -> str:
    """移除多餘空白"""
    return re.sub(r"\s+", "", text).strip() if text else ""


def truncate_instructions(text: str) -> str:
    """
    保留重大訊息的前 11 點內容，刪除第 12 點（含）以後的所有文字。
    支援多種常見格式：半形/全形數字點、中文序號、全型括號。
    """
    if not text:
        return ""

    pattern = r"(?m)^\s*(?:12\s*[\.．、]|（十二）|十二[、．])"
    parts = re.split(pattern, text, maxsplit=1)
    result = parts[0].strip()
    result = re.sub(r"[\r\n]{3,}", "\n\n", result)
    return result


# Fix 2 & 3：get_date_range() 同時回傳民國年格式日期集合供 spoke_date 比對
def get_date_range(days: int = 7) -> tuple[set, set, set]:
    """
    回傳：
      query_targets    - {(民國年 int, 月 int), ...}  用於組 URL 查詢參數
      valid_roc_dates  - {'YYYMMDD', ...}              民國年格式，用於比對 spoke_date
      valid_ad_dates   - {'YYYYMMDD', ...}             西元格式，備用

    MOPS 的 spoke_date 欄位為民國年 7 位格式，例如 1140101。
    """
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=days)

    dates = []
    curr  = start_dt
    while curr <= end_dt:
        dates.append(curr)
        curr += timedelta(days=1)

    query_targets   = {(d.year - 1911, d.month) for d in dates}
    valid_roc_dates = {
        f"{d.year - 1911:03d}{d.month:02d}{d.day:02d}"   # e.g. 1140404
        for d in dates
    }
    valid_ad_dates  = {d.strftime("%Y%m%d") for d in dates}

    return query_targets, valid_roc_dates, valid_ad_dates


# ── 列表頁解析 ────────────────────────────────────────────────────────────────

def get_list_post_params(
    sess: MopsSession,
    keyword: str,
    year: int,
    month: int,
) -> list[dict]:
    """
    取得搜尋結果列表，回傳各公告的 POST 參數字典。
    同時抓取上市(L)與上櫃(O)。
    """
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

        for i, inp in enumerate(inputs):
            onclick = inp.get("onclick", "")
            onclick = re.sub(r'document\.fm\.|\.value|"', "", onclick)
            onclick = re.sub(r"openWindow\(,\);?", "", onclick)
            parts   = [p.strip() for p in onclick.split(";") if "=" in p.strip()]

            param_dict: dict = {}
            for part in parts:
                k, v = part.split("=", 1)
                param_dict[k.strip()] = v.strip()

            if not param_dict.get("co_id"):
                continue

            param_dict["_check_name"] = names[i] if i < len(names) else ""
            param_dict["_check_date"] = dates[i] if i < len(dates) else ""
            param_dict["_keyword"]    = keyword
            param_dict["_month"]      = month
            param_dict["_kind"]       = kind
            all_records.append(param_dict)

    return all_records


# ── 詳細頁解析 ────────────────────────────────────────────────────────────────

def _find_cell_by_header(soup: BeautifulSoup, header_keyword: str) -> str:
    """
    Fix 5：以標題文字搜尋對應的 td.odd 欄位值，取代硬編碼 row/col。
    在同一列中找到含有 header_keyword 的 th 或標籤，回傳緊接著的 td.odd。
    """
    for tr in soup.select("table tr"):
        # 找含關鍵字的 th 或 td（非 odd）
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
    """
    進入詳細頁，抓取公告完整內容。
    篩選條件：說明本文必須包含「1.標的物之名稱」或關鍵字「委建」。
    """
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

    # ── 說明本文 ──────────────────────────────────────────────────────────────
    instr_el = soup.select_one("tr:nth-of-type(5) td.odd pre")
    if instr_el:
        instructions_raw = instr_el.get_text(separator="\n").strip()
    else:
        tds = soup.select("table tr:nth-of-type(5) td.odd")
        instructions_raw = clean(tds[0].get_text()) if tds else ""

    is_real_estate   = "1.標的物之名稱" in instructions_raw
    is_construction  = "委建" in instructions_raw
    if not (is_real_estate or is_construction):
        return None

    instructions_cleaned = truncate_instructions(instructions_raw)

    # ── 主旨 ──────────────────────────────────────────────────────────────────
    subject_el = (
        soup.select_one("tr:nth-of-type(3) td.odd pre font")
        or soup.select_one("tr:nth-of-type(3) td.odd")
    )
    subject = clean(subject_el.get_text()) if subject_el else ""

    # Fix 5：改用標題文字比對，不再依賴硬編碼位置
    terms           = _find_cell_by_header(soup, "條款")   # 視實際欄位標題調整
    occurrence_date = _find_cell_by_header(soup, "發生日期")

    # 若文字比對找不到，回退到原本位置抓法（保險）
    if not terms or not occurrence_date:
        trs = soup.select("table tr")
        if len(trs) >= 4:
            tds = trs[3].select("td.odd")
            if len(tds) >= 2:
                terms           = terms or clean(tds[0].get_text())
                occurrence_date = occurrence_date or clean(tds[1].get_text())

    # ── 委建：標題與說明本文均須含「委建」才保留 ──────────────────────────────
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


# ── 共用爬取邏輯（main 與 API 共用） ──────────────────────────────────────────

def run_crawl(days: int = 7) -> list[dict]:
    """
    核心爬取流程，回傳結果清單。
    Fix 2：使用 valid_roc_dates（民國年格式）比對 spoke_date。
    """
    sess = MopsSession()
    sess.detect_host()

    all_results: list[dict] = []
    seen_keys:   set        = set()

    # Fix 3：取得民國年日期集合
    query_targets, valid_roc_dates, _ = get_date_range(days=days)
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
                # Fix 2：比對民國年格式 spoke_date
                if param_dict.get("spoke_date") not in valid_roc_dates:
                    continue

                dedup_key = (
                    param_dict["co_id"],
                    param_dict["spoke_date"],
                    param_dict["seq_no"],
                )
                if dedup_key in seen_keys:
                    continue

                log.info(
                    "  → 抓取 %s (%s)...",
                    param_dict["co_id"],
                    param_dict.get("_check_name", ""),
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


# ── 本機測試入口（Fix 4：移除遺留的 argparse，改為直接帶參數） ─────────────────

def main(days: int = 7) -> None:
    # Fix 7：OUTPUT_FILE 移入 main()，避免模組載入時產生無用常數
    output_file = f"mops_real_estate_{datetime.now().strftime('%Y%m%d')}.csv"
    results = run_crawl(days=days)
    _save(results, output_file)
    log.info("✅ 完成！共抓取 %d 筆 → %s", len(results), output_file)


# ── FastAPI 路由 ───────────────────────────────────────────────────────────────

@app.get("/fetch-news")
async def fetch_news(days: int = Query(default=7, ge=1, le=30)) -> dict:
    """
    抓取重大訊息 API（供 n8n 或其他服務呼叫）
    """
    try:
        results = run_crawl(days=days)
        return {"count": len(results), "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Zeabur / 本機啟動 ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)