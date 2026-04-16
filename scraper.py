import os
import json
import logging
import requests
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

NANSEN_EMAIL    = os.environ.get("NANSEN_EMAIL", "")
NANSEN_PASSWORD = os.environ.get("NANSEN_PASSWORD", "")
SPREADSHEET_ID  = os.environ.get("SPREADSHEET_ID")

TOKENS = [
    {"symbol": "AKE", "address": "0x2c3a8ee94ddd97244a93bc48298f97d2c412f7db", "chain": "bnb"},
]


def get_token_via_playwright():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = {runtime: {}};
        """)
        page = context.new_page()

        token_holder = {}

        def handle_request(request):
            auth = request.headers.get("authorization", "")
            if auth.startswith("Bearer ") and "nansen.ai/api" in request.url:
                token_holder["token"] = auth.replace("Bearer ", "")

        page.on("request", handle_request)

        log.info("Opening Nansen login page...")
        page.goto("https://app.nansen.ai/login", timeout=60000)
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

        log.info("URL: " + page.url)
        log.info("Title: " + page.title())
        html = page.content()
        log.info("HTML length: " + str(len(html)))
        log.info("HTML snippet: " + html[:2000])

        try:
            page.wait_for_selector('input[type="email"]', timeout=15000)
            page.fill('input[type="email"]', NANSEN_EMAIL)
            log.info("Filled email")
        except Exception as e:
            log.error(f"Email input not found: {e}")
            raise Exception(f"Email input not found: {e}")

        try:
            page.wait_for_selector('input[type="password"]', timeout=10000)
            page.fill('input[type="password"]', NANSEN_PASSWORD)
            log.info("Filled password")
        except Exception as e:
            raise Exception(f"Password input not found: {e}")

        try:
            page.click('button[type="submit"]', timeout=10000)
            log.info("Clicked submit")
        except Exception as e:
            raise Exception(f"Submit button not found: {e}")

        page.wait_for_load_state("domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        log.info("After login URL: " + page.url)

        page.goto(
            "https://app.nansen.ai/token-god-mode?tokenAddress=0x2c3a8ee94ddd97244a93bc48298f97d2c412f7db&chain=bnb",
            timeout=60000
        )
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        page.wait_for_timeout(8000)
        log.info("Tokens captured: " + str(len(token_holder)))

        browser.close()

        if "token" not in token_holder:
            raise Exception("Could not capture Bearer token")

        log.info("Got Bearer token!")
        return token_holder["token"]


def fetch_gini_stats(token, address, chain):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": "https://app.nansen.ai",
        "Referer": "https://app.nansen.ai/token-god-mode",
        "User-Agent": "Mozilla/5.0",
    }
    payload = {
        "parameters": {
            "chain": chain,
            "tokenAddress": address.lower(),
        }
    }
    url = "https://app.nansen.ai/api/questions/tgm-holders-gini-stats"
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    log.info(f"gini-stats status: {r.status_code}")
    if r.status_code == 200:
        return r.json()
    log.warning(f"Response: {r.text[:300]}")
    return None


def parse_data(raw):
    if not raw:
        return {"holders": "N/A", "top100_pct": "N/A", "fresh_pct": "N/A"}
    data = raw.get("data", [{}])
    if isinstance(data, list) and len(data) > 0:
        data = data[0]
    elif not isinstance(data, dict):
        data = {}

    holders = data.get("totalHolders", "N/A")
    top100  = data.get("top100HoldersBalancePercent", "N/A")
    fresh   = data.get("freshWalletBalancePercent", "N/A")

    def fmt(v):
        if isinstance(v, (int, float)):
            return str(round(v * 100, 2)) + "%"
        return v

    return {
        "holders":    int(holders) if isinstance(holders, (int, float)) else holders,
        "top100_pct": fmt(top100),
        "fresh_pct":  fmt(fresh),
    }


def get_sheet():
    creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet("Nansen Data")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("Nansen Data", rows=1000, cols=20)
    return ws


def ensure_headers(ws):
    headers = [
        "Timestamp (UTC)", "Symbol", "Contract Address",
        "Holders", "Top 100 Holders %", "Fresh Wallets %",
    ]
    if ws.row_values(1) != headers:
        ws.insert_row(headers, 1)


def run():
    log.info("Starting...")
    token = get_token_via_playwright()
    log.info("Got token successfully!")

    ws = get_sheet()
    ensure_headers(ws)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_rows = []

    for t in TOKENS:
        symbol  = t["symbol"]
        address = t["address"]
        chain   = t["chain"]
        log.info(f"Fetching {symbol}...")
        try:
            raw    = fetch_gini_stats(token, address, chain)
            fields = parse_data(raw)
        except Exception as e:
            log.error(f"Error fetching {symbol}: {e}")
            fields = {"holders": "ERROR", "top100_pct": "ERROR", "fresh_pct": "ERROR"}

        new_rows.append([
            timestamp, symbol, address,
            fields["holders"], fields["top100_pct"], fields["fresh_pct"],
        ])
        log.info(f"Done {symbol}: {fields}")

    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    log.info("All done!")


if __name__ == "__main__":
    run()
