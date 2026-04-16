import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
from typing import Optional
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TOKENS = [
    {"symbol": "IRYS", "address": "0x50f41F589aFACa2EF41FDF590FE7b90cD26DEe64"},
    {"symbol": "LYN", "address": "0x302DFaF2CDbE51a18d97186A7384e87CF599877D"},
    {"symbol": "BARD", "address": "0xf0DB65D17e30a966C2ae6A21f6BBA71cea6e9754"},
    {"symbol": "AKE", "address": "0x2c3a8Ee94dDD97244a93Bc48298f97d2C412F7Db"},
    {"symbol": "ZAMA", "address": "0xa12cc123ba206d4031d1c7f6223d1c2ec249f4f3"},
    {"symbol": "STBL", "address": "0x8dedf84656fa932157e27c060d8613824e7979e3"},
    {"symbol": "PLAY", "address": "0x853a7c99227499dba9db8c3a02aa691afdebf841"},
    {"symbol": "CLO", "address": "0x81D3A238b02827F62B9f390f947D36d4A5bf89D2"},
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://app.nansen.ai/",
}

def fetch_token_data(address):
    addr = address.lower()
    try:
        url = "https://app.nansen.ai/api/portfolio/token/" + addr
        r = requests.get(url, headers=HEADERS_BROWSER, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("API failed for " + address + ": " + str(e))
    try:
        import re
        url = "https://app.nansen.ai/token/" + addr
        r = requests.get(url, headers=HEADERS_BROWSER, timeout=30)
        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', r.text, re.DOTALL)
        if match:
            nd = json.loads(match.group(1))
            props = nd.get("props", {}).get("pageProps", {})
            return props.get("tokenData") or props
    except Exception as e:
        log.error("Scrape failed for " + address + ": " + str(e))
    return None

def parse_fields(raw):
    if not raw:
        return {"holders": "N/A", "top100_pct": "N/A", "fresh_wallets_pct": "N/A"}
    holders = raw.get("holderCount") or raw.get("holders") or raw.get("total_holders") or "N/A"
    top100 = raw.get("top100HoldersPct") or raw.get("supplyHeldByTop100") or raw.get("topHoldersPct") or "N/A"
    fresh = raw.get("freshWalletsPct") or raw.get("supplyHeldByFreshWallets") or raw.get("fresh_wallets_pct") or "N/A"
    def fmt_pct(v):
        if isinstance(v, (int, float)):
            return str(round(v, 2)) + "%"
        if isinstance(v, str) and v != "N/A":
            return v if "%" in v else v + "%"
        return v
    return {"holders": int(holders) if isinstance(holders, (int, float)) else holders,
            "top100_pct": fmt_pct(top100), "fresh_wallets_pct": fmt_pct(fresh)}

def get_sheet():
    creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet("Nansen Data")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("Nansen Data", rows=1000, cols=20)
    return ws

def ensure_headers(ws):
    headers = ["Timestamp (UTC)", "Symbol", "Contract Address", "Holders", "Top 100 Holders %", "Fresh Wallets %"]
    if ws.row_values(1) != headers:
        ws.insert_row(headers, 1)

def run():
    log.info("Starting Nansen data collection...")
    ws = get_sheet()
    ensure_headers(ws)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_rows = []
    for token in TOKENS:
        symbol = token["symbol"]
        address = token["address"]
        log.info("Fetching " + symbol)
        try:
            raw = fetch_token_data(address)
            fields = parse_fields(raw)
        except Exception as e:
            log.error("Error " + symbol + ": " + str(e))
            fields = {"holders": "ERROR", "top100_pct": "ERROR", "fresh_wallets_pct": "ERROR"}
        new_rows.append([timestamp, symbol, address, fields["holders"], fields["top100_pct"], fields["fresh_wallets_pct"]])
        log.info("Done " + symbol + ": " + str(fields))
        time.sleep(1.5)
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    log.info("All done!")

if __name__ == "__main__":
    run()
