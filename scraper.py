import os
import json
import logging
import requests
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

NANSEN_API_KEY  = os.environ.get("NANSEN_API_KEY", "")
SPREADSHEET_ID  = os.environ.get("SPREADSHEET_ID")

TOKENS = [
    {"symbol": "AKE", "address": "0x2c3a8ee94ddd97244a93bc48298f97d2c412f7db", "chain": "bnb"},
]


def fetch_gini_stats(address, chain):
    headers = {
        "Authorization": f"Bearer {NANSEN_API_KEY}",
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
    log.info(f"Status: {r.status_code}")
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
    log.info("Starting...")
    ws = get_sheet()
    ensure_headers(ws)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_rows = []
    for t in TOKENS:
        log.info(f"Fetching {t['symbol']}...")
        try:
            raw = fetch_gini_stats(t["address"], t["chain"])
            fields = parse_data(raw)
        except Exception as e:
            log.error(f"Error: {e}")
            fields = {"holders": "ERROR", "top100_pct": "ERROR", "fresh_pct": "ERROR"}
        new_rows.append([timestamp, t["symbol"], t["address"], fields["holders"], fields["top100_pct"], fields["fresh_pct"]])
        log.info(f"Done: {fields}")
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    log.info("All done!")

if __name__ == "__main__":
    run()
