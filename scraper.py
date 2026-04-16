import os
import json
import time
import logging
import requests
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TOKENS = [
    {"symbol": "IRYS", "address": "0x50f41F589aFACa2EF41FDF590FE7b90cD26DEe64"},
    {"symbol": "AKE",  "address": "0x2c3a8Ee94dDD97244a93Bc48298f97d2C412F7Db"},
]

NANSEN_API_KEY = os.environ.get("NANSEN_API_KEY", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

def fetch_nansen_data(address):
    addr = address.lower()
    headers = {
        "Content-Type": "application/json",
        "apiKey": NANSEN_API_KEY,
    }
    try:
        url = "https://api.nansen.ai/api/v1/token/" + addr + "/holder-stats"
        r = requests.get(url, headers=headers, timeout=20)
        log.info("holder-stats status: " + str(r.status_code))
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        log.warning("holder-stats failed: " + str(e))
    try:
        url = "https://api.nansen.ai/api/v1/token-god-mode/token/summary"
        payload = {"chain": "ethereum", "token_address": addr}
        r = requests.post(url, headers=headers, json=payload, timeout=20)
        log.info("token-summary status: " + str(r.status_code))
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        log.warning("token-summary failed: " + str(e))
    return None

def parse_fields(raw):
    if not raw:
        return {"holders": "N/A", "top100_pct": "N/A", "fresh_wallets_pct": "N/A"}
    log.info("Raw keys: " + str(list(raw.keys()) if isinstance(raw, dict) else "not dict"))
    holders = (raw.get("holderCount") or raw.get("holders") or
               raw.get("total_holders") or raw.get("holder_count") or "N/A")
    top100 = (raw.get("top100HoldersPct") or raw.get("supplyHeldByTop100") or
              raw.get("topHoldersPct") or raw.get("top_100_pct") or "N/A")
    fresh = (raw.get("freshWalletsPct") or raw.get("supplyHeldByFreshWallets") or
             raw.get("fresh_wallets_pct") or "N/A")
    def fmt(v):
        if isinstance(v, (int, float)) and v <= 100:
            return str(round(v, 2)) + "%"
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str) and v != "N/A":
            return v if "%" in v else v + "%"
        return v
    return {
        "holders": int(holders) if isinstance(holders, (int, float)) else holders,
        "top100_pct": fmt(top100),
        "fresh_wallets_pct": fmt(fresh),
    }

def get_sheet():
    creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet("Nansen Data")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("Nansen Data", rows=1000, cols=20)
    return ws

def ensure_headers(ws):
    headers = ["Timestamp (UTC)", "Symbol", "Contract Address",
               "Holders", "Top 100 Holders %", "Fresh Wallets %"]
    if ws.row_values(1) != headers:
        ws.insert_row(headers, 1)

def run():
    log.info("Starting...")
    ws = get_sheet()
    ensure_headers(ws)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_rows = []
    for token in TOKENS:
        symbol = token["symbol"]
        address = token["address"]
        log.info("Fetching " + symbol)
        try:
            raw = fetch_nansen_data(address)
            fields = parse_fields(raw)
        except Exception as e:
            log.error("Error " + symbol + ": " + str(e))
            fields = {"holders": "ERROR", "top100_pct": "ERROR", "fresh_wallets_pct": "ERROR"}
        new_rows.append([timestamp, symbol, address,
                         fields["holders"], fields["top100_pct"], fields["fresh_wallets_pct"]])
        log.info("Done " + symbol + ": " + str(fields))
        time.sleep(2)
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    log.info("All done!")

if __name__ == "__main__":
    run()
