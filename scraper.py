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
    {"symbol": "IRYS", "address": "0x91152b4ef635403efbae860edd0f8c321d7c035d"},
    {"symbol": "AKE",  "address": "0x2c3a8ee94ddd97244a93bc48298f97d2c412f7db"},
]

NANSEN_API_KEY = os.environ.get("NANSEN_API_KEY", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

def fetch_token_info(address):
    headers = {
        "Content-Type": "application/json",
        "apiKey": NANSEN_API_KEY,
    }
    payload = {
        "chain": "bnb",
        "token_address": address.lower(),
        "timeframe": "1d"
    }
    url = "https://api.nansen.ai/api/v1/tgm/token-information"
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    log.info("token-information status: " + str(r.status_code))
    if r.status_code == 200:
        return r.json()
    log.warning("Response: " + r.text[:300])
    return None

def parse_token_info(raw):
    if not raw:
        return {"holders": "N/A", "top100_pct": "N/A", "fresh_wallets_pct": "N/A"}
    data = raw.get("data", raw)
    spot = data.get("spot_metrics", {})
    holders = spot.get("total_holders") or data.get("total_holders") or "N/A"
    top100 = data.get("top100HoldersPct") or data.get("top_100_pct") or "N/A"
    fresh = data.get("freshWalletsPct") or data.get("fresh_wallets_pct") or "N/A"
    log.info("Parsed - holders: " + str(holders) + " top100: " + str(top100) + " fresh: " + str(fresh))
    return {
        "holders": int(holders) if isinstance(holders, (int, float)) else holders,
        "top100_pct": str(round(top100, 2)) + "%" if isinstance(top100, (int, float)) else top100,
        "fresh_wallets_pct": str(round(fresh, 2)) + "%" if isinstance(fresh, (int, float)) else fresh,
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
            raw = fetch_token_info(address)
            fields = parse_token_info(raw)
        except Exception as e:
            log.error("Error " + symbol + ": " + str(e))
            fields = {"holders": "ERROR", "top100_pct": "ERROR", "fresh_wallets_pct": "ERROR"}
        new_rows.append([timestamp, symbol, address,
                         fields["holders"], fields["top100_pct"], fields["fresh_wallets_pct"]])
        log.info("Done " + symbol + ": " + str(fields))
        time.sleep(3)
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    log.info("All done!")

if __name__ == "__main__":
    run()
