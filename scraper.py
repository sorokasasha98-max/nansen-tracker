import os
import json
import time
import logging
import requests
from datetime import datetime, timezone, date, timedelta
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TOKENS = [
    {"symbol": "AKE", "address": "0x2c3a8ee94ddd97244a93bc48298f97d2c412f7db"},
]

NANSEN_API_KEY = os.environ.get("NANSEN_API_KEY", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

def fetch_fresh_wallets(address):
    headers = {
        "Content-Type": "application/json",
        "apiKey": NANSEN_API_KEY,
    }
    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    payload = {
        "chain": "bnb",
        "token_address": address.lower(),
        "label_type": "fresh_wallet",
        "aggregate_by_entity": False,
        "date": {"from": yesterday, "to": today},
        "pagination": {"page": 1, "per_page": 1}
    }
    url = "https://api.nansen.ai/api/v1/tgm/holders"
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    log.info("tgm/holders fresh_wallet status: " + str(r.status_code))
    log.info("Response: " + r.text[:500])
    if r.status_code == 200:
        return r.json()
    return None

def parse_fresh(raw):
    if not raw:
        return "N/A"
    data = raw.get("data", raw)
    total = (data.get("total_holders") or
             data.get("totalHolders") or
             raw.get("total") or "N/A")
    log.info("Fresh wallets total: " + str(total))
    return total

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
    headers = ["Timestamp (UTC)", "Symbol", "Contract Address", "Fresh Wallets"]
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
            raw = fetch_fresh_wallets(address)
            fresh = parse_fresh(raw)
        except Exception as e:
            log.error("Error: " + str(e))
            fresh = "ERROR"
        new_rows.append([timestamp, symbol, address, fresh])
        log.info("Done " + symbol + ": fresh=" + str(fresh))
        time.sleep(2)
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    log.info("All done!")

if __name__ == "__main__":
    run()
