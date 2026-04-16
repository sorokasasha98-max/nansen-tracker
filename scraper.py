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

FIREBASE_API_KEY = "AIzaSyDummyKeyForNansen"

TOKENS = [
    {"symbol": "AKE", "address": "0x2c3a8ee94ddd97244a93bc48298f97d2c412f7db", "chain": "bnb"},
]

def get_firebase_api_key():
    r = requests.get("https://app.nansen.ai", timeout=10)
    import re
    match = re.search(r'"apiKey":"(AIza[^"]+)"', r.text)
    if match:
        return match.group(1)
    return None

def login_nansen():
    api_key = get_firebase_api_key()
    if not api_key:
        raise Exception("Could not find Firebase API key")
    log.info("Got Firebase API key")
    
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={api_key}"
    payload = {
        "email": NANSEN_EMAIL,
        "password": NANSEN_PASSWORD,
        "returnSecureToken": True
    }
    r = requests.post(url, json=payload, timeout=20)
    log.info("Login status: " + str(r.status_code))
    if r.status_code != 200:
        raise Exception("Login failed: " + r.text[:200])
    data = r.json()
    return data["idToken"]

def fetch_gini_stats(token, address, chain):
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
        "dashboard-id": "token_god_mode",
        "widget-id": "tgm-holders-summary",
        "tab-id": "holders",
        "Referer": "https://app.nansen.ai/token-god-mode",
        "Origin": "https://app.nansen.ai",
    }
    payload = {
        "parameters": {
            "chain": chain,
            "tokenAddress": address.lower()
        }
    }
    url = "https://app.nansen.ai/api/questions/tgm-holders-gini-stats"
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    log.info("gini-stats status: " + str(r.status_code))
    if r.status_code == 200:
        return r.json()
    log.warning("Response: " + r.text[:300])
    return None

def parse_data(raw):
    if not raw:
        return {"holders": "N/A", "top100_pct": "N/A", "fresh_pct": "N/A"}
    data = raw.get("data", [{}])
    if isinstance(data, list) and len(data) > 0:
        data = data[0]
    holders = data.get("totalHolders", "N/A")
    top100  = data.get("top100HoldersBalancePercent", "N/A")
    fresh   = data.get("freshWalletBalancePercent", "N/A")
    def fmt(v):
        if isinstance(v, (int, float)):
            return str(round(v * 100, 2)) + "%"
        return v
    return {
        "holders": int(holders) if isinstance(holders, (int, float)) else holders,
        "top100_pct": fmt(top100),
        "fresh_pct": fmt(fresh),
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
    token = login_nansen()
    log.info("Logged in successfully!")
    ws = get_sheet()
    ensure_headers(ws)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    new_rows = []
    for t in TOKENS:
        symbol  = t["symbol"]
        address = t["address"]
        chain   = t["chain"]
        log.info("Fetching " + symbol)
        try:
            raw    = fetch_gini_stats(token, address, chain)
            fields = parse_data(raw)
        except Exception as e:
            log.error("Error " + symbol + ": " + str(e))
            fields = {"holders": "ERROR", "top100_pct": "ERROR", "fresh_pct": "ERROR"}
        new_rows.append([timestamp, symbol, address,
                         fields["holders"], fields["top100_pct"], fields["fresh_pct"]])
        log.info("Done " + symbol + ": " + str(fields))
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    log.info("All done!")

if __name__ == "__main__":
    run()
