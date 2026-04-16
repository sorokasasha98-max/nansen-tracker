import os
import json
import logging
import requests
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

NANSEN_EMAIL     = os.environ.get("NANSEN_EMAIL", "")
NANSEN_PASSWORD  = os.environ.get("NANSEN_PASSWORD", "")
SPREADSHEET_ID   = os.environ.get("SPREADSHEET_ID")
PRIVY_APP_ID     = os.environ.get("PRIVY_APP_ID", "66eb5c3e-f6cf-44f1-b32c-7902c7c103a6")

TOKENS = [
    {"symbol": "AKE", "address": "0x2c3a8ee94ddd97244a93bc48298f97d2c412f7db", "chain": "bnb"},
]


def login_privy():
    # Шаг 1: инициируем логин через Privy
    headers = {
        "Content-Type": "application/json",
        "privy-app-id": PRIVY_APP_ID,
        "Origin": "https://app.nansen.ai",
        "Referer": "https://app.nansen.ai/",
    }

    # Инициируем OTP на email
    r = requests.post(
        "https://auth.privy.io/api/v1/passwordless/init",
        headers=headers,
        json={"email": NANSEN_EMAIL},
        timeout=20
    )
    log.info(f"Privy init status: {r.status_code} {r.text[:200]}")

    if r.status_code not in (200, 201):
        # Попробуем логин через email+password
        return login_privy_password(headers)

    raise Exception("Privy OTP flow not supported in automation - need password flow")


def login_privy_password(headers):
    # Логин через email + password
    r = requests.post(
        "https://auth.privy.io/api/v1/siwe/init",
        headers=headers,
        json={
            "email": NANSEN_EMAIL,
            "password": NANSEN_PASSWORD,
        },
        timeout=20
    )
    log.info(f"Privy password init: {r.status_code} {r.text[:300]}")

    # Попробуем прямой логин
    r2 = requests.post(
        "https://auth.privy.io/api/v1/sessions",
        headers=headers,
        json={
            "email": NANSEN_EMAIL,
            "password": NANSEN_PASSWORD,
        },
        timeout=20
    )
    log.info(f"Privy sessions: {r2.status_code} {r2.text[:300]}")

    if r2.status_code in (200, 201):
        data = r2.json()
        token = data.get("token") or data.get("access_token") or data.get("privy_access_token")
        if token:
            return get_nansen_token(token)

    raise Exception(f"Privy login failed: {r2.text[:300]}")


def get_nansen_token(privy_token):
    # Обмениваем Privy токен на Nansen Bearer токен
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://app.nansen.ai",
        "Referer": "https://app.nansen.ai/",
        "Authorization": f"Bearer {privy_token}",
    }
    r = requests.post(
        "https://app.nansen.ai/api/auth/login",
        headers=headers,
        json={"privyToken": privy_token},
        timeout=20
    )
    log.info(f"Nansen auth: {r.status_code} {r.text[:300]}")

    if r.status_code in (200, 201):
        data = r.json()
        log.info(f"Nansen auth response keys: {list(data.keys())}")
        token = data.get("token") or data.get("accessToken") or data.get("idToken")
        if token:
            return token

    raise Exception(f"Nansen login failed: {r.text[:300]}")


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
    token = login_privy()
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
