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
    {"symbol": "IRYS",  "address": "0x50f41F589aFACa2EF41FDF590FE7b90cD26DEe64"},
    {"symbol": "LYN",   "address": "0x302DFaF2CDbE51a18d97186A7384e87CF599877D"},
    {"symbol": "BARD",  "address": "0xf0DB65D17e30a966C2ae6A21f6BBA71cea6e9754"},
    {"symbol": "AKE",   "address": "0x2c3a8Ee94dDD97244a93Bc48298f97d2C412F7Db"},
    {"symbol": "ZAMA",  "address": "0xa12cc123ba206d4031d1c7f6223d1c2ec249f4f3"},
    {"symbol": "STBL",  "address": "0x8dedf84656fa932157e27c060d8613824e7979e3"},
    {"symbol": "PLAY",  "address": "0x853a7c99227499dba9db8c3a02aa691afdebf841"},
    {"symbol": "CLO",   "address
