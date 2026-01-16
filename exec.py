#!/usr/bin/env python3
"""
Exec: Remote Signal Aggregator & Execution Engine.
Fetches signals from 'http://localhost:8080' (scheduler.py)
"""

import os
import sys
import time
import re
import logging
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple

# --- Local Imports ---
try:
    from kraken_futures import KrakenFuturesApi
except ImportError as e:
    print(f"CRITICAL: Import failed: {e}. Ensure 'kraken_futures.py' is in the directory.")
    sys.exit(1)

# --- Configuration ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# API Keys
KF_KEY = os.getenv("KRAKEN_FUTURES_KEY")
KF_SECRET = os.getenv("KRAKEN_FUTURES_SECRET")

# Global Settings
LEVERAGE = 70.0
SIGNAL_FEED_URL = "http://localhost:8080" 

# Asset Mapping (Feed Ticker -> Kraken Futures Perpetual)
# Updated to match scheduler.py tickers ("BTC", "ETH")
SYMBOL_MAP = {
    # --- Majors ---
    "BTC": "ff_xbtusd_260327",
    "ETH": "pf_ethusd",
    "SOL": "pf_solusd",
    "BNB": "pf_bnbusd",
    "XRP": "pf_xrpusd",
    "ADA": "pf_adausd",
    
    # --- Alts ---
    "DOGE": "pf_dogeusd",
    "AVAX": "pf_avaxusd",
    "DOT": "pf_dotusd",
    "LINK": "pf_linkusd",
    "TRX": "pf_trxusd",
    "BCH": "pf_bchusd",
    "XLM": "pf_xlmusd",
    "LTC": "pf_ltcusd",
    "SUI": "pf_suiusd",
    "HBAR": "pf_hbarusd",
    "SHIB": "pf_shibusd", 
    "TON": "pf_tonusd",
    "UNI": "pf_uniusd",
    "ZEC": "pf_zecusd",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.FileHandler("exec.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Exec")

# --- Signal Fetcher ---

class SignalFetcher:
    def __init__(self, url):
        self.url = url
        # Regex to capture: "BTC: 2 ..."
        self.line_pattern = re.compile(r"^([A-Z]+):\s*(-?\d+)")

    def fetch_signals(self) -> Tuple[Dict[str, int], int]:
        """
        Parses text output from scheduler.py.
        Returns:
            - asset_votes: { "BTC": 2, "ETH": -1 }
            - ticker_count: The number of active tickers (e.g., 3 for BTC, ETH, SOL)
        """
        try:
            logger.info(f"Fetching signals from {self.url}...")
            resp = requests.get(self.url, timeout=5)
            resp.raise_for_status()
            text_lines = resp.text.splitlines()

            asset_votes = {}
            
            for line in text_lines:
                # Line format expected: "BTC: 2 [Components: 1, 1]"
                match = self.line_pattern.search(line.strip())
                if match:
                    ticker = match.group(1)
                    score = int(match.group(2))

                    if ticker in SYMBOL_MAP:
                        asset_votes[ticker] = score
            
            # Count the number of tickers found (e.g., BTC, ETH, SOL = 3)
            ticker_count = len(asset_votes)
            
            logger.info(f"Parsed {ticker_count} tickers from feed.")
            return asset_votes, ticker_count

        except Exception as e:
            logger.error(f"Failed to fetch signals: {e}")
            return {}, 0

# --- Main Exec Engine ---

class Exec:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.fetcher = SignalFetcher(SIGNAL_FEED_URL)
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.instrument_specs = {}

    def initialize(self):
        logger.info("Initializing Exec Engine...")
        self._fetch_instrument_specs()
        
        # Check Connection
        try:
            acc = self.kf.get_accounts()
            if "error" in acc:
                logger.error(f"API Error: {acc}")
            else:
                logger.info("API Connection Successful.")
        except Exception as e:
            logger.error(f"API Connection Failed: {e}")

    def _fetch_instrument_specs(self):
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    sym = inst["symbol"].lower()
                    tick_size = float(inst.get("tickSize", 0.1))
                    precision = inst.get("contractValueTradePrecision")
                    size_step = 10 ** (-int(precision)) if precision is not None else 1.0
                    
                    self.instrument_specs[sym] = {
                        "sizeStep": size_step,
                        "tickSize": tick_size,
                        "contractSize": float(inst.get("contractSize", 1.0))
                    }
        except Exception as e:
            logger.error(f"Error fetching specs: {e}")

    def _round_to_step(self, value: float, step: float) -> float:
        if step == 0: return value
        rounded = round(value / step) * step
        if isinstance(step, float) and "." in str(step):
            decimals = len(str(step).split(".")[1])
            rounded = round(rounded, decimals)
        elif isinstance(step, int) or step.is_integer():
            rounded = int(rounded)
        return rounded

    def run(self):
        logger.info("Exec Engine started. Syncing with 15m intervals...")
        while True:
            now = datetime.now(timezone.utc)
            
            # Trigger every 15 minutes at second 30
            if now.minute % 15 == 0 and 30 <= now.second < 35:
                logger.info(f"--- Trigger: {now.strftime('%H:%M:%S')} ---")
                self._process_signals()
                time.sleep(50) 
            
            time.sleep(1) 

    def _process_signals(self):
        # 1. Fetch Signals from Scheduler
        asset_votes, active_strategies = self.fetcher.fetch_signals()
        
        if active_strategies == 0:
            logger.warning("No active strategies found. Skipping execution.")
            return

        # 2. Get Account Equity
        try:
            acc = self.kf.get_accounts()
            # Handle Flex vs Single-Collateral response structures
            if "flex" in acc.get("accounts", {}):
                equity = float(acc["accounts"]["flex"].get("marginEquity", 0))
            elif "accounts" in acc:
                first_acc = list(acc["accounts"].values())[0]
                equity = float(first_acc.get("marginEquity", 0))
            else:
                equity = 0
                
            if equity <= 0:
                logger.error("Equity <= 0. Aborting.")
                return
        except Exception as e:
            logger.error(f"Account fetch failed: {e}")
            return

        # 3. Execution Loop
        logger.info(f"Equity: ${equity:.2f} | Active Strategies (Count): {active_strategies}")
        
        # Execution Settings
        exec_duration = 60
        exec_interval = 5
        start_offset_bp = 0 
        step_bp = 1.0 

        for asset, signal_sum in asset_votes.items():
            # FORMULA: (Leverage * SignalSum * MarginEquity) / ActiveStrategies
            # ActiveStrategies = The count of tickers (e.g. 3)
            
            target_usd = (LEVERAGE * signal_sum * equity) / active_strategies
            
            logger.info(f"[{asset}] SignalSum: {signal_sum} -> Target Alloc: ${target_usd:.2f}")
            
            self.executor.submit(
                self._execute_single_asset_logic, 
                asset, 
                target_usd,
                exec_duration, 
                exec_interval, 
                start_offset_bp, 
                step_bp
            )

    def _execute_single_asset_logic(self, ticker: str, net_target_usd: float, 
                                    duration: int, interval: int, start_bp: float, step_bp: float):
        kf_symbol = SYMBOL_MAP.get(ticker)
        if not kf_symbol: return

        try:
            # Get Current Position
            open_pos = self.kf.get_open_positions()
            current_pos_size = 0.0
            if "openPositions" in open_pos:
                for p in open_pos["openPositions"]:
                    if p["symbol"].lower() == kf_symbol.lower():
                        size = float(p["size"])
                        if p["side"] == "short": size = -size
                        current_pos_size = size
                        break
            
            # Get Mark Price
            tickers = self.kf.get_tickers()
            mark_price = 0.0
            for t in tickers.get("tickers", []):
                if t["symbol"].lower() == kf_symbol.lower():
                    mark_price = float(t["markPrice"])
                    break
            
            if mark_price == 0: return
            
            # Calculate Delta
            target_contracts = net_target_usd / mark_price
            delta = target_contracts - current_pos_size
            
            specs = self.instrument_specs.get(kf_symbol.lower())
            size_increment = specs['sizeStep'] if specs else 0.001
            check_qty = self._round_to_step(abs(delta), size_increment)

            if check_qty < size_increment: 
                return

            logger.info(f"[{kf_symbol}] Executing Delta: {delta:.4f} (Current: {current_pos_size:.4f} -> Target: {target_contracts:.4f})")

            self._run_maker_loop(kf_symbol, delta, mark_price, duration, interval, start_bp, step_bp)

        except Exception as e:
            logger.error(f"[{kf_symbol}] Exec Error: {e}")

    def _run_maker_loop(self, symbol: str, quantity: float, initial_mark: float, 
                        max_duration: int, interval: int, start_offset_bp: float, step_bp: float):
        side = "buy" if quantity > 0 else "sell"
        abs_qty = abs(quantity)
        
        specs = self.instrument_specs.get(symbol.lower())
        size_inc = specs['sizeStep'] if specs else 0.001
        price_inc = specs['tickSize'] if specs else 0.01

        steps = max_duration // interval
        order_id = None
        
        for i in range(steps + 1):
            try:
                tickers = self.kf.get_tickers()
                curr_mark = 0.0
                for t in tickers.get("tickers", []):
                    if t["symbol"].lower() == symbol.lower():
                        curr_mark = float(t["markPrice"])
                        break
                if curr_mark == 0: curr_mark = initial_mark
                
                current_aggression_bp = start_offset_bp + (i * step_bp)
                pct_change = current_aggression_bp * 0.0001
                
                if side == "buy":
                    final_limit = curr_mark * (1 + pct_change)
                else:
                    final_limit = curr_mark * (1 - pct_change)

                final_limit = self._round_to_step(final_limit, price_inc)
                final_size = self._round_to_step(abs_qty, size_inc)
                
                if order_id is None:
                    resp = self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol, "side": side,
                        "size": final_size, "limitPrice": final_limit
                    })
                    if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                         order_id = resp["sendStatus"]["order_id"]
                         logger.info(f"[{symbol}] Order Placed @ {final_limit} ({current_aggression_bp}bp)")
                    else:
                        logger.warning(f"[{symbol}] Order Failed: {resp}")
                else:
                    self.kf.edit_order({
                        "orderId": order_id, "limitPrice": final_limit,
                        "size": final_size, "symbol": symbol 
                    })
                    logger.info(f"[{symbol}] Adjusted @ {final_limit} ({current_aggression_bp}bp)")
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"[{symbol}] Maker Loop Error: {e}")
                time.sleep(1) 
        
        if order_id:
            try:
                self.kf.cancel_order({"order_id": order_id, "symbol": symbol})
            except: pass

if __name__ == "__main__":
    bot = Exec()
    bot.initialize()
    bot.run()
