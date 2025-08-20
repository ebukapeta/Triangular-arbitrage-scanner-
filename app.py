# app.py — Part 1/2
import streamlit as st
import ccxt
import time
import json
from collections import defaultdict
from typing import Optional, Dict, Tuple

# ------------------------------
# (Optional) WebSocket manager
# kept for parity with your existing file.
# Not required for the market-price calc below,
# but left intact so your structure matches.
# ------------------------------
import threading
import asyncio
import aiohttp

class WSOrderBookManager:
    def __init__(self, depth_levels: int = 20):
        self.depth_levels = depth_levels
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.books = {}  # (exchange, symbol) -> {"bids":..., "asks":..., "ts":...}
        self.lock = threading.Lock()
        # minimal placeholders for kucoin flow (kept from your file)
        self.session = None
        self.kucoin_token = None

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
@st.cache_resource
def get_ws_manager(depth_levels=20):
    return WSOrderBookManager(depth_levels=depth_levels)

# ------------------------------
# Streamlit UI / settings
# ------------------------------
st.set_page_config(page_title="Triangular Arbitrage Scanner", layout="wide")
st.title("🔺 Triangular Arbitrage Scanner — Market Price (fixed inversion)")

# Exchange list (single-exchange triangular scanner)
EXCHANGE_OPTIONS = ["binance", "kucoin", "bybit", "gateio", "okx", "mexc"]

col1, col2, col3 = st.columns([2, 2, 2])
with col1:
    exchange_name = st.selectbox("Exchange", EXCHANGE_OPTIONS, index=0)
with col2:
    min_profit = st.number_input("Highlight profit % threshold (after fees)", min_value=0.0, value=0.5, step=0.1, format="%.3f")
with col3:
    max_triangles = st.number_input("Max triangles to scan", min_value=30, max_value=5000, value=600, step=50)

show_leg_prices = st.checkbox("Show leg market prices (debug)", value=False)
scan_btn = st.button("Scan for Opportunities (Market Price)")

# WS manager kept but not used for pricing
ws_manager = get_ws_manager(depth_levels=20)
# ------------------------------
# Helpers: symbol parsing & lookup
# ------------------------------
def normalize_symbol(symbol: str) -> str:
    """Normalize symbol string and strip chain suffix like :USDT if present."""
    if not isinstance(symbol, str):
        return symbol
    if ":" in symbol:
        # e.g., "BTC/USDT:USDT" -> "BTC/USDT"
        base, rest = symbol.split("/", 1)
        quote = rest.split(":", 1)[0]
        return f"{base}/{quote}"
    return symbol

def split_symbol(sym: str) -> Tuple[str, str]:
    s = normalize_symbol(sym)
    parts = s.split("/")
    if len(parts) >= 2:
        return parts[0], parts[1]
    return s, ""

def get_pair_for(ex, a: str, b: str) -> Optional[str]:
    """
    Return canonical market symbol present on exchange connecting a and b,
    in the form available in ex.markets keys, normalized using normalize_symbol.
    """
    # Prefer exact 'A/B' if present
    cand1 = f"{a}/{b}"
    cand2 = f"{b}/{a}"
    mk = ex.markets
    if cand1 in mk:
        return cand1
    if cand2 in mk:
        return cand2
    # sometimes markets have suffixes or different case; try normalized check
    for s in mk.keys():
        ns = normalize_symbol(s)
        if ns == cand1:
            return s
        if ns == cand2:
            return s
    return None

# ------------------------------
# Price retrieval using ticker['last'] WITH inversion handling
# ------------------------------
def price_last_from_tickers(tickers: Dict, symbol_on_exchange: str) -> Optional[float]:
    """Return ticker['last'] for the exact symbol key if available."""
    if not tickers:
        return None
    # symbol_on_exchange might be normalized or raw; try keys
    if symbol_on_exchange in tickers:
        p = tickers[symbol_on_exchange].get("last")
        try:
            return float(p) if p is not None else None
        except:
            return None
    # try normalized match
    norm = normalize_symbol(symbol_on_exchange)
    for k, t in tickers.items():
        if normalize_symbol(k) == norm:
            p = t.get("last")
            try:
                return float(p) if p is not None else None
            except:
                return None
    return None

def get_conversion_rate_for_leg(tickers: Dict, market_symbol_on_ex: str, from_coin: str, to_coin: str) -> Optional[float]:
    """
    Given a market symbol present on the exchange (market_symbol_on_ex) and desired direction
    from_coin -> to_coin, return the multiplier (quote per unit) using ticker['last'] and inverting when needed.
    If market is BASE/QUOTE and from_coin==BASE,to_coin==QUOTE => return last
    If market is BASE/QUOTE and from_coin==QUOTE,to_coin==BASE => return 1/last
    Otherwise return None.
    """
    p = price_last_from_tickers(tickers, market_symbol_on_ex)
    if p is None or p <= 0:
        return None
    base, quote = split_symbol(market_symbol_on_ex)
    # base/quote semantics: 1 BASE = p QUOTE
    if from_coin == base and to_coin == quote:
        return p
    if from_coin == quote and to_coin == base:
        return 1.0 / p
    # if market symbol doesn't match exactly direction, return None
    return None

# ------------------------------
# Triangle enumeration (spot-only)
# ------------------------------
def enumerate_triangles_for_exchange(ex) -> list:
    """
    Build unique unordered triangles of currencies (A,B,C) from exchange.markets (spot only).
    Returns list of triangles as tuples (A,B,C).
    """
    markets = ex.markets
    graph = defaultdict(set)
    currencies = set()
    for symbol, m in markets.items():
        # require spot market
        if not m.get("spot", False):
            continue
        # ignore weird symbol forms
        if "/" not in symbol:
            continue
        base, quote = split_symbol(symbol)
        graph[base].add(quote)
        graph[quote].add(base)
        currencies.add(base); currencies.add(quote)

    triangles = []
    seen = set()
    for a in sorted(currencies):
        neigh = sorted(graph[a])
        for i in range(len(neigh)):
            for j in range(i+1, len(neigh)):
                b = neigh[i]; c = neigh[j]
                # need b connected to c as well
                if c in graph[b]:
                    key = tuple(sorted([a,b,c]))
                    if key not in seen:
                        seen.add(key)
                        triangles.append((a,b,c))
    return triangles

# ------------------------------
# Evaluate oriented triangle using market 'last' and inversion handling
# ------------------------------
def evaluate_oriented_triangle(ex, tickers, oriented_tri, taker_fee=0.001, include_prices=False):
    """
    oriented_tri = (A,B,C) meaning trades:
       A -> B using market connecting A,B
       B -> C using market connecting B,C
       C -> A using market connecting C,A
    Uses tickers (fetched once) and get_pair_for/ex.markets info to find the appropriate market symbol.
    Returns dict with before/after fees profit %, return multiplier, and optional leg prices.
    """
    A,B,C = oriented_tri
    # find market symbols on exchange (could be in either orientation)
    s1 = get_pair_for(ex, A, B)
    s2 = get_pair_for(ex, B, C)
    s3 = get_pair_for(ex, C, A)
    if not (s1 and s2 and s3):
        return None

    # get conversion multipliers for each leg using last price and inversion logic
    r1 = get_conversion_rate_for_leg(tickers, s1, A, B)
    r2 = get_conversion_rate_for_leg(tickers, s2, B, C)
    r3 = get_conversion_rate_for_leg(tickers, s3, C, A)
    if r1 is None or r2 is None or r3 is None:
        return None

    before = 1.0 * r1 * r2 * r3
    after = before * (1 - taker_fee) ** 3

    row = {
        "Triangle": f"{A} → {B} → {C} → {A}",
        "s1": s1, "s2": s2, "s3": s3,
        "Before Return": before,
        "Profit % BEFORE Fees": round((before - 1.0) * 100.0, 6),
        "Profit % AFTER Fees": round((after - 1.0) * 100.0, 6),
        "Total Fee %": round(taker_fee * 3 * 100.0, 6)
    }
    if include_prices:
        # also expose the raw last prices used (for debugging)
        p1 = price_last_from_tickers(tickers, s1)
        p2 = price_last_from_tickers(tickers, s2)
        p3 = price_last_from_tickers(tickers, s3)
        row.update({"Leg1 last": p1, "Leg2 last": p2, "Leg3 last": p3})
    return row

# ------------------------------
# Choose taker fee: per-exchange fallback
# ------------------------------
def pick_taker_fee(ex):
    try:
        f = ex.fees.get("trading", {}).get("taker")
        if f:
            return float(f)
    except Exception:
        pass
    # fallback try a representative market
    for s, m in ex.markets.items():
        if m.get("spot", False):
            tak = m.get("taker")
            if tak:
                return float(tak)
    # default 0.1% if nothing found
    return 0.001

# ------------------------------
# Main scan loop — uses fetch_tickers once
# ------------------------------
def run_triangular_scan(exchange_id: str, max_triangles: int, show_prices_flag: bool, min_profit_threshold: float):
    ex_class = getattr(ccxt, exchange_id)
    ex = ex_class({"enableRateLimit": True, "timeout": 15000})
    # load markets (ensures ex.markets populated)
    ex.load_markets()

    # enumerate unique triangles
    triangles = enumerate_triangles_for_exchange(ex)
    raw_count = len(triangles)
    triangles = triangles[:max_triangles]

    # fetch tickers once
    tickers = ex.fetch_tickers()

    taker_fee = pick_taker_fee(ex)

    results = []
    # evaluate both orientations for every triangle (A,B,C and A,C,B)
    for (A,B,C) in triangles:
        for oriented in ((A,B,C), (A,C,B)):
            row = evaluate_oriented_triangle(ex, tickers, oriented, taker_fee=taker_fee, include_prices=show_prices_flag)
            if row:
                results.append(row)

    # sort by after-fee profit descending
    results.sort(key=lambda r: r["Profit % AFTER Fees"], reverse=True)
    meta = {
        "raw_triangles": raw_count,
        "scanned": len(triangles),
        "evaluated": len(results),
        "taker_fee": taker_fee
    }
    return results, meta

# ------------------------------
# UI trigger & display
# ------------------------------
if scan_btn:
    with st.spinner("Scanning triangles (market 'last' price, inversion fixed)…"):
        try:
            results, meta = run_triangular_scan(exchange_name, int(max_triangles), show_leg_prices, float(min_profit))
        except Exception as e:
            st.error(f"Scan failed: {e}")
            results, meta = [], {}

    st.subheader(f"Scan results ({exchange_name}) — all cycles (market last, inversion fixed)")

    if results:
        import pandas as pd
        df = pd.DataFrame(results)
        # Sort and number
        df = df.sort_values("Profit % AFTER Fees", ascending=False).reset_index(drop=True)
        df.index = df.index + 1

        # Top N cycles table
        st.write(f"Top {min(50, len(df))} cycles (sorted by after-fee profit):")
        st.dataframe(df.head(50), use_container_width=True)

        # Filter profitable above threshold
        prof_df = df[df["Profit % AFTER Fees"] > float(min_profit)]
        if not prof_df.empty:
            st.subheader("Profitable Above Threshold")
            st.dataframe(prof_df, use_container_width=True)
        else:
            st.info("No cycles above the profit threshold after fees.")

        st.caption(f"Raw triangles discovered: {meta.get('raw_triangles')} • Scanned: {meta.get('scanned')} • Evaluated (priced): {meta.get('evaluated')} • taker fee used: {meta.get('taker_fee')}")
        # CSV download
        st.download_button("⬇️ Download CSV", df.to_csv(index=True), file_name=f"triangular_{exchange_name}.csv", mime="text/csv")
    else:
        st.info("No triangles could be evaluated. Try another exchange or increase Max triangles.")
