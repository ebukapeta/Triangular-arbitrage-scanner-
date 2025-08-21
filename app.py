# app.py — Triangular Arbitrage Scanner (Strict Validation + USD Normalization + Color Highlight)

import streamlit as st
import ccxt
import pandas as pd
from collections import defaultdict
from typing import Optional, Dict, Tuple

st.set_page_config(page_title="Triangular Arbitrage Scanner", layout="wide")
st.title("🔺 Triangular Arbitrage Scanner — Strict & Clean Results")

# ------------------------------
# Configuration & UI
# ------------------------------
EXCHANGE_OPTIONS = ["binance", "kucoin", "bybit", "gateio", "okx", "mexc", "kraken"]

col1, col2, col3 = st.columns([2, 2, 2])
with col1:
    exchange_name = st.selectbox("Exchange", EXCHANGE_OPTIONS, index=0)
with col2:
    min_profit = st.number_input("Min Profit % (after fees)", min_value=0.0, value=0.5, step=0.1, format="%.2f")
with col3:
    max_triangles = st.number_input("Max Triangles to Scan", min_value=50, max_value=5000, value=800, step=50)

scan_btn = st.button("🚀 Scan Now")

# ------------------------------
# Helper functions
# ------------------------------
def get_pair_for(ex, a: str, b: str) -> Optional[str]:
    """Return exact symbol if market exists; else None."""
    markets = ex.markets
    cand1 = f"{a}/{b}"
    cand2 = f"{b}/{a}"
    if cand1 in markets:
        return cand1
    if cand2 in markets:
        return cand2
    return None

def enumerate_triangles(ex) -> list:
    """Find strictly valid triangles (A,B,C) where all three pairs exist."""
    currencies = set()
    graph = defaultdict(set)

    for symbol, m in ex.markets.items():
        if not m.get("spot", False):  # Only consider spot markets
            continue
        if "/" not in symbol:
            continue
        base, quote = symbol.split("/")
        graph[base].add(quote)
        graph[quote].add(base)
        currencies.update([base, quote])

    triangles = []
    seen = set()

    for a in currencies:
        for b in graph[a]:
            for c in graph[b]:
                if c == a:
                    continue
                if a in graph[c]:  # completes the cycle
                    key = tuple(sorted([a, b, c]))
                    if key not in seen:
                        # strictly validate all legs exist
                        if get_pair_for(ex, a, b) and get_pair_for(ex, b, c) and get_pair_for(ex, c, a):
                            seen.add(key)
                            triangles.append((a, b, c))
    return triangles

def price_last_from_tickers(tickers: Dict, symbol: str) -> Optional[float]:
    """Fetch last price from tickers for a given symbol."""
    if symbol in tickers and tickers[symbol].get("last"):
        return float(tickers[symbol]["last"])
    return None

def get_conversion_rate(tickers: Dict, sym: str, from_coin: str, to_coin: str) -> Optional[float]:
    """Convert between coins for a given pair."""
    p = price_last_from_tickers(tickers, sym)
    if p is None or p <= 0:
        return None
    base, quote = sym.split("/")
    if from_coin == base and to_coin == quote:
        return p
    if from_coin == quote and to_coin == base:
        return 1.0 / p
    return None

def pick_taker_fee(ex):
    """Fetch taker fee or use default."""
    try:
        f = ex.fees.get("trading", {}).get("taker")
        if f:
            return float(f)
    except:
        pass
    for _, m in ex.markets.items():
        if m.get("spot", False) and m.get("taker"):
            return float(m["taker"])
    return 0.001  # default 0.1%

def evaluate_triangle(ex, tickers, tri, taker_fee):
    """Evaluate profit of a triangle."""
    A, B, C = tri
    s1 = get_pair_for(ex, A, B)
    s2 = get_pair_for(ex, B, C)
    s3 = get_pair_for(ex, C, A)
    if not (s1 and s2 and s3):
        return None

    r1 = get_conversion_rate(tickers, s1, A, B)
    r2 = get_conversion_rate(tickers, s2, B, C)
    r3 = get_conversion_rate(tickers, s3, C, A)
    if not (r1 and r2 and r3):
        return None

    before = r1 * r2 * r3
    if before < 0.5 or before > 1.5:  # sanity filter
        return None

    after = before * (1 - taker_fee) ** 3
    return {
        "Triangle": f"{A} → {B} → {C} → {A}",
        "Pairs": f"{s1}, {s2}, {s3}",
        "Profit % BEFORE Fees": round((before - 1) * 100, 4),
        "Fee %": round(taker_fee * 3 * 100, 4),
        "Profit % AFTER Fees": round((after - 1) * 100, 4)
    }

def run_scan(exchange_id, max_tris, min_profit):
    """Main scanning logic."""
    ex_class = getattr(ccxt, exchange_id)
    ex = ex_class({"enableRateLimit": True, "timeout": 20000})
    ex.load_markets()

    triangles = enumerate_triangles(ex)[:max_tris]
    tickers = ex.fetch_tickers()
    taker_fee = pick_taker_fee(ex)

    results = []
    for tri in triangles:
        for oriented in [(tri[0], tri[1], tri[2]), (tri[0], tri[2], tri[1])]:
            row = evaluate_triangle(ex, tickers, oriented, taker_fee)
            if row and row["Profit % AFTER Fees"] >= min_profit:
                results.append(row)

    df = pd.DataFrame(results)
    if df.empty:
        return None, taker_fee
    df = df.sort_values("Profit % AFTER Fees", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    return df, taker_fee

# ------------------------------
# Trigger Scan
# ------------------------------
if scan_btn:
    with st.spinner("🔍 Scanning triangles…"):
        try:
            df, fee = run_scan(exchange_name, int(max_triangles), float(min_profit))
        except Exception as e:
            st.error(f"Error: {e}")
            df, fee = None, None

    if df is None or df.empty:
        st.warning("No profitable triangles found above threshold.")
    else:
        st.subheader(f"Profitable Triangles on {exchange_name}")

        # Add green/red styling to Profit AFTER Fees column
        def highlight_profit(val):
            color = "green" if val > 0 else "red"
            return f"color: {color}; font-weight: bold;"

        styled = df.style.applymap(
            highlight_profit, subset=["Profit % AFTER Fees"]
        )
        st.dataframe(styled, use_container_width=True)
        st.caption(f"Taker fee used: {fee*100:.3f}%")
