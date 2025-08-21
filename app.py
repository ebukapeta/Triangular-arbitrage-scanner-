import streamlit as st
import ccxt
from collections import defaultdict
import pandas as pd
import time
from typing import Dict, Tuple, Optional

# =========================================================
# ---------------- Triangle Finder ------------------------
# =========================================================

def find_triangles(markets):
    graph = defaultdict(set)
    currencies = set()
    for symbol, market in markets.items():
        if not market.get('spot', False) or not market.get('active', True):
            continue
        if ':' in symbol or '/' not in symbol:
            continue
        base, quote = symbol.split('/')
        graph[base].add(quote)
        graph[quote].add(base)
        currencies.add(base)
        currencies.add(quote)
    
    triangles = set()
    for node in sorted(currencies):
        neighbors = sorted(graph[node])
        for i in range(len(neighbors)):
            for j in range(i + 1, len(neighbors)):
                n1, n2 = neighbors[i], neighbors[j]
                if n2 in graph[n1]:
                    tri = frozenset([node, n1, n2])
                    triangles.add(tri)
    
    return [list(t) for t in triangles]

def get_pair(ex, a, b):
    p1 = f"{a}/{b}"
    if p1 in ex.markets:
        return p1
    p2 = f"{b}/{a}"
    if p2 in ex.markets:
        return p2
    return None

# =========================================================
# ----------------------- Streamlit UI --------------------
# =========================================================

st.set_page_config(page_title="Triangular Arbitrage Scanner", layout="wide")

st.title("Triangular Arbitrage Scanner — Spot Market Only (Market Price)")

st.info("This scanner uses 'last' traded prices, which may show positive opportunities that disappear with bid/ask spreads. Use for screening only.")

c1, c2, c3, c4 = st.columns(4)
with c1:
    exchange_name = st.selectbox("Exchange", [
        "binance", "kucoin", "bybit", "kraken", "bitfinex", "huobi",
        "gateio", "mexc", "bitget", "bitmart"
    ])
with c2:
    min_profit = st.number_input("Highlight Profit % Threshold", min_value=0.0, value=0.0, step=0.01)
with c3:
    max_triangles = st.number_input("Max Triangles to Scan", min_value=30, max_value=5000, value=600, step=50)
with c4:
    min_volume = st.number_input("Min 24h Volume (USDT equiv.)", min_value=0, value=10000, step=1000)

show_leg_prices = st.checkbox("Show leg market prices (debug)", value=False)

scan_btn = st.button("Scan for Opportunities (Market Price)")

# =========================================================
# ---------------- Evaluation Functions -------------------
# =========================================================

def price_last(tickers: dict, symbol: str) -> Optional[float]:
    t = tickers.get(symbol)
    if not t:
        return None
    p = t.get("last")
    try:
        return float(p) if p is not None else None
    except Exception:
        return None

def conversion_rate_from_last(symbol: str, from_coin: str, to_coin: str, last_price: Optional[float]) -> Optional[float]:
    if last_price is None or last_price <= 0:
        return None
    base, quote = symbol.split("/")
    if from_coin == base and to_coin == quote:
        return last_price
    if from_coin == quote and to_coin == base:
        return 1.0 / last_price
    return None

def evaluate_triangle_market_last(ex, direction_coins, tickers, taker_fee: float, include_prices: bool, min_vol: int):
    """
    direction_coins: [A, B, C] meaning A->B, B->C, C->A
    Uses market 'last' price for all legs. Applies taker fee per leg.
    Returns dict or None.
    """
    A, B, C = direction_coins
    s1 = get_pair(ex, A, B)
    s2 = get_pair(ex, B, C)
    s3 = get_pair(ex, C, A)
    if not s1 or not s2 or not s3:
        return None

    t1 = tickers.get(s1)
    t2 = tickers.get(s2)
    t3 = tickers.get(s3)
    if not t1 or not t2 or not t3:
        return None

    # Skip stale prices (older than 1 hour)
    current_ts = int(time.time() * 1000)
    if any(t.get('timestamp', 0) < current_ts - 3600000 for t in [t1, t2, t3]):
        return {"Coin Pairs": f"{A} -> {B} -> {C} -> {A}", "Reason": "Stale price(s)"}

    # Skip low liquidity
    if any(t.get('quoteVolume', 0) < min_vol for t in [t1, t2, t3]):
        return {"Coin Pairs": f"{A} -> {B} -> {C} -> {A}", "Reason": "Low liquidity"}

    p1 = price_last(tickers, s1)
    p2 = price_last(tickers, s2)
    p3 = price_last(tickers, s3)
    if p1 is None or p2 is None or p3 is None:
        return {"Coin Pairs": f"{A} -> {B} -> {C} -> {A}", "Reason": "Missing last price(s)"}

    r1 = conversion_rate_from_last(s1, A, B, p1)
    r2 = conversion_rate_from_last(s2, B, C, p2)
    r3 = conversion_rate_from_last(s3, C, A, p3)
    if r1 is None or r2 is None or r3 is None:
        return None

    before = 1.0 * r1 * r2 * r3
    after = before * (1 - taker_fee) ** 3

    row = {
        "Coin Pairs": f"{A} -> {B} -> {C} -> {A}",
        "Profit % BEFORE Fees": round((before - 1.0) * 100.0, 6),
        "Profit % AFTER Fees": round((after - 1.0) * 100.0, 6),
        "Fee % (total)": round(taker_fee * 3 * 100.0, 6),
        "Reason": "OK"
    }
    if include_prices:
        row.update({"Leg1 last": p1, "Leg2 last": p2, "Leg3 last": p3, "s1": s1, "s2": s2, "s3": s3})
    return row

def pick_taker_fee(ex, spot_markets: dict) -> float:
    # Known defaults for common exchanges (in decimal)
    default_fees = {
        "binance": 0.001,  # 0.1%
        "kucoin": 0.001,
        "bybit": 0.001,
        "kraken": 0.0026,
        "bitfinex": 0.002,
        "huobi": 0.002,
        "gateio": 0.002,
        "mexc": 0.002,
        "bitget": 0.001,
        "bitmart": 0.0025,
    }
    
    # Try unified fees first
    fee = ex.fees.get("trading", {}).get("taker")
    if fee:
        return float(fee)
    
    # Fall back to any market taker fee
    first_symbol = next(iter(spot_markets)) if spot_markets else None
    if first_symbol:
        fee = spot_markets[first_symbol].get("taker")
        if fee:
            return float(fee)
    
    # Use exchange-specific default or global 0.1%
    return default_fees.get(ex.id.lower(), 0.001)

if scan_btn:
    with st.spinner("Scanning with market prices (last)…"):
        try:
            ex = getattr(ccxt, exchange_name)()
            ex.enableRateLimit = True
            ex.load_markets()

            # spot-only and active
            spot_markets = {s: m for s, m in ex.markets.items() if m.get("spot", False) and m.get('active', True)}

            triangles = find_triangles(spot_markets)
            total_raw = len(triangles)
            triangles = triangles[:int(max_triangles)]

            st.write(f"Found {total_raw} unique spot triangles. Scanning {len(triangles)} (market price)… 🔍")

            # fetch all tickers once (efficient, avoids rate-limit)
            tickers = ex.fetch_tickers()
            if not tickers:
                st.error("No tickers fetched—check exchange connectivity.")
                st.stop()

            taker_fee = pick_taker_fee(ex, spot_markets)

            results = []
            for tri in triangles:
                # two directions (A->B->C and A->C->B)
                seq1 = [tri[0], tri[1], tri[2]]
                seq2 = [tri[0], tri[2], tri[1]]
                for seq in (seq1, seq2):
                    row = evaluate_triangle_market_last(
                        ex, seq, tickers, taker_fee, include_prices=show_leg_prices, min_vol=min_volume
                    )
                    if row and row["Reason"] == "OK":
                        results.append(row)

            if results:
                results.sort(key=lambda x: x["Profit % AFTER Fees"], reverse=True)
                st.subheader(f"Top {min(len(results), 20)} Cycles (Market Price)")
                st.dataframe(pd.DataFrame(results[:20]), use_container_width=True)

                profitable = [
                    r for r in results
                    if (r["Reason"] == "OK") and (r["Profit % AFTER Fees"] > float(min_profit))
                ]
                if profitable:
                    st.subheader("Profitable Above Threshold")
                    st.dataframe(pd.DataFrame(profitable[:20]), use_container_width=True)
                else:
                    st.info("No profitable cycles above threshold.")

                # Download
                st.download_button(
                    "⬇ Download CSV",
                    pd.DataFrame(results).to_csv(index=False),
                    file_name=f"triangular_market_{exchange_name}.csv",
                    mime="text/csv",
                )
            else:
                st.info("No cycles produced results (missing prices or pairs).")

        except ccxt.RateLimitExceeded:
            st.error("Rate limit exceeded. (This version fetches tickers once per scan to minimize calls.)")
        except Exception as e:
            st.error(f"Error: {e}")

st.caption(
    "Using market price (ticker['last']) for all legs. Fees applied as 3× taker. "
    "Calculations are optimistic; verify with bid/ask spreads for real trades."
        )
