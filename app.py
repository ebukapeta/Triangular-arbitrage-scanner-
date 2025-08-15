import streamlit as st
import ccxt
from collections import defaultdict
import pandas as pd
import time

# =========================
# Graph + Triangle Finder
# =========================
def find_triangles(markets, base_currency):
    graph = defaultdict(set)
    currencies = set()

    for symbol, market in markets.items():
        if not market.get('spot', False):
            continue
        if ':' in symbol or '/' not in symbol:
            continue
        base, quote = symbol.split('/')
        graph[base].add(quote)
        graph[quote].add(base)
        currencies.add(base)
        currencies.add(quote)

    triangles = []
    for node in sorted(currencies):
        neighbors = sorted(graph[node])
        for i in range(len(neighbors)):
            for j in range(i + 1, len(neighbors)):
                n1, n2 = neighbors[i], neighbors[j]
                if n2 in graph[n1]:
                    tri = tuple(sorted([node, n1, n2]))
                    if base_currency in tri and tri not in triangles:
                        triangles.append(tri)
    return triangles

# =========================
# Market helpers
# =========================
def get_pair(ex, a, b):
    pair1 = f"{a}/{b}"
    if pair1 in ex.markets:
        return pair1
    pair2 = f"{b}/{a}"
    if pair2 in ex.markets:
        return pair2
    return None

# =========================
# Multi-level fill helpers
# =========================
def _fill_sell_base_for_quote(amount_base, orderbook_bids):
    remaining = float(amount_base)
    quote_received = 0.0
    filled_base = 0.0
    for price, vol_base in orderbook_bids:
        if remaining <= 0:
            break
        trade_base = min(remaining, vol_base)
        quote_received += trade_base * price
        remaining -= trade_base
        filled_base += trade_base
    return quote_received, filled_base

def _fill_buy_base_with_quote(amount_quote, orderbook_asks):
    remaining_quote = float(amount_quote)
    base_bought = 0.0
    for price, vol_base in orderbook_asks:
        if remaining_quote <= 0:
            break
        max_base_affordable = remaining_quote / price
        trade_base = min(vol_base, max_base_affordable)
        cost = trade_base * price
        base_bought += trade_base
        remaining_quote -= cost
    return base_bought, amount_quote - remaining_quote

# =========================
# Profit calculator (depth-aware + auto direction)
# =========================
def calculate_profit(ex, tickers, ob_cache, cycle, fee, min_volume, trade_size, volume_filter_on, ob_limit):
    amount = float(trade_size)
    initial_amount = amount
    before_fees_product = 1.0

    for i in range(3):
        from_c = cycle[i]
        to_c   = cycle[(i + 1) % 3]

        pair = get_pair(ex, from_c, to_c)
        if not pair or pair not in ex.markets:
            return None, None

        if volume_filter_on and pair in tickers:
            if tickers[pair].get('quoteVolume', 0) < min_volume:
                return None, None

        if pair not in ob_cache:
            try:
                ob_cache[pair] = ex.fetch_order_book(pair, limit=ob_limit)
                time.sleep(0.05)
            except Exception:
                return None, None

        ob = ob_cache[pair]
        if not ob or not ob.get('bids') or not ob.get('asks'):
            return None, None

        base, quote = pair.split('/')

        if base == from_c and quote == to_c:
            quote_recv, filled_base = _fill_sell_base_for_quote(amount, ob['bids'])
            if filled_base + 1e-12 < amount:
                return None, None
            leg_ratio_pre_fee = quote_recv / amount
            before_fees_product *= leg_ratio_pre_fee
            amount = quote_recv * (1.0 - fee)

        elif base == to_c and quote == from_c:
            base_bought, spent_quote = _fill_buy_base_with_quote(amount, ob['asks'])
            if spent_quote + 1e-12 < amount:
                return None, None
            leg_ratio_pre_fee = base_bought / amount
            before_fees_product *= leg_ratio_pre_fee
            amount = base_bought * (1.0 - fee)

        else:
            return None, None

    profit_before = (before_fees_product - 1.0) * 100.0
    profit_after  = ((amount / initial_amount) - 1.0) * 100.0
    return round(profit_before, 6), round(profit_after, 6)

# =========================
# Streamlit UI
# =========================
st.title("Triangular Arbitrage Scanner + BTC Triangle Debug")

col1, col2, col3 = st.columns(3)
with col1:
    exchange_name = st.selectbox("Select Exchange", ["binance", "kraken", "kucoin", "bitfinex", "huobi", "gateio", "mexc", "bitget", "bybit", "bitmart"])
with col2:
    base_currency = st.selectbox("Base Currency", ["USDT", "BTC", "ETH", "USD", "BNB"])
with col3:
    min_profit = st.number_input("Min Profit %", min_value=0.0, value=0.0, step=0.01)

col4, col5, col6 = st.columns(3)
with col4:
    min_volume = st.number_input("Min 24h Volume", min_value=0, value=0, step=10000)
with col5:
    trade_size = st.number_input("Trade Size", min_value=0.0, value=200.0, step=50.0)
with col6:
    num_opp = st.selectbox("Num Opportunities", [10, 15, 20, 30])

volume_filter_on = st.checkbox("Enable Volume Filtering", value=False)
ob_limit = st.select_slider("Order Book Depth", options=[5, 10, 20, 40, 60, 100], value=40)
max_triangles_default = 150 if exchange_name.lower() == "binance" else 400
max_triangles = st.number_input("Max Triangles to Scan", min_value=50, max_value=1000, value=max_triangles_default, step=50)
rate_delay = 0.3 if exchange_name.lower() == "binance" else 0.15

# =========================
# Scan button
# =========================
if st.button("Scan for Opportunities"):
    with st.spinner("Scanning..."):
        try:
            ex = getattr(ccxt, exchange_name)()
            ex.enableRateLimit = True
            ex.load_markets()

            triangles = find_triangles(ex.markets, base_currency.upper())
            raw_count = len(triangles)
            triangles = triangles[:max_triangles]
            st.write(f"🔍 Raw triangles found: {raw_count} | Scanning: {len(triangles)}")

            # Debug: Show all BTC triangles if BTC is base coin
            if base_currency.upper() == "BTC":
                btc_triangles = [tri for tri in triangles if "BTC" in tri]
                st.subheader(f"BTC-related triangles found: {len(btc_triangles)}")
                st.write(btc_triangles)  # This will let you check if ('BTC','USDT','VET') exists

            try:
                tickers = ex.fetch_tickers()
            except Exception:
                tickers = {}

            first_market = next((m for m in ex.markets if ex.markets[m].get('spot', False)), None)
            fee = ex.markets[first_market].get('taker', 0.001) if first_market else 0.001

            ob_cache = {}
            results = []

            for tri in triangles:
                for direction in [list(tri), list(tri)[::-1]]:
                    pb, pa = calculate_profit(
                        ex, tickers, ob_cache, direction, fee, min_volume, trade_size, volume_filter_on, ob_limit
                    )
                    if pb is not None and pa is not None:
                        results.append({
                            'Coin Pairs': ' -> '.join(direction) + f' -> {direction[0]}',
                            'Initial Profit % (before fees)': pb,
                            'Final Profit % (after fees)': pa,
                            'Fee % (total)': round(fee * 3 * 100, 6)
                        })
                    time.sleep(rate_delay)

            if results:
                results.sort(key=lambda x: x['Final Profit % (after fees)'], reverse=True)
                top_all = results[:num_opp]
                st.subheader(f"Top {num_opp} Cycles by Profit")
                st.dataframe(pd.DataFrame(top_all), use_container_width=True)
            else:
                st.info("No triangles produced results.")

        except ccxt.RateLimitExceeded:
            st.error("Rate limit exceeded. Try fewer triangles or higher delay.")
        except Exception as e:
            st.error(f"Error: {e}")
