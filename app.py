import streamlit as st
import ccxt
from collections import defaultdict
import pandas as pd
import time

# ---------------------------
# Find all unique triangles
# ---------------------------
def find_triangles(markets, base_currency):
    graph = defaultdict(set)
    currencies = set()

    for symbol, market in markets.items():
        if not market.get('spot', False):
            continue  # Skip non-spot markets
        if ':' in symbol or '/' not in symbol:
            continue  # Skip futures and invalid
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


# ---------------------------
# Get trading pair between two currencies
# ---------------------------
def get_pair(ex, a, b):
    pair1 = f"{a}/{b}"
    if pair1 in ex.markets:
        return pair1
    pair2 = f"{b}/{a}"
    if pair2 in ex.markets:
        return pair2
    return None


# ---------------------------
# Calculate profit for a cycle
# ---------------------------
def calculate_profit(ex, tickers, ob_cache, cycle, fee, min_volume, trade_size, volume_filter_on):
    amount = trade_size
    initial_amount = amount
    before_fees_product = 1.0
    limit_val = 20 if ex.id in ['kucoin', 'bybit', 'bitget', 'bitmart'] else 1

    for i in range(3):
        from_c = cycle[i]
        to_c = cycle[(i + 1) % 3]

        pair = get_pair(ex, from_c, to_c)
        if not pair or pair not in tickers:
            return None, None

        # Optional volume check
        if volume_filter_on and tickers[pair].get('quoteVolume', 0) < min_volume:
            return None, None

        # Fetch order book if not cached
        if pair not in ob_cache:
            try:
                ob_cache[pair] = ex.fetch_order_book(pair, limit=limit_val)
                time.sleep(0.05)
            except Exception:
                return None, None

        ob = ob_cache[pair]
        if not ob['bids'] or not ob['asks']:
            return None, None

        bid_price, bid_vol = ob['bids'][0]
        ask_price, ask_vol = ob['asks'][0]
        base, quote = pair.split('/')

        if base == from_c and quote == to_c:  # Sell base for quote
            rate = bid_price
            needed_base = amount
            if ask_vol < needed_base:
                return None, None
        elif base == to_c and quote == from_c:  # Buy base with quote
            rate = 1 / ask_price
            needed_base = amount / ask_price
            if bid_vol < needed_base:
                return None, None
        else:
            return None, None

        before_fees_product *= rate
        amount *= rate
        amount *= (1 - fee)

    profit_before = (before_fees_product - 1) * 100 if before_fees_product > 1 else 0
    profit_after = (amount / initial_amount - 1) * 100 if amount > initial_amount else 0
    return profit_before, profit_after


# ---------------------------
# Streamlit UI
# ---------------------------
st.title("Triangular Arbitrage Scanner (Debug Mode)")

col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    exchange_name = st.selectbox("Select Exchange", [
        "binance", "kraken", "kucoin", "bitfinex", "huobi", "gateio", "mexc", "bitget", "bybit", "bitmart"
    ])
with col2:
    base_currency = st.selectbox("Base Currency", ["USDT", "BTC", "ETH", "USD", "BNB"])
with col3:
    min_profit = st.number_input("Min Profit %", min_value=0.0, value=0.0, step=0.05)
with col4:
    min_volume = st.number_input("Min 24h Volume", min_value=0, value=100000, step=10000)
with col5:
    trade_size = st.number_input("Trade Size", min_value=0, value=1000, step=100)
with col6:
    num_opp = st.selectbox("Num Opportunities", [10, 15, 20])

volume_filter_on = st.checkbox("Enable Volume Filtering", value=True)

# ---------------------------
# Scan button
# ---------------------------
if st.button("Scan for Opportunities"):
    with st.spinner("Scanning exchanges for triangular opportunities..."):
        try:
            ex = getattr(ccxt, exchange_name)()
            ex.enableRateLimit = True
            ex.load_markets()

            triangles = find_triangles(ex.markets, base_currency.upper())
            st.write(f"🔍 Raw triangles found: {len(triangles)}")

            # Limit triangles for Binance
            max_triangles = 150 if exchange_name.lower() == "binance" else 400
            triangles = triangles[:max_triangles]

            tickers = ex.fetch_tickers()

            first_market = next((m for m in ex.markets if ex.markets[m].get('spot', False)), None)
            if first_market:
                fee = ex.markets[first_market].get('taker', 0.001)
            else:
                raise ValueError("No spot markets found on this exchange.")

            ob_cache = {}
            opps = []
            passed_volume_check = 0
            filtered_out_samples = []

            for tri in triangles:
                for direction in [list(tri), list(tri)[::-1]]:
                    if volume_filter_on:
                        # Pre-check volumes before full calculation
                        vol_fail = False
                        for i in range(3):
                            from_c = direction[i]
                            to_c = direction[(i + 1) % 3]
                            pair = get_pair(ex, from_c, to_c)
                            if not pair or pair not in tickers:
                                vol_fail = True
                                break
                            if tickers[pair].get('quoteVolume', 0) < min_volume:
                                vol_fail = True
                                break
                        if vol_fail:
                            if len(filtered_out_samples) < 5:
                                filtered_out_samples.append(direction)
                            continue
                        passed_volume_check += 1

                    profit_before, profit_after = calculate_profit(
                        ex, tickers, ob_cache, direction, fee, min_volume, trade_size, volume_filter_on
                    )

                    if profit_after is not None and profit_after > min_profit:
                        coin_pairs = ' -> '.join(direction) + f' -> {direction[0]}'
                        opps.append({
                            'Coin Pairs': coin_pairs,
                            'Initial Profit % (before fees)': round(profit_before, 4),
                            'Final Profit % (after fees)': round(profit_after, 4),
                            'Fee % (total for 3 trades)': round(fee * 3 * 100, 4)
                        })

                    time.sleep(0.3 if exchange_name.lower() == "binance" else 0.15)

            if volume_filter_on:
                st.write(f"📊 Triangles passing volume check: {passed_volume_check}")
                st.write(f"🚫 Sample filtered triangles (due to volume): {filtered_out_samples}")
            else:
                st.write("⚠ Volume filtering disabled — results may include low-liquidity pairs.")

            if opps:
                opps.sort(key=lambda x: x['Final Profit % (after fees)'], reverse=True)
                top_opps = opps[:num_opp]
                st.subheader("Results")
                st.dataframe(pd.DataFrame(top_opps), use_container_width=True)
            else:
                st.info("No profitable opportunities found above the threshold.")

        except ccxt.RateLimitExceeded:
            st.error("Rate limit exceeded. Try scanning less frequently or with fewer triangles.")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}. Try another exchange or check your connection.")
