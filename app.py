import streamlit as st
import ccxt
from collections import defaultdict
import pandas as pd
import time

# Function to find all unique triangles (3-currency cycles), filtered for spot markets and base currency
def find_triangles(markets, ex, base_currency):
    graph = defaultdict(set)
    currencies = set()
    for symbol, market in markets.items():
        if not market.get('spot', False): continue  # Skip non-spot markets
        if ':' in symbol or '/' not in symbol: continue  # Skip futures and invalid
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
                n1 = neighbors[i]
                n2 = neighbors[j]
                if n2 in graph[n1]:
                    tri = tuple(sorted([node, n1, n2]))
                    if base_currency in tri and tri not in triangles:
                        triangles.append(tri)
    return triangles

# Function to get the trading pair between two currencies
def get_pair(ex, a, b):
    pair1 = f"{a}/{b}"
    if pair1 in ex.markets:
        return pair1
    pair2 = f"{b}/{a}"
    if pair2 in ex.markets:
        return pair2
    return None

# Function to calculate profits for a cycle (before/after fees), with volume and liquidity checks
def calculate_profit(ex, tickers, ob_cache, cycle, fee, min_volume, trade_size):
    amount = trade_size
    initial_amount = amount
    before_fees_product = 1.0
    limit_val = 20 if ex.id in ['kucoin', 'bybit', 'bitget', 'bitmart'] else 1  # Adjust for exchanges requiring higher limits
    for i in range(3):
        from_c = cycle[i]
        to_c = cycle[(i + 1) % 3]
        pair = get_pair(ex, from_c, to_c)
        if not pair or pair not in tickers:
            return None, None
        # Volume check
        if tickers[pair]['quoteVolume'] < min_volume:
            return None, None
        # Fetch order book if not cached
        if pair not in ob_cache:
            try:
                ob_cache[pair] = ex.fetch_order_book(pair, limit=limit_val)
                time.sleep(0.1)  # Increased delay to avoid rate limits
            except Exception:
                return None, None
        ob = ob_cache[pair]
        if not ob['bids'] or not ob['asks']:
            return None, None
        bid_price, bid_vol = ob['bids'][0]
        ask_price, ask_vol = ob['asks'][0]
        base, quote = pair.split('/')
        if base == to_c and quote == from_c:  # Buy base with quote (use ask)
            rate = 1 / ask_price
            needed_base = amount / ask_price  # Amount of base to buy
            if ask_vol < needed_base:
                return None, None  # Insufficient liquidity
        elif base == from_c and quote == to_c:  # Sell base for quote (use bid)
            rate = bid_price
            needed_base = amount  # Amount of base to sell
            if bid_vol < needed_base:
                return None, None  # Insufficient liquidity
        else:
            return None, None
        before_fees_product *= rate
        amount *= rate
        amount *= (1 - fee)
    profit_before = (before_fees_product - 1) * 100 if before_fees_product > 1 else 0
    profit_after = (amount / initial_amount - 1) * 100 if amount > initial_amount else 0
    return profit_before, profit_after

# Streamlit app
st.title("Triangular Arbitrage Scanner")

# UI inputs in a row (expanded for base currency)
col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    exchange_name = st.selectbox("Select Exchange", ["binance", "kraken", "kucoin", "bitfinex", "huobi", "gateio", "mexc", "bitget", "bybit", "bitmart"])
with col2:
    base_currency = st.selectbox("Base Currency", ["USDT", "BTC", "ETH", "USD", "BNB"])
with col3:
    min_profit = st.number_input("Min Profit %", min_value=0.0, value=0.1, step=0.05)
with col4:
    min_volume = st.number_input("Min 24h Volume", min_value=0, value=100000, step=10000)
with col5:
    trade_size = st.number_input("Trade Size", min_value=0, value=1000, step=100)
with col6:
    num_opp = st.selectbox("Num Opportunities", [10, 15, 20])

# Scan button
if st.button("Scan for Opportunities"):
    with st.spinner("Scanning exchanges for triangular opportunities..."):
        try:
            ex = getattr(ccxt, exchange_name)()
            ex.load_markets()
            triangles = find_triangles(ex.markets, ex, base_currency.upper())
            tickers = ex.fetch_tickers()
            
            # Get taker fee (fallback to 0.1% if not available)
            first_market = next((m for m in ex.markets if ex.markets[m].get('spot', False)), None)
            if first_market:
                fee = ex.markets[first_market].get('taker', 0.001)
            else:
                raise ValueError("No spot markets found on this exchange.")
            
            ob_cache = {}  # Cache for order books
            opps = []
            for tri in triangles[:500]:  # Limit to first 500 triangles to avoid rate limits
                # Check both directions
                for direction in [list(tri), list(tri)[::-1]]:
                    profit_before, profit_after = calculate_profit(ex, tickers, ob_cache, direction, fee, min_volume, trade_size)
                    if profit_after is not None and profit_after > min_profit:
                        coin_pairs = ' -> '.join(direction) + f' -> {direction[0]}'  # Show cycle
                        opps.append({
                            'Coin Pairs': coin_pairs,
                            'Initial Profit % (before fees)': round(profit_before, 4),
                            'Final Profit % (after fees)': round(profit_after, 4),
                            'Fee % (total for 3 trades)': round(fee * 3 * 100, 4)
                        })
            
            if opps:
                # Sort by final profit descending and take top N
                opps.sort(key=lambda x: x['Final Profit % (after fees)'], reverse=True)
                top_opps = opps[:num_opp]
                st.subheader("Results")
                st.dataframe(pd.DataFrame(top_opps), use_container_width=True)
            else:
                st.info("No profitable opportunities found above the threshold.")
        except ccxt.RateLimitExceeded as e:
            st.error("Rate limit exceeded. Try scanning less frequently or with fewer triangles.")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}. Try another exchange or check your connection.")
