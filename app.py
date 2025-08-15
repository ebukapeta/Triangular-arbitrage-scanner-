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
            continue  # Skip non-spot
        if ':' in symbol or '/' not in symbol:
            continue  # Skip futures/synthetics
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
    """
    Sell 'amount_base' (base) into bids across multiple levels.
    Returns (quote_received, avg_price, filled_base, used_levels).
    """
    remaining = float(amount_base)
    quote_received = 0.0
    filled_base = 0.0
    used_levels = 0

    for price, vol_base in orderbook_bids:
        if remaining <= 0:
            break
        trade_base = min(remaining, vol_base)
        quote_received += trade_base * price
        remaining -= trade_base
        filled_base += trade_base
        used_levels += 1

    avg_price = (quote_received / filled_base) if filled_base > 0 else 0.0
    return quote_received, avg_price, filled_base, used_levels


def _fill_buy_base_with_quote(amount_quote, orderbook_asks):
    """
    Spend 'amount_quote' (quote) into asks to buy base across multiple levels.
    Returns (base_bought, avg_price, spent_quote, used_levels).
    """
    remaining_quote = float(amount_quote)
    base_bought = 0.0
    spent_quote = 0.0
    used_levels = 0

    for price, vol_base in orderbook_asks:
        if remaining_quote <= 0:
            break
        max_base_affordable = remaining_quote / price
        trade_base = min(vol_base, max_base_affordable)
        cost = trade_base * price

        base_bought += trade_base
        spent_quote += cost
        remaining_quote -= cost
        used_levels += 1

        if remaining_quote <= 1e-12:
            remaining_quote = 0.0
            break

    avg_price = (spent_quote / base_bought) if base_bought > 0 else 0.0
    return base_bought, avg_price, spent_quote, used_levels


# =========================
# Profit calculator (multi-level + auto direction)
# =========================
def calculate_profit(
    ex,
    tickers,
    ob_cache,
    cycle,
    fee,
    min_volume,
    trade_size,
    volume_filter_on,
    ob_limit
):
    """
    Multi-level orderbook simulation across the triangle.
    - Consumes multiple levels until 'trade_size' is fully filled on each leg.
    - Automatically handles A/B vs B/A listing.
    - Applies taker fee per leg.
    Returns (profit_before_percent, profit_after_percent) or (None, None) if not fillable.
    """
    amount = float(trade_size)  # amount starts in currency of cycle[0]
    initial_amount = amount
    before_fees_product = 1.0

    for i in range(3):
        from_c = cycle[i]
        to_c   = cycle[(i + 1) % 3]

        pair = get_pair(ex, from_c, to_c)
        if not pair or pair not in ex.markets:
            return None, None

        # Optional volume pre-check using tickers (cheap)
        if volume_filter_on and pair in tickers:
            if tickers[pair].get('quoteVolume', 0) < min_volume:
                return None, None

        # Fetch + cache order book (depth = ob_limit)
        if pair not in ob_cache:
            try:
                ob_cache[pair] = ex.fetch_order_book(pair, limit=ob_limit)
                time.sleep(0.05)  # small back-off; outer loop also sleeps
            except Exception:
                return None, None

        ob = ob_cache[pair]
        if not ob or not ob.get('bids') or not ob.get('asks'):
            return None, None

        base, quote = pair.split('/')

        # SELL base (you hold base) -> receive quote, use BIDS
        if base == from_c and quote == to_c:
            quote_recv, avg_px, filled_base, _levels = _fill_sell_base_for_quote(amount, ob['bids'])
            if filled_base + 1e-12 < amount:   # insufficient depth
                return None, None

            # pre-fee conversion ratio for this leg:
            leg_ratio_pre_fee = quote_recv / amount  # (quote out) / (base in)
            before_fees_product *= leg_ratio_pre_fee

            # after taker fee on proceeds:
            quote_after_fee = quote_recv * (1.0 - fee)
            amount = quote_after_fee  # now denominated in 'to_c' (quote)

        # BUY base (you hold quote) -> spend quote, receive base, use ASKS
        elif base == to_c and quote == from_c:
            base_bought, avg_px, spent_quote, _levels = _fill_buy_base_with_quote(amount, ob['asks'])
            if spent_quote + 1e-12 < amount:   # insufficient depth to spend all quote
                return None, None

            leg_ratio_pre_fee = base_bought / amount  # (base out) / (quote in)
            before_fees_product *= leg_ratio_pre_fee

            # taker fee on acquired asset:
            base_after_fee = base_bought * (1.0 - fee)
            amount = base_after_fee  # now denominated in 'to_c' (base)

        else:
            # Shouldn't happen if get_pair() succeeded
            return None, None

    profit_before = max((before_fees_product - 1.0) * 100.0, 0.0)
    profit_after  = max(((amount / initial_amount) - 1.0) * 100.0, 0.0)
    return round(profit_before, 6), round(profit_after, 6)


# =========================
# Streamlit UI
# =========================
st.title("Triangular Arbitrage Scanner — Depth-Aware")

col1, col2, col3 = st.columns(3)
with col1:
    exchange_name = st.selectbox(
        "Select Exchange",
        ["binance", "kraken", "kucoin", "bitfinex", "huobi", "gateio", "mexc", "bitget", "bybit", "bitmart"]
    )
with col2:
    base_currency = st.selectbox("Base Currency", ["USDT", "BTC", "ETH", "USD", "BNB"])
with col3:
    min_profit = st.number_input("Min Profit %", min_value=0.0, value=0.0, step=0.01)

col4, col5, col6 = st.columns(3)
with col4:
    min_volume = st.number_input("Min 24h Volume (quote units)", min_value=0, value=0, step=10000)
with col5:
    trade_size = st.number_input("Trade Size (in base currency of cycle[0])", min_value=0.0, value=200.0, step=50.0)
with col6:
    num_opp = st.selectbox("Num Opportunities", [10, 15, 20, 30])

col7, col8, col9 = st.columns(3)
with col7:
    volume_filter_on = st.checkbox("Enable Volume Filtering", value=False)
with col8:
    ob_limit = st.select_slider("Order Book Depth (levels)", options=[5, 10, 20, 40, 60, 100], value=40)
with col9:
    # keep Binance more conservative by default
    max_triangles_default = 150 if exchange_name.lower() == "binance" else 400
    max_triangles = st.number_input("Max Triangles to Scan", min_value=50, max_value=1000, value=max_triangles_default, step=50)

rate_delay = 0.3 if exchange_name.lower() == "binance" else 0.15


# =========================
# Scan button
# =========================
if st.button("Scan for Opportunities"):
    with st.spinner("Scanning exchanges for triangular opportunities..."):
        try:
            ex = getattr(ccxt, exchange_name)()
            ex.enableRateLimit = True
            ex.load_markets()

            triangles = find_triangles(ex.markets, base_currency.upper())
            raw_count = len(triangles)
            triangles = triangles[:max_triangles]
            st.write(f"🔍 Raw triangles found: {raw_count} | Scanning: {len(triangles)}")

            tickers = {}
            try:
                tickers = ex.fetch_tickers()
            except Exception:
                # Some exchanges throttle tickers; continue without tickers (volume filter may be off anyway)
                tickers = {}

            # taker fee fallback
            first_market = next((m for m in ex.markets if ex.markets[m].get('spot', False)), None)
            if first_market:
                fee = ex.markets[first_market].get('taker', 0.001)
            else:
                fee = 0.001  # fallback

            ob_cache = {}
            opps = []

            # Debug counters
            considered_cycles = 0
            skipped_missing_pair_or_ticker = 0
            skipped_volume = 0
            skipped_depth = 0
            computed_ok = 0

            for tri in triangles:
                for direction in [list(tri), list(tri)[::-1]]:
                    considered_cycles += 1

                    # Optional quick pre-check for volume/ticker presence
                    if volume_filter_on:
                        vol_fail = False
                        for i in range(3):
                            from_c = direction[i]
                            to_c = direction[(i + 1) % 3]
                            pair = get_pair(ex, from_c, to_c)
                            if not pair:
                                vol_fail = True
                                break
                            t = tickers.get(pair)
                            if (t is None) or (t.get('quoteVolume', 0) < min_volume):
                                vol_fail = True
                                break
                        if vol_fail:
                            skipped_volume += 1
                            continue

                    # Profit sim (depth-aware)
                    pb, pa = calculate_profit(
                        ex, tickers, ob_cache, direction, fee, min_volume, trade_size, volume_filter_on, ob_limit
                    )

                    if pb is None or pa is None:
                        # Determine if it was due to missing market/ticker or depth
                        # Quick heuristic: if all three pairs exist, call it depth; else missing.
                        three_pairs_ok = True
                        for i in range(3):
                            from_c = direction[i]
                            to_c = direction[(i + 1) % 3]
                            if not get_pair(ex, from_c, to_c):
                                three_pairs_ok = False
                                break
                        if three_pairs_ok:
                            skipped_depth += 1
                        else:
                            skipped_missing_pair_or_ticker += 1
                    else:
                        computed_ok += 1
                        if pa > min_profit:
                            coin_pairs = ' -> '.join(direction) + f' -> {direction[0]}'
                            opps.append({
                                'Coin Pairs': coin_pairs,
                                'Initial Profit % (before fees)': pb,
                                'Final Profit % (after fees)': pa,
                                'Fee % (total for 3 trades)': round(fee * 3 * 100, 6)
                            })

                    # Respect rate limits
                    time.sleep(rate_delay)

            # Debug summary
            st.caption("Diagnostics")
            st.write(
                f"• Considered cycle directions: {considered_cycles}  \n"
                f"• Skipped (missing pair/ticker): {skipped_missing_pair_or_ticker}  \n"
                f"• Skipped (volume pre-check): {skipped_volume}  \n"
                f"• Skipped (insufficient depth to fill trade size): {skipped_depth}  \n"
                f"• Simulated successfully: {computed_ok}"
            )

            if opps:
                opps.sort(key=lambda x: x['Final Profit % (after fees)'], reverse=True)
                top_opps = opps[:num_opp]
                st.subheader("Results")
                st.dataframe(pd.DataFrame(top_opps), use_container_width=True)
            else:
                st.info("No profitable opportunities found above the threshold.")

        except ccxt.RateLimitExceeded:
            st.error("Rate limit exceeded. Reduce 'Max Triangles', increase delay, or try a different exchange.")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
