# app.py — Part 1/2
import streamlit as st
import ccxt
from collections import defaultdict
import pandas as pd
import time
import threading
import asyncio
import aiohttp
import json
from typing import Dict, Tuple, Optional

# =========================================================
# ---------------- WebSocket Order Book Manager -----------
# (Kept for parity with your existing code; not needed when
#  using market price/last for profit calc.)
# =========================================================

class WSOrderBookManager:
    """
    Live order book cache for Binance, KuCoin, Bybit via native WebSockets.
    For other exchanges, we fall back to REST. (Not used by the
    market-price scanner, but retained to match your file.)
    """
    def __init__(self, depth_levels: int = 20):
        self.depth_levels = depth_levels
        self.loop = asyncio.new_event_loop()
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

        self.books: Dict[Tuple[str, str], Dict] = {}
        self.subscribed: Dict[Tuple[str, str], bool] = {}
        self.lock = threading.Lock()

        # KuCoin token
        self.kucoin_endpoint = None
        self.kucoin_token = None
        self.kucoin_token_expiry = 0

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._ensure_session())
        self.loop.run_forever()

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    def ensure_subscription(self, exchange: str, pair: str):
        key = (exchange.lower(), pair)
        if self.subscribed.get(key):
            return
        self.subscribed[key] = True

        if exchange.lower() == "binance":
            asyncio.run_coroutine_threadsafe(
                self._binance_subscribe(pair), self.loop
            )
        elif exchange.lower() == "kucoin":
            asyncio.run_coroutine_threadsafe(
                self._kucoin_subscribe(pair), self.loop
            )
        elif exchange.lower() == "bybit":
            asyncio.run_coroutine_threadsafe(
                self._bybit_subscribe(pair), self.loop
            )

    def get_book(self, exchange: str, pair: str) -> Optional[Dict]:
        with self.lock:
            return self.books.get((exchange.lower(), pair))

    # ---------------- Binance ----------------
    async def _binance_subscribe(self, pair: str):
        await self._ensure_session()
        symbol = pair.replace("/", "").lower()
        depth_tag = f"{symbol}@depth{self.depth_levels}@100ms"
        url = "wss://stream.binance.com:9443/ws"

        async def listen():
            try:
                async with self.session.ws_connect(url, heartbeat=30) as ws:
                    sub_msg = {
                        "method": "SUBSCRIBE",
                        "params": [depth_tag],
                        "id": int(time.time()*1000) % 10_000_000
                    }
                    await ws.send_json(sub_msg)
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if 'b' in data and 'a' in data:
                                bids = [(float(p), float(q)) for p, q in data.get('b', [])][:self.depth_levels]
                                asks = [(float(p), float(q)) for p, q in data.get('a', [])][:self.depth_levels]
                                with self.lock:
                                    self.books[("binance", pair)] = {"bids": bids, "asks": asks, "ts": int(time.time()*1000)}
            except Exception:
                await asyncio.sleep(1.5)
                asyncio.create_task(listen())
        asyncio.create_task(listen())

    # ---------------- KuCoin ----------------
    async def _kucoin_public_token(self):
        await self._ensure_session()
        try:
            async with self.session.post("https://api.kucoin.com/api/v1/bullet-public") as resp:
                j = await resp.json()
                token = j["data"]["token"]
                endpoint = j["data"]["instanceServers"][0]["endpoint"]
                pingInterval = j["data"]["instanceServers"][0]["pingInterval"]
                expireTime = j["data"]["instanceServers"][0]["expireTime"]
                self.kucoin_endpoint = endpoint
                self.kucoin_token = token
                self.kucoin_token_expiry = int(time.time()*1000) + (expireTime or 60_000)
                return endpoint, token, pingInterval
        except Exception:
            return None, None, None

    async def _kucoin_subscribe(self, pair: str):
        await self._ensure_session()
        endpoint, token, ping_ms = await self._kucoin_public_token()
        if not endpoint or not token:
            await asyncio.sleep(2)
            asyncio.create_task(self._kucoin_subscribe(pair))
            return

        symbol = pair.replace("/", "-").upper()
        url = f"{endpoint}?token={token}"

        async def listen():
            try:
                async with self.session.ws_connect(url, heartbeat=max(10, (ping_ms or 20000)//1000)) as ws:
                    depth_ch = "level2Depth5" if self.depth_levels <= 5 else "level2Depth20"
                    sub = {
                        "id": str(int(time.time()*1000)),
                        "type": "subscribe",
                        "topic": f"/market/{depth_ch}:{symbol}",
                        "privateChannel": False,
                        "response": True
                    }
                    await ws.send_json(sub)

                    async def pinger():
                        while True:
                            try:
                                await ws.send_json({"id": str(int(time.time()*1000)), "type": "ping"})
                            except Exception:
                                break
                            await asyncio.sleep(max(5, (ping_ms or 20000)/1000.0 - 2))
                    asyncio.create_task(pinger())

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get("type") == "message" and "data" in data:
                                d = data["data"]
                                bids = [(float(p), float(q)) for p, q in d.get("bids", [])][:self.depth_levels]
                                asks = [(float(p), float(q)) for p, q in d.get("asks", [])][:self.depth_levels]
                                with self.lock:
                                    self.books[("kucoin", pair)] = {"bids": bids, "asks": asks, "ts": d.get("time", int(time.time()*1000))}
            except Exception:
                await asyncio.sleep(1.5)
                asyncio.create_task(listen())
        asyncio.create_task(listen())

    # ---------------- Bybit ----------------
    async def _bybit_subscribe(self, pair: str):
        await self._ensure_session()
        url = "wss://stream.bybit.com/v5/public/spot"
        symbol = pair.replace("/", "").upper()

        async def listen():
            try:
                async with self.session.ws_connect(url, heartbeat=30) as ws:
                    sub = {"op": "subscribe", "args": [f"orderbook.50.{symbol}"]}
                    await ws.send_json(sub)
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get("topic", "").startswith("orderbook") and "data" in data:
                                d = data["data"]
                                bids = [(float(p), float(q)) for p, q in d.get("b", [])][:self.depth_levels]
                                asks = [(float(p), float(q)) for p, q in d.get("a", [])][:self.depth_levels]
                                if bids or asks:
                                    with self.lock:
                                        self.books[("bybit", pair)] = {"bids": bids, "asks": asks, "ts": int(time.time()*1000)}
            except Exception:
                await asyncio.sleep(1.5)
                asyncio.create_task(listen())
        asyncio.create_task(listen())

# Cache resource for WS manager (not used by market-price calc, kept for parity)
@st.cache_resource
def get_ws_manager(depth_levels: int):
    return WSOrderBookManager(depth_levels=depth_levels)

# =========================================================
# ---------------- Triangle Finder ------------------------
# =========================================================

def split_symbol(sym: str):
    # Handle symbols like 'BTC/USDT:USDT' → ('BTC','USDT')
    parts = sym.split("/")
    base = parts[0]
    quote = parts[1].split(":")[0] if len(parts) > 1 else ""
    return base, quote

def find_triangles(markets):
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
                    if tri not in triangles:
                        triangles.append(tri)
    return triangles

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

c1, c2, c3 = st.columns(3)
with c1:
    exchange_name = st.selectbox("Exchange", [
        "binance", "kucoin", "bybit", "kraken", "bitfinex", "huobi",
        "gateio", "mexc", "bitget", "bitmart"
    ])
with c2:
    min_profit = st.number_input("Highlight Profit % Threshold", min_value=0.0, value=0.0, step=0.01)
with c3:
    max_triangles = st.number_input("Max Triangles to Scan", min_value=30, max_value=5000, value=600, step=50)

# (WS manager kept for parity; not used by market-price calc)
ws_manager = get_ws_manager(depth_levels=20)

show_leg_prices = st.checkbox("Show leg market prices (debug)", value=False)
scan_btn = st.button("Scan for Opportunities (Market Price)")
# app.py — Part 2/2

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
    """
    For market symbol BASE/QUOTE with last price P (= QUOTE per 1 BASE):
      - from BASE to QUOTE: rate = P
      - from QUOTE to BASE: rate = 1/P
    """
    if last_price is None or last_price <= 0:
        return None
    base, quote = symbol.split("/")
    # handle symbols like 'AAA/BBB:BBB'
    if ":" in quote:
        quote = quote.split(":")[0]
    if from_coin == base and to_coin == quote:
        return last_price
    if from_coin == quote and to_coin == base:
        return 1.0 / last_price
    return None

def evaluate_triangle_market_last(ex, direction_coins, tickers, taker_fee: float, include_prices: bool):
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

    p1 = price_last(tickers, s1)
    p2 = price_last(tickers, s2)
    p3 = price_last(tickers, s3)
    if p1 is None or p2 is None or p3 is None:
        return None

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
    # try per-exchange unified fees first
    fee = None
    try:
        fee = ex.fees.get("trading", {}).get("taker")
    except Exception:
        fee = None
    if not fee:
        # fall back to any market taker fee
        first_symbol = next(iter(spot_markets)) if spot_markets else None
        if first_symbol:
            fee = spot_markets[first_symbol].get("taker")
    return float(fee) if fee else 0.001  # 0.1% default

if scan_btn:
    with st.spinner("Scanning with market prices (last)…"):
        try:
            ex = getattr(ccxt, exchange_name)()
            ex.enableRateLimit = True
            ex.load_markets()

            # spot-only
            spot_markets = {s: m for s, m in ex.markets.items() if m.get("spot", False)}

            triangles = find_triangles(spot_markets)
            total_raw = len(triangles)
            triangles = triangles[:int(max_triangles)]
            st.write(f"🔍 Found {total_raw} unique spot triangles. Scanning {len(triangles)} (market price)…")

            # fetch all tickers once (efficient, avoids rate-limit)
            tickers = ex.fetch_tickers()

            taker_fee = pick_taker_fee(ex, spot_markets)

            results = []
            for tri in triangles:
                # two directions (A->B->C and A->C->B)
                seq1 = [tri[0], tri[1], tri[2]]
                seq2 = [tri[0], tri[2], tri[1]]

                for seq in (seq1, seq2):
                    row = evaluate_triangle_market_last(
                        ex, seq, tickers, taker_fee, include_prices=show_leg_prices
                    )
                    if row:
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
                    "⬇️ Download CSV",
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
    "Now using market price (ticker['last']) for all legs. Fees applied as 3× taker. "
    "WS order book manager kept for parity but not used in price calc."
)
