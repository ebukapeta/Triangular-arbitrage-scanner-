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
# =========================================================

class WSOrderBookManager:
    """
    Live order book cache for Binance, KuCoin, Bybit via native WebSockets.
    For other exchanges, we fall back to REST.
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

# Cache resource for WS manager
@st.cache_resource
def get_ws_manager(depth_levels: int):
    return WSOrderBookManager(depth_levels=depth_levels)

# =========================================================
# ---------------- Triangle Finder ------------------------
# =========================================================

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
    # =========================================================
# ---------------- Profit Calculation ---------------------
# =========================================================

def get_pair(ex, a, b):
    p1 = f"{a}/{b}"
    if p1 in ex.markets:
        return p1
    p2 = f"{b}/{a}"
    if p2 in ex.markets:
        return p2
    return None

def calc_profit_best_bidask(cycle, fee, book_fetcher):
    amount_ratio = 1.0
    before_ratio = 1.0
    for i in range(3):
        from_c = cycle[i]
        to_c = cycle[(i + 1) % 3]
        pair, ob, source = book_fetcher(from_c, to_c)
        if pair is None or ob is None:
            return None, None, "Missing data"

        bids = ob.get("bids", [])
        asks = ob.get("asks", [])
        if not bids or not asks:
            return None, None, "No liquidity"

        base, quote = pair.split('/')

        if base == from_c and quote == to_c:
            # sell base → get quote, use best bid
            bid_price = bids[0][0]
            before_ratio *= bid_price
            amount_ratio *= bid_price * (1 - fee)
        elif base == to_c and quote == from_c:
            # buy base with quote, use best ask
            ask_price = asks[0][0]
            before_ratio *= (1.0 / ask_price)
            amount_ratio *= (1.0 / ask_price) * (1 - fee)
        else:
            return None, None, "Pair mismatch"

    profit_before = (before_ratio - 1.0) * 100.0
    profit_after = (amount_ratio - 1.0) * 100.0
    return round(profit_before, 6), round(profit_after, 6), "OK"

# =========================================================
# ----------------------- Streamlit UI --------------------
# =========================================================
st.set_page_config(page_title="Triangular Arbitrage Scanner", layout="wide")
st.title("Triangular Arbitrage Scanner — Spot Market Only")

c1, c2, c3 = st.columns(3)
with c1:
    exchange_name = st.selectbox("Exchange", [
        "binance", "kucoin", "bybit", "kraken", "bitfinex", "huobi",
        "gateio", "mexc", "bitget", "bitmart"
    ])
with c2:
    min_profit = st.number_input("Highlight Profit % Threshold", min_value=0.0, value=0.0, step=0.01)
with c3:
    ob_limit = st.select_slider("Order Book Depth (levels)", options=[5, 10, 20, 40, 60, 100], value=5)

max_triangles = st.number_input("Max Triangles to Scan", min_value=30, max_value=2000, value=300, step=50)

# Create/get WS manager
ws_manager = get_ws_manager(depth_levels=min(50, max(5, ob_limit)))

# ---------------- Build Book Fetcher ----------------
def build_book_fetcher(ex, exch_name: str, ob_cache_rest: dict):
    exlower = exch_name.lower()

    def fetch_from_ws_or_rest(from_c: str, to_c: str):
        pair = get_pair(ex, from_c, to_c)
        if not pair:
            return None, None, None

        if exlower in ("binance", "kucoin", "bybit"):
            ws_manager.ensure_subscription(exlower, pair)

            # ✅ Try WS cache immediately
            book = ws_manager.get_book(exlower, pair)
            if book and book.get("bids") and book.get("asks"):
                return pair, book, "ws"

            # ✅ Fallback to REST if WS not ready
            try:
                if pair not in ob_cache_rest:
                    ob_cache_rest[pair] = ex.fetch_order_book(pair, limit=ob_limit)
                ob = ob_cache_rest[pair]
                bids = [(float(p), float(q)) for p, q in ob.get("bids", [])][:ob_limit]
                asks = [(float(p), float(q)) for p, q in ob.get("asks", [])][:ob_limit]
                if bids and asks:
                    return pair, {"bids": bids, "asks": asks, "ts": ob.get("timestamp", int(time.time()*1000))}, "rest-fallback"
            except Exception:
                return None, None, None
            return None, None, None

        # ✅ REST only for other exchanges
        try:
            if pair not in ob_cache_rest:
                ob_cache_rest[pair] = ex.fetch_order_book(pair, limit=ob_limit)
            ob = ob_cache_rest[pair]
            bids = [(float(p), float(q)) for p, q in ob.get("bids", [])][:ob_limit]
            asks = [(float(p), float(q)) for p, q in ob.get("asks", [])][:ob_limit]
            if bids and asks:
                return pair, {"bids": bids, "asks": asks, "ts": ob.get("timestamp", int(time.time()*1000))}, "rest"
        except Exception:
            return None, None, None
        return None, None, None

    return fetch_from_ws_or_rest

# ---------------- Scan Button ----------------
if st.button("Scan for Opportunities"):
    with st.spinner("Scanning..."):
        try:
            ex = getattr(ccxt, exchange_name)()
            ex.enableRateLimit = True
            ex.load_markets()

            # spot-only
            spot_markets = {s: m for s, m in ex.markets.items() if m.get("spot", False)}
            triangles = find_triangles(spot_markets)
            total_raw = len(triangles)
            triangles = triangles[:max_triangles]
            st.write(f"🔍 Found {total_raw} unique spot triangles. Scanning {len(triangles)}...")

            first_market = next((m for m in spot_markets) if spot_markets else None)
            fee = ex.markets[first_market].get('taker', 0.001) if first_market else 0.001

            ob_cache_rest: Dict[str, dict] = {}
            book_fetcher = build_book_fetcher(ex, exchange_name, ob_cache_rest)

            results = []
            for tri in triangles:
                for direction in [list(tri), list(tri)[::-1]]:
                    pb, pa, reason = calc_profit_best_bidask(direction, fee, book_fetcher)
                    if pb is not None and pa is not None:
                        results.append({
                            "Coin Pairs": " -> ".join(direction) + f" -> {direction[0]}",
                            "Profit % BEFORE Fees": pb,
                            "Profit % AFTER Fees": pa,
                            "Reason": reason,
                            "Fee % (total)": round(fee * 3 * 100, 6)
                        })

            if results:
                results.sort(key=lambda x: x["Profit % AFTER Fees"], reverse=True)
                st.subheader(f"Top {min(len(results), 20)} Cycles")
                st.dataframe(pd.DataFrame(results[:20]), use_container_width=True)

                profitable = [
                    r for r in results
                    if r["Profit % AFTER Fees"] > min_profit and r["Reason"] == "OK"
                ]
                if profitable:
                    st.subheader("Profitable Above Threshold")
                    st.dataframe(pd.DataFrame(profitable[:20]), use_container_width=True)
                else:
                    st.info("No profitable cycles above threshold.")
            else:
                st.info("No cycles produced results.")

        except ccxt.RateLimitExceeded:
            st.error("Rate limit exceeded. For Binance/KuCoin/Bybit, WebSockets are used to reduce this.")
        except Exception as e:
            st.error(f"Error: {e}")

st.caption(
    "Now scanning all SPOT markets only. Shows both raw (before fees) and net (after fees) profit. "
    "Binance/KuCoin/Bybit use WebSockets; others use REST."
        )
