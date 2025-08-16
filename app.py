import streamlit as st
import ccxt
from collections import defaultdict
import pandas as pd
import time
import threading
import asyncio
import aiohttp
import json
from typing import Dict, Tuple, List, Optional

# =========================================================
# ---------------- WebSocket Order Book Manager -----------
# =========================================================

class WSOrderBookManager:
    """
    Live order book cache for Binance, KuCoin, Bybit via native WebSockets.
    For other exchanges, use REST (outside this class).

    Cache structure:
      self.books[(exchange, pair)] = {"bids": [(price, amount), ...],
                                      "asks": [(price, amount), ...],
                                      "ts": int_millis}
    """
    def __init__(self, depth_levels: int = 20):
        self.depth_levels = depth_levels
        self.loop = asyncio.new_event_loop()
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

        # live cache + subscriptions
        self.books: Dict[Tuple[str, str], Dict] = {}
        self.subscribed: Dict[Tuple[str, str], bool] = {}
        self.lock = threading.Lock()

        # KuCoin: dynamic endpoint (requires token)
        self.kucoin_endpoint = None
        self.kucoin_token = None
        self.kucoin_token_expiry = 0

    # ---------- event loop runner ----------
    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._ensure_session())
        self.loop.run_forever()

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    def close(self):
        async def _close():
            if self.session and not self.session.closed:
                await self.session.close()
        if self.loop.is_running():
            asyncio.run_coroutine_threadsafe(_close(), self.loop)

    # ---------- public API ----------
    def ensure_subscription(self, exchange: str, pair: str):
        """
        Ensure a (exchange, pair) is being streamed. Non-blocking.
        """
        key = (exchange.lower(), pair)
        if self.subscribed.get(key):  # already requested
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
        else:
            # Not supported (REST will be used)
            pass

    def get_book(self, exchange: str, pair: str) -> Optional[Dict]:
        with self.lock:
            return self.books.get((exchange.lower(), pair))

    # =====================================================
    # -------------------- BINANCE WS ---------------------
    # =====================================================
    async def _binance_subscribe(self, pair: str):
        """
        Subscribe to Binance spot partial depth for a symbol (pair like 'VET/USDT').
        Uses stream: wss://stream.binance.com:9443/ws
        """
        await self._ensure_session()
        symbol = pair.replace("/", "").lower()  # e.g. BTC/USDT -> btcusdt
        depth_tag = f"{symbol}@depth{self.depth_levels}@100ms"
        url = "wss://stream.binance.com:9443/ws"

        async def listen():
            try:
                async with self.session.ws_connect(url, heartbeat=30) as ws:
                    # Subscribe
                    sub_msg = {
                        "method": "SUBSCRIBE",
                        "params": [depth_tag],
                        "id": int(time.time()*1000) % 10_000_000
                    }
                    await ws.send_json(sub_msg)

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            # depth update: 'b' = bids, 'a' = asks (arrays of [price, qty])
                            if 'b' in data and 'a' in data:
                                bids = [(float(p), float(q)) for p, q in data.get('b', [])][:self.depth_levels]
                                asks = [(float(p), float(q)) for p, q in data.get('a', [])][:self.depth_levels]
                                with self.lock:
                                    self.books[("binance", pair)] = {
                                        "bids": bids, "asks": asks, "ts": int(time.time()*1000)
                                    }
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except Exception:
                # backoff and retry
                await asyncio.sleep(1.5)
                asyncio.create_task(listen())

        asyncio.create_task(listen())

    # =====================================================
    # --------------------- KUCOIN WS ---------------------
    # =====================================================
    async def _kucoin_public_token(self):
        # Get KuCoin public bullet
        await self._ensure_session()
        try:
            async with self.session.post("https://api.kucoin.com/api/v1/bullet-public") as resp:
                j = await resp.json()
                token = j["data"]["token"]
                # pick first server
                endpoint = j["data"]["instanceServers"][0]["endpoint"]
                pingInterval = j["data"]["instanceServers"][0]["pingInterval"]  # ms
                expireTime = j["data"]["instanceServers"][0]["expireTime"]      # ms
                self.kucoin_endpoint = endpoint
                self.kucoin_token = token
                self.kucoin_token_expiry = int(time.time()*1000) + (expireTime or 60_000)
                return endpoint, token, pingInterval
        except Exception:
            return None, None, None

    async def _kucoin_subscribe(self, pair: str):
        """
        Subscribe to KuCoin spot partial depth 5 or 20 for symbol like 'VET-USDT'
        KuCoin symbol format uses hyphen.
        """
        await self._ensure_session()
        endpoint, token, ping_ms = await self._kucoin_public_token()
        if not endpoint or not token:
            # Retry later
            await asyncio.sleep(2)
            asyncio.create_task(self._kucoin_subscribe(pair))
            return

        # Convert symbol: e.g. VET/USDT -> VET-USDT
        symbol = pair.replace("/", "-").upper()
        url = f"{endpoint}?token={token}"

        async def listen():
            try:
                async with self.session.ws_connect(url, heartbeat=max(10, (ping_ms or 20000)//1000)) as ws:
                    # Join channel market level2Depth{depth}
                    depth_ch = "level2Depth5" if self.depth_levels <= 5 else "level2Depth20"
                    sub = {
                        "id": str(int(time.time()*1000)),
                        "type": "subscribe",
                        "topic": f"/market/{depth_ch}:{symbol}",
                        "privateChannel": False,
                        "response": True
                    }
                    await ws.send_json(sub)

                    # Ping loop
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
                                # d has 'bids': [[price, size], ...], 'asks': [[price, size], ...]
                                bids = [(float(p), float(q)) for p, q in d.get("bids", [])][:self.depth_levels]
                                asks = [(float(p), float(q)) for p, q in d.get("asks", [])][:self.depth_levels]
                                with self.lock:
                                    self.books[("kucoin", pair)] = {
                                        "bids": bids, "asks": asks, "ts": d.get("time", int(time.time()*1000))
                                    }
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except Exception:
                await asyncio.sleep(1.5)
                asyncio.create_task(listen())

        asyncio.create_task(listen())

    # =====================================================
    # ---------------------- BYBIT WS ---------------------
    # =====================================================
    async def _bybit_subscribe(self, pair: str):
        """
        Bybit Spot public WS v5:
          wss://stream.bybit.com/v5/public/spot
        Topic: orderbook. Use depth = 50 (we'll slice to depth_levels).
        Symbol: e.g. VETUSDT
        """
        await self._ensure_session()
        url = "wss://stream.bybit.com/v5/public/spot"
        symbol = pair.replace("/", "").upper()

        async def listen():
            try:
                async with self.session.ws_connect(url, heartbeat=30) as ws:
                    sub = {
                        "op": "subscribe",
                        "args": [f"orderbook.50.{symbol}"]
                    }
                    await ws.send_json(sub)

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            # Snapshot or delta messages under "data" -> "b" (bids), "a" (asks), each [price, size]
                            if data.get("topic", "").startswith("orderbook") and "data" in data:
                                d = data["data"]
                                # Bybit might send dict with 'b'/'a' lists
                                bids = [(float(p), float(q)) for p, q in d.get("b", [])][:self.depth_levels]
                                asks = [(float(p), float(q)) for p, q in d.get("a", [])][:self.depth_levels]
                                if bids or asks:
                                    with self.lock:
                                        self.books[("bybit", pair)] = {
                                            "bids": bids, "asks": asks, "ts": int(time.time()*1000)
                                        }
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except Exception:
                await asyncio.sleep(1.5)
                asyncio.create_task(listen())

        asyncio.create_task(listen())


# Create a singleton WS manager for the Streamlit session (cached across reruns)
@st.cache_resource
def get_ws_manager(depth_levels: int):
    return WSOrderBookManager(depth_levels=depth_levels)

# =========================================================
# ---------------- Triangle/Profit Utilities --------------
# =========================================================

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

def get_pair(ex, a, b):
    p1 = f"{a}/{b}"
    if p1 in ex.markets:
        return p1
    p2 = f"{b}/{a}"
    if p2 in ex.markets:
        return p2
    return None

def fill_sell_base_for_quote(amount_base, orderbook_bids):
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

def fill_buy_base_with_quote(amount_quote, orderbook_asks):
    remaining_quote = float(amount_quote)
    base_bought = 0.0
    spent_quote = 0.0
    for price, vol_base in orderbook_asks:
        if remaining_quote <= 0:
            break
        max_base_affordable = remaining_quote / price
        trade_base = min(vol_base, max_base_affordable)
        cost = trade_base * price
        base_bought += trade_base
        spent_quote += cost
        remaining_quote -= cost
    return base_bought, spent_quote

def calc_profit_with_books(cycle, fee, trade_size, book_fetcher, ob_limit):
    """
    book_fetcher(exchange, pair) -> orderbook dict with 'bids'/'asks'
    """
    amount = float(trade_size)
    initial = amount
    before_product = 1.0
    reason = "OK"
    full_fill = True

    for i in range(3):
        from_c = cycle[i]
        to_c = cycle[(i + 1) % 3]
        pair, ob, side = book_fetcher(from_c, to_c)
        if pair is None or ob is None:
            return None, None, 0.0, "Missing data"

        bids = ob.get("bids", [])[:ob_limit]
        asks = ob.get("asks", [])[:ob_limit]
        if not bids or not asks:
            return None, None, 0.0, "No liquidity"

        base, quote = pair.split('/')

        if base == from_c and quote == to_c:
            # Sell base -> quote (use bids)
            quote_recv, filled_base = fill_sell_base_for_quote(amount, bids)
            if filled_base + 1e-12 < amount:
                full_fill = False
                reason = "Insufficient depth"
            ratio_pre_fee = (quote_recv / amount) if amount > 0 else 0.0
            before_product *= ratio_pre_fee
            amount = quote_recv * (1.0 - fee)

        elif base == to_c and quote == from_c:
            # Buy base with quote (use asks)
            base_bought, spent_quote = fill_buy_base_with_quote(amount, asks)
            if spent_quote + 1e-12 < amount:
                full_fill = False
                reason = "Insufficient depth"
            ratio_pre_fee = (base_bought / amount) if amount > 0 else 0.0
            before_product *= ratio_pre_fee
            amount = base_bought * (1.0 - fee)
        else:
            return None, None, 0.0, "Pair mismatch"

    profit_before = (before_product - 1.0) * 100.0
    profit_after = ((amount / initial) - 1.0) * 100.0
    fill_pct = (amount / initial) * 100.0
    return round(profit_before, 6), round(profit_after, 6), round(fill_pct, 2), ("OK" if full_fill else reason)

# =========================================================
# ----------------------- Streamlit UI --------------------
# =========================================================
st.set_page_config(page_title="Triangular Arbitrage (WS + REST)", layout="wide")
st.title("Triangular Arbitrage Scanner — WebSockets (Binance/KuCoin/Bybit) + REST (others)")

c1, c2, c3 = st.columns(3)
with c1:
    exchange_name = st.selectbox("Exchange", ["binance", "kucoin", "bybit", "kraken", "bitfinex", "huobi", "gateio", "mexc", "bitget", "bitmart"])
with c2:
    base_currency = st.selectbox("Base Currency", ["USDT", "BTC", "ETH", "USD", "BNB"])
with c3:
    min_profit = st.number_input("Highlight Profit % Threshold", min_value=0.0, value=0.0, step=0.01)

c4, c5, c6 = st.columns(3)
with c4:
    trade_size = st.number_input("Trade Size", min_value=0.0, value=200.0, step=50.0)
with c5:
    num_opp = st.selectbox("Show Top N", [10, 15, 20, 30])
with c6:
    ob_limit = st.select_slider("Order Book Depth (levels)", options=[5, 10, 20, 40, 60, 100], value=20)

max_triangles_default = 120 if exchange_name.lower() in ("binance", "kucoin", "bybit") else 300
max_triangles = st.number_input("Max Triangles to Scan", min_value=30, max_value=1000, value=max_triangles_default, step=30)

# Create/get WS manager with chosen depth
ws_manager = get_ws_manager(depth_levels=min(50, max(5, ob_limit)))

# =========================================================
# -------------- Helper: building book fetcher ------------
# =========================================================

def build_book_fetcher(ex, exch_name: str, ob_cache_rest: dict):
    exlower = exch_name.lower()

    def fetch_from_ws_or_rest(from_c: str, to_c: str):
        pair = get_pair(ex, from_c, to_c)
        if not pair:
            return None, None, None

        # If exchange supports WS, ensure subscription and read cache
        if exlower in ("binance", "kucoin", "bybit"):
            # Ask WS manager to subscribe (non-blocking)
            ws_manager.ensure_subscription(exlower, pair)

            # Try to get a fresh snapshot from cache; wait briefly
            start = time.time()
            book = None
            while time.time() - start < 1.2:  # wait up to ~1.2s for first snapshot
                book = ws_manager.get_book(exlower, pair)
                if book and book.get("bids") and book.get("asks"):
                    break
                time.sleep(0.05)

            if book and book.get("bids") and book.get("asks"):
                return pair, book, "ws"

            # Fallback to REST if WS not yet ready
            try:
                if pair not in ob_cache_rest:
                    ob_cache_rest[pair] = ex.fetch_order_book(pair, limit=ob_limit)
                ob = ob_cache_rest[pair]
                # Convert to our (price, amount) list format
                bids = [(float(p), float(q)) for p, q in ob.get("bids", [])][:ob_limit]
                asks = [(float(p), float(q)) for p, q in ob.get("asks", [])][:ob_limit]
                if bids and asks:
                    return pair, {"bids": bids, "asks": asks, "ts": ob.get("timestamp", int(time.time()*1000))}, "rest-fallback"
            except Exception:
                return None, None, None

            return None, None, None

        # Non-WS exchanges: REST only (with simple cache)
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

# =========================================================
# -------------------------- Scan -------------------------
# =========================================================
if st.button("Scan for Opportunities"):
    with st.spinner("Scanning..."):
        try:
            ex = getattr(ccxt, exchange_name)()
            ex.enableRateLimit 
