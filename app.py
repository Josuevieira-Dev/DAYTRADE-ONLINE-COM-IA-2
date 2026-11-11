import asyncio, json, math, os, time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT").split(",") if s.strip()]
REST_URL = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(
    f"{s.lower()}@kline_1m" for s in SYMBOLS
)
STARTING_CAPITAL = float(os.getenv("STARTING_CAPITAL", "1000"))
RISK_PCT = float(os.getenv("RISK_PCT", "2"))
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"

app_state = {
    "connected": False,
    "mode": "SIMULAÇÃO",
    "capital": STARTING_CAPITAL,
    "risk_pct": RISK_PCT,
    "symbols": {},
    "events": deque(maxlen=100),
    "last_errors": {},
    "last_ticks": {},
    "alerts": deque(maxlen=50),
}
last_confirmed_signal = {symbol: "AGUARDAR" for symbol in SYMBOLS}
last_processed_closed = {symbol: 0 for symbol in SYMBOLS}
history = defaultdict(lambda: deque(maxlen=250))
clients = set()

def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    value = sum(values[:period]) / period
    for x in values[period:]:
        value = x * k + value * (1-k)
    return value

def rsi(values, period=14):
    if len(values) <= period:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_atr(candles, period=14):
    if len(candles) <= period:
        return None
    trs = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i-1]
        trs.append(max(c["high"]-c["low"], abs(c["high"]-p["close"]), abs(c["low"]-p["close"])))
    return sum(trs[-period:]) / period

def analyze(symbol):
    candles = list(history[symbol])
    if len(candles) < 55:
        return {"symbol": symbol, "signal": "AGUARDAR", "score": 0, "reason": f"Histórico insuficiente: {len(candles)}/55 candles recebidos da Binance", "ready": False, "candles_loaded": len(candles)}
    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]
    e9, e20, e50 = ema(closes, 9), ema(closes, 20), ema(closes, 50)
    r = rsi(closes)
    a = calc_atr(candles)
    macd_line = (ema(closes, 12) or closes[-1]) - (ema(closes, 26) or closes[-1])
    signal_line = macd_line  # aproximação conservadora: cruzamento MACD completo fica para versão seguinte
    vol_avg = sum(volumes[-20:]) / 20
    vol_ok = volumes[-1] > vol_avg
    long_score = 0
    short_score = 0
    if e9 > e20 > e50: long_score += 30
    if e9 < e20 < e50: short_score += 30
    if r is not None and 50 < r < 70: long_score += 20
    if r is not None and 30 < r < 50: short_score += 20
    if closes[-1] > e9: long_score += 15
    if closes[-1] < e9: short_score += 15
    if macd_line > 0: long_score += 15
    if macd_line < 0: short_score += 15
    if vol_ok:
        if closes[-1] >= closes[-2]: long_score += 20
        else: short_score += 20
    score = max(long_score, short_score)
    signal = "AGUARDAR"
    reason = "Condições insuficientes ou indicadores divergentes"
    if long_score >= 70 and long_score - short_score >= 20 and r is not None and r < 75:
        signal, reason = "LONG", "Tendência de alta e confirmações técnicas alinhadas"
    elif short_score >= 70 and short_score - long_score >= 20 and r is not None and r > 25:
        signal, reason = "SHORT", "Tendência de baixa e confirmações técnicas alinhadas"
    price = closes[-1]
    stop_distance = (a or price * 0.005) * 1.5
    risk_amount = app_state["capital"] * app_state["risk_pct"] / 100
    qty = risk_amount / stop_distance if stop_distance > 0 else 0
    stop = price - stop_distance if signal == "LONG" else price + stop_distance if signal == "SHORT" else None
    target = price + stop_distance * 2 if signal == "LONG" else price - stop_distance * 2 if signal == "SHORT" else None
    return {
        "symbol": symbol, "signal": signal, "score": score,
        "long_score": long_score, "short_score": short_score,
        "price": price, "rsi": r, "ema9": e9, "ema20": e20, "ema50": e50,
        "atr": a, "volume_ok": vol_ok, "stop": stop, "target": target,
        "risk_usd": risk_amount, "quantity_estimate": qty,
        "reason": reason, "ready": True,
        "candle_time": candles[-1]["time"]
    }

async def broadcast(payload):
    dead = []
    msg = json.dumps(payload)
    for ws in list(clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)

async def load_history():
    # Futures primeiro; Spot público como alternativa para obter o histórico M1.
    endpoints = [(f'{REST_URL}/fapi/v1/klines', 'Futures'), ('https://api.binance.com/api/v3/klines', 'Spot fallback')]
    async with httpx.AsyncClient(timeout=20, headers={'User-Agent': 'Mozilla/5.0'}) as client:
        for symbol in SYMBOLS:
            loaded = False
            errors = []
            for url, label in endpoints:
                try:
                    r = await client.get(url, params={'symbol': symbol, 'interval': '1m', 'limit': 200})
                    r.raise_for_status()
                    rows = r.json()
                    if not isinstance(rows, list) or len(rows) < 55:
                        raise ValueError(f'Resposta com apenas {len(rows) if isinstance(rows, list) else 0} candles')
                    history[symbol].clear()
                    for row in rows:
                        history[symbol].append({'time': int(row[0] / 1000), 'open': float(row[1]), 'high': float(row[2]), 'low': float(row[3]), 'close': float(row[4]), 'volume': float(row[5]), 'closed': int(row[6]) < int(time.time()*1000)})
                    app_state["last_errors"].pop(symbol, None)
                    loaded = True
                    break
                except Exception as e:
                    errors.append(f'{label}: {str(e)[:180]}')
            if not loaded:
                app_state["last_errors"][symbol] = " | ".join(errors)
                print(f"[HISTORY] {symbol}: " + app_state["last_errors"][symbol], flush=True)

async def history_refresh_loop():
    while True:
        await asyncio.sleep(30)
        if any(len(history[symbol]) < 55 for symbol in SYMBOLS):
            await load_history()

async def ingest_candle(symbol, candle):
    if history[symbol] and history[symbol][-1]["time"] == candle["time"]:
        history[symbol][-1] = candle
    elif not history[symbol] or candle["time"] > history[symbol][-1]["time"]:
        history[symbol].append(candle)
    result = analyze(symbol)
    app_state["symbols"][symbol] = result
    await broadcast({"type": "market", "symbol": symbol, "candle": candle, "analysis": result})
    if candle["closed"] and candle["time"] > last_processed_closed.get(symbol, 0):
        last_processed_closed[symbol] = candle["time"]
        result = analyze(symbol)
        app_state["symbols"][symbol] = result
        confirmed = result["signal"]
        prior = last_confirmed_signal.get(symbol, "AGUARDAR")
        last_confirmed_signal[symbol] = confirmed
        if confirmed in ("LONG", "SHORT") and confirmed != prior:
            alert = {
                "id": f"{symbol}-{candle['time']}-{confirmed}",
                "symbol": symbol, "signal": confirmed,
                "action": "COMPRA" if confirmed == "LONG" else "VENDA",
                "price": result.get("price"), "score": result.get("score"),
                "stop": result.get("stop"), "target": result.get("target"),
                "time": int(time.time()), "candle_time": candle["time"],
                "reason": result.get("reason", "")
            }
            app_state["alerts"].appendleft(alert)
            app_state["events"].appendleft({
                "time": datetime.now().strftime("%H:%M:%S"),
                "text": f'{symbol}: sinal {confirmed} — score {result["score"]}/100'
            })
            await broadcast({"type": "signal_alert", "alert": alert})
    return result

async def market_loop():
    backoff = 2
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                app_state["connected"] = True
                app_state["last_errors"].pop("websocket", None)
                backoff = 2
                await broadcast({"type": "status", "connected": True})
                async for raw in ws:
                    packet = json.loads(raw)
                    data = packet.get("data", {})
                    k = data.get("k")
                    if not k:
                        continue
                    symbol = k["s"]
                    app_state["last_ticks"][symbol] = int(time.time())
                    candle = {
                        "time": int(k["t"] / 1000), "open": float(k["o"]),
                        "high": float(k["h"]), "low": float(k["l"]),
                        "close": float(k["c"]), "volume": float(k["v"]),
                        "closed": bool(k["x"])
                    }
                    await ingest_candle(symbol, candle)
        except Exception as e:
            app_state["connected"] = False
            app_state["last_errors"]["websocket"] = str(e)
            print(f"[WEBSOCKET] {e}", flush=True)
            await broadcast({"type": "status", "connected": False, "error": str(e)})
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

@asynccontextmanager
async def lifespan(app):
    await load_history()
    task = asyncio.create_task(market_loop())
    retry_task = asyncio.create_task(history_refresh_loop())
    yield
    task.cancel()
    retry_task.cancel()

app = FastAPI(title="IA Crypto Trader M1", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def home():
    return FileResponse("static/index.html")

@app.post("/api/history/{symbol}")
async def receive_history(symbol: str, payload: dict):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {"ok": False, "error": "Símbolo não monitorado"}
    rows = payload.get("candles", [])
    if not isinstance(rows, list) or len(rows) < 55:
        return {"ok": False, "error": "Histórico insuficiente; são necessários ao menos 55 candles"}
    cleaned = []
    try:
        for row in rows[-250:]:
            cleaned.append({
                "time": int(row["time"]), "open": float(row["open"]),
                "high": float(row["high"]), "low": float(row["low"]),
                "close": float(row["close"]), "volume": float(row.get("volume", 0)),
                "closed": bool(row.get("closed", True))
            })
        cleaned.sort(key=lambda item: item["time"])
        history[symbol].clear()
        history[symbol].extend(cleaned)
        app_state["last_errors"].pop(symbol, None)
        app_state["symbols"][symbol] = analyze(symbol)
        return {"ok": True, "loaded": len(cleaned), "analysis": app_state["symbols"][symbol]}
    except (KeyError, TypeError, ValueError) as exc:
        return {"ok": False, "error": f"Formato de candle inválido: {exc}"}

@app.post("/api/market/{symbol}")
async def receive_market(symbol: str, payload: dict):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {"ok": False, "error": "Símbolo não monitorado"}
    rows = payload.get("candles", [])
    if not isinstance(rows, list) or not rows:
        return {"ok": False, "error": "Nenhum candle recebido"}
    try:
        # Processa candles em ordem; sinais só são confirmados no fechamento M1.
        result = None
        for row in sorted(rows[-3:], key=lambda item: int(item["time"])):
            candle = {
                "time": int(row["time"]), "open": float(row["open"]),
                "high": float(row["high"]), "low": float(row["low"]),
                "close": float(row["close"]), "volume": float(row.get("volume", 0)),
                "closed": bool(row.get("closed", False))
            }
            result = await ingest_candle(symbol, candle)
        return {"ok": True, "history": len(history[symbol]), "analysis": result}
    except (KeyError, TypeError, ValueError) as exc:
        return {"ok": False, "error": f"Formato de candle inválido: {exc}"}

@app.get("/api/klines/{symbol}")
async def klines(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        return {"ok": False, "error": "Símbolo não monitorado"}
    return {"ok": True, "candles": list(history[symbol])}

@app.get("/api/state")
async def state():
    return {
        "connected": app_state["connected"], "mode": app_state["mode"],
        "capital": app_state["capital"], "risk_pct": app_state["risk_pct"],
        "symbols": {s: analyze(s) for s in SYMBOLS},
        "history_counts": {s: len(history[s]) for s in SYMBOLS},
        "last_ticks": app_state["last_ticks"],
        "data_errors": app_state["last_errors"],
        "events": list(app_state["events"]),
        "alerts": list(app_state["alerts"])
    }

@app.post("/api/settings")
async def settings(payload: dict):
    capital = float(payload.get("capital", app_state["capital"]))
    risk = float(payload.get("risk_pct", app_state["risk_pct"]))
    if capital <= 0 or not 0 < risk <= 2:
        return {"ok": False, "error": "Capital deve ser positivo e risco deve ficar entre 0 e 2%."}
    app_state["capital"], app_state["risk_pct"] = capital, risk
    return {"ok": True, "capital": capital, "risk_pct": risk}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    await ws.send_text(json.dumps({"type": "snapshot", "state": await state()}))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
