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
STARTING_CAPITAL = float(os.getenv("STARTING_CAPITAL", "100000"))
RISK_PCT = float(os.getenv("RISK_PCT", "2"))
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"

app_state = {
    "connected": False,
    "mode": "SIMULAÇÃO",
    "capital": 100000.0,
    "risk_pct": RISK_PCT,
    "symbols": {},
    "events": deque(maxlen=100),
    "last_errors": {},
    "last_ticks": {},
    "alerts": deque(maxlen=50),
}
last_confirmed_signal = {symbol: "AGUARDAR" for symbol in SYMBOLS}
history = defaultdict(lambda: deque(maxlen=250))
clients = set()
paper_trades = deque(maxlen=500)
next_trade_id = 1

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
    # Pontuação simétrica: compra e venda usam os mesmos critérios e pesos.
    if e9 > e20: long_score += 20
    elif e9 < e20: short_score += 20
    if e20 > e50: long_score += 15
    elif e20 < e50: short_score += 15
    if r is not None and r >= 52: long_score += 15
    elif r is not None and r <= 48: short_score += 15
    if closes[-1] > e9: long_score += 15
    elif closes[-1] < e9: short_score += 15
    if macd_line > 0: long_score += 15
    elif macd_line < 0: short_score += 15
    if closes[-1] > closes[-2]: long_score += 10
    elif closes[-1] < closes[-2]: short_score += 10
    if vol_ok:
        if closes[-1] >= closes[-2]: long_score += 10
        else: short_score += 10
    score = max(long_score, short_score)
    signal = "AGUARDAR"
    reason = "Aguardando alinhamento dos indicadores"
    if long_score >= 65 and long_score - short_score >= 15 and r is not None and r < 78:
        signal, reason = "LONG", "COMPRA: tendência e momentum favorecem movimento de alta"
    elif short_score >= 65 and short_score - long_score >= 15 and r is not None and r > 22:
        signal, reason = "SHORT", "VENDA: tendência e momentum favorecem movimento de baixa"
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
        "candle_time": candles[-1]["time"],
        "signal_time": int(time.time())
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
    # OKX como fonte principal: Binance retorna HTTP 451 no ambiente do Render.
    async with httpx.AsyncClient(timeout=20, headers={"User-Agent": "Mozilla/5.0"}) as client:
        for symbol in SYMBOLS:
            loaded = False
            errors = []
            sources = [
                (f"https://www.okx.com/api/v5/market/candles", "OKX"),
                (f"{REST_URL}/fapi/v1/klines", "Binance Futures"),
                ("https://api.binance.com/api/v3/klines", "Binance Spot")
            ]
            for url, label in sources:
                try:
                    if label == "OKX":
                        response = await client.get(url, params={"instId": symbol.replace("USDT", "-USDT"), "bar": "1m", "limit": "200"})
                        response.raise_for_status()
                        payload = response.json()
                        if payload.get("code") != "0":
                            raise ValueError(payload.get("msg") or "Resposta inválida da OKX")
                        rows = payload.get("data", [])
                        parsed = [{"time": int(row[0]) // 1000, "open": float(row[1]), "high": float(row[2]),
                                   "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
                                   "closed": str(row[8]) == "1"} for row in rows]
                    else:
                        response = await client.get(url, params={"symbol": symbol, "interval": "1m", "limit": 200})
                        response.raise_for_status()
                        rows = response.json()
                        parsed = [{"time": int(row[0] / 1000), "open": float(row[1]), "high": float(row[2]),
                                   "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
                                   "closed": int(row[6]) < int(time.time()*1000)} for row in rows]
                    if len(parsed) < 55:
                        raise ValueError(f"Resposta com apenas {len(parsed)} candles")
                    history[symbol].clear()
                    history[symbol].extend(sorted(parsed, key=lambda item: item["time"]))
                    app_state["last_errors"].pop(symbol, None)
                    loaded = True
                    print(f"[HISTORY] {symbol}: {len(parsed)} candles via {label}", flush=True)
                    break
                except Exception as exc:
                    errors.append(f"{label}: {str(exc)[:160]}")
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
    for trade in paper_trades:
        if trade["status"] == "ABERTA" and trade["symbol"] == symbol:
            direction = 1 if trade["side"] == "COMPRA" else -1
            trade["current_price"] = candle["close"]
            trade["floating_pnl"] = (candle["close"] - trade["entry_price"]) * trade["quantity"] * direction
    await broadcast({"type": "market", "symbol": symbol, "candle": candle, "analysis": result})

    # Detecta o sinal assim que os critérios técnicos aparecem, sem esperar fechar o M1.
    detected = result["signal"]
    previous = last_confirmed_signal.get(symbol, "AGUARDAR")
    if detected != previous:
        last_confirmed_signal[symbol] = detected
        if detected in ("LONG", "SHORT"):
            now = datetime.now().astimezone()
            action = "COMPRA" if detected == "LONG" else "VENDA"
            alert = {
                "id": f"{symbol}-{candle['time']}-{detected}-{int(time.time())}",
                "symbol": symbol, "signal": detected, "action": action,
                "price": result.get("price"), "score": result.get("score"),
                "stop": result.get("stop"), "target": result.get("target"),
                "time": int(now.timestamp()), "candle_time": candle["time"],
                "entry_time": now.isoformat(timespec="seconds"),
                "timing": "Sinal detectado durante o candle M1",
                "reason": result.get("reason", "")
            }
            app_state["alerts"].appendleft(alert)
            app_state["events"].appendleft({
                "time": now.strftime("%H:%M:%S"),
                "text": f'{symbol}: {action} {detected} — score {result["score"]}/100'
            })
            await broadcast({"type": "signal_alert", "alert": alert})
    return result

async def market_loop():
    backoff = 2
    ws_url = "wss://ws.okx.com:8443/ws/v5/business"
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                args = [{"channel": "candle1m", "instId": symbol.replace("USDT", "-USDT")} for symbol in SYMBOLS]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                app_state["connected"] = True
                app_state["last_errors"].pop("websocket", None)
                backoff = 2
                await broadcast({"type": "status", "connected": True})
                async for raw in ws:
                    packet = json.loads(raw)
                    if packet.get("event"):
                        if packet.get("event") == "error":
                            raise RuntimeError(packet.get("msg", "Erro de assinatura OKX"))
                        continue
                    arg = packet.get("arg", {})
                    inst_id = arg.get("instId", "")
                    symbol = inst_id.replace("-", "")
                    if symbol not in SYMBOLS:
                        continue
                    for row in packet.get("data", []):
                        candle = {
                            "time": int(row[0]) // 1000, "open": float(row[1]),
                            "high": float(row[2]), "low": float(row[3]),
                            "close": float(row[4]), "volume": float(row[5]),
                            "closed": str(row[8]) == "1" if len(row) > 8 else False
                        }
                        app_state["last_ticks"][symbol] = int(time.time())
                        await ingest_candle(symbol, candle)
        except Exception as exc:
            app_state["connected"] = False
            app_state["last_errors"]["websocket"] = str(exc)
            print(f"[WEBSOCKET OKX] {exc}", flush=True)
            await broadcast({"type": "status", "connected": False, "error": str(exc)})
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

@app.get("/api/trades")
async def get_trades():
    opened = [dict(t) for t in paper_trades if t["status"] == "ABERTA"]
    closed = [dict(t) for t in paper_trades if t["status"] == "FECHADA"]
    return {"ok": True, "balance": app_state["capital"], "open": opened, "closed": closed[-100:],
            "floating_pnl": round(sum(t.get("floating_pnl", 0) for t in opened), 8)}

@app.post("/api/trades/open")
async def open_trade(payload: dict):
    global next_trade_id
    side = str(payload.get("side", "")).upper()
    symbol = str(payload.get("symbol", "")).upper()
    try:
        amount = float(payload.get("amount", 0))
    except (TypeError, ValueError):
        return {"ok": False, "error": "Informe um valor válido."}
    if side not in ("COMPRA", "VENDA") or symbol not in SYMBOLS:
        return {"ok": False, "error": "Selecione COMPRA ou VENDA e um ativo válido."}
    market = app_state["symbols"].get(symbol) or analyze(symbol)
    price = market.get("price")
    if not price or amount <= 0 or amount > app_state["capital"]:
        return {"ok": False, "error": "Valor inválido ou maior que o saldo disponível."}
    trade = {"id": next_trade_id, "symbol": symbol, "side": side, "amount": amount,
             "entry_price": float(price), "current_price": float(price), "quantity": amount / float(price),
             "floating_pnl": 0.0, "status": "ABERTA", "opened_at": int(time.time())}
    next_trade_id += 1
    paper_trades.appendleft(trade)
    return {"ok": True, "trade": trade, "balance": app_state["capital"]}

@app.post("/api/trades/{trade_id}/close")
async def close_trade(trade_id: int):
    trade = next((t for t in paper_trades if t["id"] == trade_id and t["status"] == "ABERTA"), None)
    if not trade:
        return {"ok": False, "error": "Operação aberta não encontrada."}
    market = app_state["symbols"].get(trade["symbol"]) or analyze(trade["symbol"])
    exit_price = float(market.get("price") or trade["current_price"])
    direction = 1 if trade["side"] == "COMPRA" else -1
    pnl = (exit_price - trade["entry_price"]) * trade["quantity"] * direction
    trade.update({"current_price": exit_price, "exit_price": exit_price, "pnl": round(pnl, 8),
                  "status": "FECHADA", "closed_at": int(time.time())})
    app_state["capital"] = round(app_state["capital"] + pnl, 8)
    trade["balance_after"] = app_state["capital"]
    return {"ok": True, "trade": trade, "balance": app_state["capital"]}

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
