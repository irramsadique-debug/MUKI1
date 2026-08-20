"""
Crypto Signal Platform - Backend + Bot (single-file, beginner-friendly version)
--------------------------------------------------------------------------
Ye ek hi file mein hai taake samajhna aur maintain karna aasan ho:
  1. Database models (Users, Signals, Settings)
  2. Binance se top coins ka data uthana aur RSI/EMA/MACD nikalna
  3. Signal generate karke database mein save karna (background scanner)
  4. Public API (dashboard/search ke liye) + Admin API (JWT protected)
"""

import os
import time
import threading
from datetime import datetime, timedelta

import ccxt
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, MACD

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean, DateTime, JSON
)
from sqlalchemy.orm import sessionmaker, declarative_base, Session

from apscheduler.schedulers.background import BackgroundScheduler

from jose import jwt, JWTError
from passlib.context import CryptContext

# --------------------------------------------------------------------------
# 1. CONFIG (Database URL embedded to bypass env lookup issues)
# --------------------------------------------------------------------------
DATABASE_URL = "postgresql://muki_db_user:W7X6cJtS31Vf66weeVs7lMOfZ8pJWxAT@dpg-da3cf3e1egvs73c9vl10-a/muki_db"

JWT_SECRET = os.getenv("JWT_SECRET", "change_me")
JWT_ALGORITHM = "HS256"
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")[:72]  # Truncated to safe length for bcrypt
SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "15"))
TOP_N_COINS = int(os.getenv("TOP_N_COINS", "200"))
DEFAULT_RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", "30"))
DEFAULT_RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", "70"))
DEFAULT_MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "65"))

# --------------------------------------------------------------------------
# 2. DATABASE SETUP
# --------------------------------------------------------------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(20), default="admin")
    created_at = Column(DateTime, default=datetime.utcnow)


class Settings(Base):
    __tablename__ = "settings"
    id = Column(Integer, primary_key=True)
    rsi_oversold = Column(Float, default=DEFAULT_RSI_OVERSOLD)
    rsi_overbought = Column(Float, default=DEFAULT_RSI_OVERBOUGHT)
    min_confidence = Column(Float, default=DEFAULT_MIN_CONFIDENCE)
    scan_interval_minutes = Column(Integer, default=SCAN_INTERVAL_MINUTES)
    top_n_coins = Column(Integer, default=TOP_N_COINS)
    scanner_enabled = Column(Boolean, default=True)
    updated_at = Column(DateTime, default=datetime.utcnow)


class Signal(Base):
    __tablename__ = "signals"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False)
    direction = Column(String(5), nullable=False)   # LONG | SHORT
    entry = Column(Float, nullable=False)
    stop_loss = Column(Float, nullable=False)
    take_profit_1 = Column(Float, nullable=False)
    take_profit_2 = Column(Float, nullable=True)
    risk_reward = Column(Float, nullable=True)
    confidence = Column(Float, nullable=True)
    rsi = Column(Float, nullable=True)
    trend = Column(String(30), nullable=True)
    status = Column(String(20), default="active")   # active | closed
    created_by = Column(String(20), default="system")  # system | admin
    created_at = Column(DateTime, default=datetime.utcnow)


class AdminLog(Base):
    __tablename__ = "admin_logs"
    id = Column(Integer, primary_key=True)
    action = Column(String(100))
    details = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(bind=engine)

# --------------------------------------------------------------------------
# 3. SEED DEFAULT ADMIN + SETTINGS (runs once, on startup)
# --------------------------------------------------------------------------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def seed_defaults():
    db = SessionLocal()
    try:
        if not db.query(Settings).first():
            db.add(Settings())
        if not db.query(User).filter(User.email == ADMIN_EMAIL).first():
            db.add(User(
                email=ADMIN_EMAIL,
                password_hash=pwd_context.hash(ADMIN_PASSWORD),
                role="admin",
            ))
        db.commit()
    finally:
        db.close()


# --------------------------------------------------------------------------
# 4. INDICATOR + SIGNAL LOGIC
# --------------------------------------------------------------------------
binance = ccxt.binance({"enableRateLimit": True})


def get_top_usdt_symbols(limit: int) -> list[str]:
    """Binance ke saare active USDT spot pairs lo, 24h volume ke hisaab se sort karo."""
    tickers = binance.fetch_tickers()
    usdt_pairs = [
        (symbol, t.get("quoteVolume") or 0)
        for symbol, t in tickers.items()
        if symbol.endswith("/USDT") and t.get("quoteVolume")
    ]
    usdt_pairs.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in usdt_pairs[:limit]]


def compute_indicators(ohlcv: list) -> dict | None:
    if not ohlcv or len(ohlcv) < 55:
        return None
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
    close = df["close"]

    rsi = RSIIndicator(close, window=14).rsi().iloc[-1]
    ema9 = EMAIndicator(close, window=9).ema_indicator().iloc[-1]
    ema21 = EMAIndicator(close, window=21).ema_indicator().iloc[-1]
    ema50 = EMAIndicator(close, window=50).ema_indicator().iloc[-1]
    macd_hist = MACD(close).macd_diff().iloc[-1]

    price = close.iloc[-1]
    if ema9 > ema21 > ema50 and price > ema9:
        trend = "strong_bullish"
    elif ema9 > ema21:
        trend = "bullish"
    elif ema9 < ema21 < ema50 and price < ema9:
        trend = "strong_bearish"
    elif ema9 < ema21:
        trend = "bearish"
    else:
        trend = "sideways"

    resistance = df["high"].tail(50).max()
    support = df["low"].tail(50).min()

    return {
        "price": float(price), "rsi": round(float(rsi), 2),
        "ema9": float(ema9), "ema21": float(ema21), "ema50": float(ema50),
        "macd_hist": float(macd_hist), "trend": trend,
        "resistance": float(resistance), "support": float(support),
    }


def build_signal(symbol: str, ind: dict, settings: Settings) -> dict | None:
    score = 0
    direction = None

    if ind["rsi"] < settings.rsi_oversold + 10 and ind["trend"] in ("bullish", "strong_bullish"):
        direction = "LONG"
        score += 30
        if ind["rsi"] < settings.rsi_oversold:
            score += 20
        if ind["macd_hist"] > 0:
            score += 25
        if ind["trend"] == "strong_bullish":
            score += 25

    elif ind["rsi"] > settings.rsi_overbought - 10 and ind["trend"] in ("bearish", "strong_bearish"):
        direction = "SHORT"
        score += 30
        if ind["rsi"] > settings.rsi_overbought:
            score += 20
        if ind["macd_hist"] < 0:
            score += 25
        if ind["trend"] == "strong_bearish":
            score += 25

    if direction is None or score < settings.min_confidence:
        return None

    price = ind["price"]
    range_size = max(ind["resistance"] - ind["support"], price * 0.01)

    if direction == "LONG":
        stop_loss = max(ind["support"], price - range_size * 0.25)
        tp1 = price + range_size * 0.5
        tp2 = ind["resistance"]
    else:
        stop_loss = min(ind["resistance"], price + range_size * 0.25)
        tp1 = price - range_size * 0.5
        tp2 = ind["support"]

    risk = abs(price - stop_loss) or 1e-9
    reward = abs(tp1 - price)
    rr = round(reward / risk, 2)
    if rr < 1.2:
        return None

    return {
        "symbol": symbol, "direction": direction, "confidence": min(score, 100),
        "entry": round(price, 6), "stop_loss": round(stop_loss, 6),
        "take_profit_1": round(tp1, 6), "take_profit_2": round(tp2, 6),
        "risk_reward": rr, "rsi": ind["rsi"], "trend": ind["trend"],
    }


def run_scan():
    """Yahi function bot ka 'dimaag' hai — har X minute mein background mein chalta hai."""
    db = SessionLocal()
    try:
        settings = db.query(Settings).first()
        if not settings or not settings.scanner_enabled:
            print("[scanner] disabled — skipping this cycle")
            return

        symbols = get_top_usdt_symbols(settings.top_n_coins)
        print(f"[scanner] scanning {len(symbols)} coins...")
        found = 0

        for symbol in symbols:
            try:
                ohlcv = binance.fetch_ohlcv(symbol, timeframe="4h", limit=200)
                ind = compute_indicators(ohlcv)
                if not ind:
                    continue
                sig = build_signal(symbol, ind, settings)
                if sig:
                    exists = db.query(Signal).filter(
                        Signal.symbol == symbol,
                        Signal.direction == sig["direction"],
                        Signal.status == "active",
                    ).first()
                    if not exists:
                        db.add(Signal(**sig, status="active", created_by="system"))
                        found += 1
            except Exception as e:
                print(f"[scanner] skipped {symbol}: {e}")
                continue

        db.commit()
        print(f"[scanner] done — {found} new signals")
    finally:
        db.close()


# --------------------------------------------------------------------------
# 5. AUTH HELPERS
# --------------------------------------------------------------------------
security = HTTPBearer()


def create_token(email: str) -> str:
    payload = {"sub": email, "exp": datetime.utcnow() + timedelta(hours=12)}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def require_admin(creds: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)) -> User:
    try:
        payload = jwt.decode(creds.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        email = payload.get("sub")
    except JWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token")
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found")
    return user


# --------------------------------------------------------------------------
# 6. FASTAPI APP + ROUTES
# --------------------------------------------------------------------------
app = FastAPI(title="Crypto Signal Platform")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    seed_defaults()
    scheduler = BackgroundScheduler()
    settings_db = SessionLocal()
    interval = settings_db.query(Settings).first().scan_interval_minutes
    settings_db.close()
    scheduler.add_job(run_scan, "interval", minutes=interval, id="scan_job", next_run_time=datetime.utcnow())
    scheduler.start()
    app.state.scheduler = scheduler


# ---- Public endpoints ----

@app.get("/api/signals/active")
def active_signals(db: Session = Depends(get_db)):
    rows = db.query(Signal).filter(Signal.status == "active").order_by(Signal.created_at.desc()).limit(100).all()
    return [
        {
            "id": r.id, "symbol": r.symbol, "direction": r.direction, "entry": r.entry,
            "stop_loss": r.stop_loss, "take_profit_1": r.take_profit_1, "take_profit_2": r.take_profit_2,
            "risk_reward": r.risk_reward, "confidence": r.confidence, "rsi": r.rsi, "trend": r.trend,
            "created_at": r.created_at.isoformat(),
        } for r in rows
    ]


@app.get("/api/coins/{symbol}/analysis")
def coin_analysis(symbol: str):
    pair = f"{symbol.upper()}/USDT"
    try:
        ohlcv = binance.fetch_ohlcv(pair, timeframe="1h", limit=200)
    except Exception:
        raise HTTPException(404, f"{symbol} Binance par USDT pair ke sath available nahi hai")

    ind = compute_indicators(ohlcv)
    if not ind:
        raise HTTPException(404, "Kaafi data nahi mila is coin ke liye")

    votes = sum([
        ind["trend"] in ("bullish", "strong_bullish"),
        ind["macd_hist"] > 0,
        ind["rsi"] < 60,
        ind["price"] > ind["ema50"],
    ])
    forecast = "UP" if votes >= 3 else "DOWN" if votes <= 1 else "NEUTRAL"

    return {"symbol": symbol.upper(), **ind, "forecast": forecast}


class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/api/auth/login")
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == payload.email).first()
    if not user or not pwd_context.verify(payload.password, user.password_hash):
        raise HTTPException(401, "Email ya password galat hai")
    return {"access_token": create_token(user.email), "token_type": "bearer"}


# ---- Admin endpoints (JWT required) ----

@app.get("/api/admin/settings")
def get_settings(db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    s = db.query(Settings).first()
    return {
        "rsi_oversold": s.rsi_oversold, "rsi_overbought": s.rsi_overbought,
        "min_confidence": s.min_confidence, "scan_interval_minutes": s.scan_interval_minutes,
        "top_n_coins": s.top_n_coins, "scanner_enabled": s.scanner_enabled,
    }


class SettingsUpdate(BaseModel):
    rsi_oversold: float | None = None
    rsi_overbought: float | None = None
    min_confidence: float | None = None
    scan_interval_minutes: int | None = None
    top_n_coins: int | None = None


@app.put("/api/admin/settings")
def update_settings(payload: SettingsUpdate, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    s = db.query(Settings).first()
    for field, value in payload.dict(exclude_none=True).items():
        setattr(s, field, value)
    s.updated_at = datetime.utcnow()
    db.commit()
    db.add(AdminLog(action="settings_update", details=payload.dict(exclude_none=True)))
    db.commit()
    return {"status": "updated"}


@app.post("/api/admin/scanner/toggle")
def toggle_scanner(enabled: bool, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    s = db.query(Settings).first()
    s.scanner_enabled = enabled
    db.commit()
    db.add(AdminLog(action="scanner_toggle", details={"enabled": enabled}))
    db.commit()
    return {"scanner_enabled": enabled}


@app.post("/api/admin/scan-now")
def scan_now(admin: User = Depends(require_admin)):
    threading.Thread(target=run_scan, daemon=True).start()
    return {"status": "scan_triggered"}


@app.delete("/api/admin/signals/{signal_id}")
def delete_signal(signal_id: int, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    db.query(Signal).filter(Signal.id == signal_id).delete()
    db.commit()
    db.add(AdminLog(action="signal_deleted", details={"id": signal_id}))
    db.commit()
    return {"status": "deleted"}


class ManualSignal(BaseModel):
    symbol: str
    direction: str
    entry: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float | None = None
    confidence: float = 100


@app.post("/api/admin/signals/push")
def push_signal(payload: ManualSignal, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    risk = abs(payload.entry - payload.stop_loss) or 1e-9
    reward = abs(payload.take_profit_1 - payload.entry)
    sig = Signal(
        symbol=payload.symbol.upper(), direction=payload.direction.upper(),
        entry=payload.entry, stop_loss=payload.stop_loss,
        take_profit_1=payload.take_profit_1, take_profit_2=payload.take_profit_2,
        risk_reward=round(reward / risk, 2), confidence=payload.confidence,
        status="active", created_by=admin.email,
    )
    db.add(sig)
    db.commit()
    db.add(AdminLog(action="manual_signal_pushed", details={"symbol": payload.symbol}))
    db.commit()
    return {"status": "pushed", "id": sig.id}


@app.get("/api/admin/logs")
def get_logs(db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    rows = db.query(AdminLog).order_by(AdminLog.created_at.desc()).limit(50).all()
    return [{"action": r.action, "details": r.details, "created_at": r.created_at.isoformat()} for r in rows]


@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
