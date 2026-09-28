"""
QUOTEX BOT PRO - API-QUOTEX (Playwright SSID - Real + Demo)
Run: python yash_V12_bot.py
"""

import os
import time
import threading
import asyncio
import concurrent.futures
from datetime import datetime

from flask import Flask, jsonify, request
import pandas as pd
import numpy as np

try:
    from api_quotex import AsyncQuotexClient, get_ssid
    API_QUOTEX = True
    print("✅ API-Quotex loaded")
except ImportError as e:
    API_QUOTEX = False
    print(f"❌ API-Quotex not installed: {e}")

app = Flask(__name__)
app.secret_key = 'qx-2026'
PORT = 7771


# ============================================================
# ASYNC EVENT LOOP
# ============================================================
class Loop:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self._run, daemon=True).start()
        while not self.loop.is_running():
            time.sleep(0.001)

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self, coro, timeout=120):
        fut = None
        try:
            fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
            return fut.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            if fut:
                try: fut.cancel()
                except: pass
            print(f"⚠️ Timeout after {timeout}s")
            return None
        except Exception as e:
            print(f"⚠️ Loop error: {e}")
            return None


loop = Loop()


# ============================================================
# ASSETS
# ============================================================
ASSETS = [
    ('EUR/USD', 'forex', False, 'EURUSD'), ('GBP/USD', 'forex', False, 'GBPUSD'),
    ('USD/JPY', 'forex', False, 'USDJPY'), ('AUD/USD', 'forex', False, 'AUDUSD'),
    ('USD/CAD', 'forex', False, 'USDCAD'), ('USD/CHF', 'forex', False, 'USDCHF'),
    ('NZD/USD', 'forex', False, 'NZDUSD'), ('EUR/GBP', 'forex', False, 'EURGBP'),
    ('EUR/JPY', 'forex', False, 'EURJPY'), ('GBP/JPY', 'forex', False, 'GBPJPY'),
    ('AUD/JPY', 'forex', False, 'AUDJPY'), ('CHF/JPY', 'forex', False, 'CHFJPY'),
    ('EUR/AUD', 'forex', False, 'EURAUD'), ('EUR/CHF', 'forex', False, 'EURCHF'),
    ('GBP/AUD', 'forex', False, 'GBPAUD'), ('AUD/CAD', 'forex', False, 'AUDCAD'),
    ('EUR/CAD', 'forex', False, 'EURCAD'), ('GBP/CAD', 'forex', False, 'GBPCAD'),
    ('GBP/CHF', 'forex', False, 'GBPCHF'), ('AUD/CHF', 'forex', False, 'AUDCHF'),
    ('NZD/CAD', 'forex', False, 'NZDCAD'), ('AUD/NZD', 'forex', False, 'AUDNZD'),
    ('EUR/NZD', 'forex', False, 'EURNZD'), ('GBP/NZD', 'forex', False, 'GBPNZD'),
    ('CAD/JPY', 'forex', False, 'CADJPY'), ('CAD/CHF', 'forex', False, 'CADCHF'),
    ('NZD/JPY', 'forex', False, 'NZDJPY'),
    ('USD/ARS (OTC)', 'forex', True, 'USDARS_otc'), ('USD/EGP (OTC)', 'forex', True, 'USDEGP_otc'),
    ('USD/IDR (OTC)', 'forex', True, 'USDIDR_otc'), ('USD/COP (OTC)', 'forex', True, 'USDCOP_otc'),
    ('USD/PKR (OTC)', 'forex', True, 'USDPKR_otc'), ('USD/BRL (OTC)', 'forex', True, 'USDBRL_otc'),
    ('USD/NGN (OTC)', 'forex', True, 'USDNGN_otc'), ('USD/BDT (OTC)', 'forex', True, 'USDBDT_otc'),
    ('USD/DZD (OTC)', 'forex', True, 'USDDZD_otc'), ('USD/ZAR (OTC)', 'forex', True, 'USDZAR_otc'),
    ('USD/MXN (OTC)', 'forex', True, 'USDMXN_otc'), ('USD/PHP (OTC)', 'forex', True, 'USDPHP_otc'),
    ('USD/INR (OTC)', 'forex', True, 'USDINR_otc'), ('USD/TRY (OTC)', 'forex', True, 'USDTRY_otc'),
    ('USD/SGD (OTC)', 'forex', True, 'USDSGD_otc'), ('USD/HKD (OTC)', 'forex', True, 'USDHKD_otc'),
    ('USD/RUB (OTC)', 'forex', True, 'USDRUB_otc'), ('USD/KRW (OTC)', 'forex', True, 'USDKRW_otc'),
    ('Bitcoin (OTC)', 'crypto', True, 'BTCUSD_otc'), ('Ethereum (OTC)', 'crypto', True, 'ETHUSD_otc'),
    ('Ripple (OTC)', 'crypto', True, 'XRPUSD_otc'), ('Litecoin (OTC)', 'crypto', True, 'LTCUSD_otc'),
    ('Solana (OTC)', 'crypto', True, 'SOLUSD_otc'), ('Polkadot (OTC)', 'crypto', True, 'DOTUSD_otc'),
    ('Chainlink (OTC)', 'crypto', True, 'LINKUSD_otc'), ('Binance Coin (OTC)', 'crypto', True, 'BNBUSD_otc'),
    ('Dogecoin (OTC)', 'crypto', True, 'DOGEUSD_otc'), ('Cardano (OTC)', 'crypto', True, 'ADAUSD_otc'),
    ('Avalanche (OTC)', 'crypto', True, 'AVAXUSD_otc'), ('Toncoin (OTC)', 'crypto', True, 'TONUSD_otc'),
    ('Ethereum Classic (OTC)', 'crypto', True, 'ETCUSD_otc'), ('Cosmos (OTC)', 'crypto', True, 'ATOMUSD_otc'),
    ('Dash (OTC)', 'crypto', True, 'DASHUSD_otc'), ('Zcash (OTC)', 'crypto', True, 'ZECUSD_otc'),
    ('Bitcoin Cash (OTC)', 'crypto', True, 'BCHUSD_otc'), ('Trump (OTC)', 'crypto', True, 'TRUMPUSD_otc'),
    ('Axie Infinity (OTC)', 'crypto', True, 'AXSUSD_otc'), ('Polygon (OTC)', 'crypto', True, 'MATICUSD_otc'),
    ('Uniswap (OTC)', 'crypto', True, 'UNIUSD_otc'),
    ('Gold (OTC)', 'commodity', True, 'XAUUSD_otc'), ('Silver (OTC)', 'commodity', True, 'XAGUSD_otc'),
    ('USCrude (OTC)', 'commodity', True, 'USOIL_otc'), ('UKBrent (OTC)', 'commodity', True, 'UKOIL_otc'),
    ('S&P 500', 'stocks', False, 'SP500'), ('NASDAQ', 'stocks', False, 'NASDAQ'),
    ('Dow Jones', 'stocks', False, 'DJ30'), ('FTSE 100', 'stocks', False, 'FTSE100'),
    ('CAC 40', 'stocks', False, 'CAC40'), ('Nikkei 225', 'stocks', False, 'NIKKEI225'),
    ('DAX 40', 'stocks', False, 'DAX40'), ('NIFTY 50', 'stocks', False, 'NIFTY50'),
    ('Sensex', 'stocks', False, 'SENSEX'), ('Hang Seng', 'stocks', False, 'HANG SENG'),
    ('Shanghai Comp', 'stocks', False, 'SHANGHAI'), ('S&P/ASX 200', 'stocks', False, 'ASX200'),
    ('FTSE China A50', 'stocks', False, 'FTSEA50'), ('Hong Kong 50', 'stocks', False, 'HK50'),
    ('IBEX 35', 'stocks', False, 'IBEX35'), ('EURO STOXX 50', 'stocks', False, 'STOXX50'),
]


def get_assets():
    return [{'id': str(i), 'name': n, 'type': t, 'is_otc': o, 'code': c}
            for i, (n, t, o, c) in enumerate(ASSETS, 1)]


# ============================================================
# BOT CLASS
# ============================================================
class Bot:
    def __init__(self):
        self.client = None
        self.connected = False
        self.assets = get_assets()
        self.real_codes = {}
        self.data = {}
        self.count = 0
        self.last_error = None

    def connect(self, email, password):
        if not API_QUOTEX:
            self.last_error = "API-Quotex not installed"
            return False

        print("🔗 Fetching SSID via Playwright (Cloudflare bypass)...")

        try:
            ssid_info = loop.run(get_ssid(email=email, password=password), timeout=180)
            if not ssid_info:
                self.last_error = "SSID fetch failed (browser timeout or login failed)"
                return False

            print(f"📋 SSID info keys: {list(ssid_info.keys()) if isinstance(ssid_info, dict) else 'not a dict'}")

            # ============================================================
            # FIX: Real SSID pehle try karo, phir Demo
            # ============================================================
            ssid = None
            is_demo = True

            if isinstance(ssid_info, dict):
                # Real SSID pehle
                if ssid_info.get("real"):
                    ssid = ssid_info["real"]
                    is_demo = False
                    print("✅ Using REAL SSID")
                # Phir demo
                elif ssid_info.get("demo"):
                    ssid = ssid_info["demo"]
                    is_demo = True
                    print("✅ Using DEMO SSID")
                # Agar tuple mila toh
                elif len(ssid_info) == 2:
                    ssid, is_demo = ssid_info
                    print(f"✅ Using SSID (tuple) is_demo={is_demo}")

            if not ssid:
                self.last_error = f"No SSID returned. Keys: {list(ssid_info.keys()) if isinstance(ssid_info, dict) else ssid_info}"
                print(f"❌ {self.last_error}")
                return False

            print(f"✅ SSID obtained. Connecting to WebSocket (demo={is_demo})...")

            self.client = AsyncQuotexClient(ssid=ssid, is_demo=is_demo)
            ok = loop.run(self.client.connect(), timeout=30)

            if ok:
                self.connected = True
                self.last_error = None
                print(f"✅ Connected via API-Quotex (demo={is_demo})")

                try:
                    assets = loop.run(self.client.get_assets(), timeout=15)
                    if assets:
                        self._cache_real_codes(assets)
                except Exception as e:
                    print(f"⚠️ get_assets error: {e}")

                return True
            else:
                self.last_error = "WebSocket connect failed"
                print(f"❌ {self.last_error}")
                return False
        except Exception as e:
            self.last_error = str(e)
            print(f"❌ Connect error: {e}")
            return False

    def _cache_real_codes(self, assets):
        try:
            for a in assets:
                name = (a.get('name') or '').strip()
                symbol = a.get('asset') or a.get('symbol') or a.get('id') or ''
                if name and symbol:
                    self.real_codes[name.upper()] = str(symbol)
                    self.real_codes[name.upper().replace(' (OTC)', '')] = str(symbol)
                    self.real_codes[name.upper().replace('/', '')] = str(symbol)
            print(f"✅ Cached {len(self.real_codes)} real codes")
        except Exception as e:
            print(f"⚠️ Cache error: {e}")

    def get_data(self, asset_id, period=60, count=100):
        if not self.connected or not self.client:
            return None

        code, name = 'EURUSD', 'EUR/USD'
        for a in self.assets:
            if a['id'] == asset_id:
                code, name = a['code'], a['name']
                break

        if name.upper() in self.real_codes:
            code = self.real_codes[name.upper()]

        print(f"📊 Fetching {name} ({code})...")
        try:
            candles = loop.run(self.client.get_candles(code, count), timeout=20)
            if candles and len(candles) >= 10:
                df = pd.DataFrame(candles)
                if 'timestamp' not in df.columns and 'time' in df.columns:
                    df['timestamp'] = df['time']

                for col in ('timestamp', 'open', 'high', 'low', 'close', 'volume'):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                df = df.dropna(subset=['close']).reset_index(drop=True)
                if len(df) < 10:
                    return None

                self.data[asset_id] = df
                print(f"✅ {name}: {len(candles)} candles")
                return df
            else:
                print(f"⚠️ {name}: No candles")
                return None
        except Exception as e:
            print(f"❌ {name}: {e}")
            return None

    def analyze(self, asset_id):
        if asset_id not in self.data:
            return None

        df = self.data[asset_id]
        if df is None or len(df) < 15:
            return None

        c = df['close'].values
        h = df['high'].values
        l = df['low'].values
        cp = float(c[-1])

        def rsi(data, p):
            if len(data) < p + 1: return 50
            d = pd.Series(data).diff()
            g = d.where(d > 0, 0).rolling(p).mean()
            ls = (-d.where(d < 0, 0)).rolling(p).mean()
            if ls.iloc[-1] == 0: return 100
            return float(100 - 100 / (1 + g.iloc[-1] / ls.iloc[-1]))

        rsi14 = rsi(c, 14)

        def macd(data):
            if len(data) < 26: return 0, 0
            p = pd.Series(data)
            ef = p.ewm(span=12, adjust=False).mean()
            es = p.ewm(span=26, adjust=False).mean()
            m = ef - es
            s = m.ewm(span=9, adjust=False).mean()
            return float(m.iloc[-1]), float(s.iloc[-1])

        macd_line, macd_sig = macd(c)

        def bb(data, p=20, std=2):
            if len(data) < p: return cp, cp, cp
            s = pd.Series(data)
            m = s.rolling(p).mean()
            sd = s.rolling(p).std()
            return float((m + sd * std).iloc[-1]), float(m.iloc[-1]), float((m - sd * std).iloc[-1])

        bbu, bbm, bbl = bb(c)

        bs, ss = 0, 0
        confs = []

        if rsi14 < 30:
            bs += 2
            confs.append({'indicator': 'RSI-14', 'signal': 'BUY', 'value': round(rsi14, 2)})
        elif rsi14 > 70:
            ss += 2
            confs.append({'indicator': 'RSI-14', 'signal': 'SELL', 'value': round(rsi14, 2)})
        else:
            confs.append({'indicator': 'RSI-14', 'signal': 'NEUTRAL', 'value': round(rsi14, 2)})

        if macd_line > macd_sig:
            bs += 2
            confs.append({'indicator': 'MACD', 'signal': 'BUY'})
        else:
            ss += 2
            confs.append({'indicator': 'MACD', 'signal': 'SELL'})

        if cp < bbl:
            bs += 2
            confs.append({'indicator': 'BB-Lower', 'signal': 'BUY'})
        elif cp > bbu:
            ss += 2
            confs.append({'indicator': 'BB-Upper', 'signal': 'SELL'})
        else:
            confs.append({'indicator': 'BB', 'signal': 'NEUTRAL'})

        sma10 = float(pd.Series(c).rolling(10).mean().iloc[-1]) if len(c) >= 10 else cp
        sma20 = float(pd.Series(c).rolling(20).mean().iloc[-1]) if len(c) >= 20 else cp

        if sma10 > sma20:
            bs += 2
            trend = 'UPTREND'
            confs.append({'indicator': 'SMA-Cross', 'signal': 'BUY'})
        else:
            ss += 2
            trend = 'DOWNTREND'
            confs.append({'indicator': 'SMA-Cross', 'signal': 'SELL'})

        total = bs + ss
        conf = (max(bs, ss) / total * 100) if total > 0 else 50
        conf = max(40, min(95, conf))

        signal = 'BUY' if bs >= ss else 'SELL'

        return {
            'signal': signal,
            'strength': float(conf / 100),
            'confidence': int(conf),
            'buy_score': round(bs, 2),
            'sell_score': round(ss, 2),
            'confirmations': confs,
            'trend': trend,
            'candles_used': len(df),
            'price': {
                'current': cp,
                'high': float(max(h)),
                'low': float(min(l)),
                'change': float(((cp - c[0]) / c[0]) * 100) if c[0] != 0 else 0
            }
        }

    def generate(self, asset_id, hour, minute):
        print(f"\n🔄 Generating signal for asset {asset_id}")

        df = self.get_data(asset_id, 60, 100)
        if df is None or len(df) < 15:
            return None

        analysis = self.analyze(asset_id)
        if not analysis:
            return None

        self.count += 1
        return {
            **analysis,
            'time': f"{hour:02d}:{minute:02d}",
            'signal_number': self.count,
            'data_source': 'API-Quotex (Playwright)',
            'timestamp': datetime.now().isoformat()
        }


bot = Bot()


# ============================================================
# ROUTES
# ============================================================
@app.route('/')
def index():
    return DASHBOARD_HTML


@app.route('/api/connect', methods=['POST'])
def api_connect():
    d = request.json or {}
    ok = bot.connect(d.get('email'), d.get('password'))
    return jsonify({'success': ok, 'error': bot.last_error})


@app.route('/api/assets')
def api_assets():
    return jsonify(bot.assets)


@app.route('/api/data/<aid>')
def api_data(aid):
    df = bot.get_data(aid, 60, 100)
    if df is not None:
        return jsonify(df.to_dict('records'))
    return jsonify([])


@app.route('/api/generate/<aid>')
def api_generate(aid):
    hour = request.args.get('hour', 10, type=int)
    minute = request.args.get('minute', 3, type=int)
    r = bot.generate(aid, hour, minute)
    if r:
        return jsonify(r)
    return jsonify({'error': 'No data available'})


# ============================================================
# DASHBOARD HTML
# ============================================================
DASHBOARD_HTML = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Quotex Bot Pro</title>
<style>
body{font-family:sans-serif;background:#0a0a1a;color:#fff;padding:10px}
.card{background:#14143c;border-radius:12px;padding:15px;margin:10px 0}
input,button{padding:10px;margin:5px;border-radius:8px;border:1px solid #333}
button{background:#8e2de2;color:#fff;cursor:pointer;padding:10px 20px;font-weight:bold}
.signal-box{padding:20px;border-radius:12px;text-align:center;margin:10px 0;font-size:24px;font-weight:bold}
.signal-box.buy{background:#0f3;color:#000}
.signal-box.sell{background:#f36;color:#fff}
.signal-box.neutral{background:#333;color:#fff}
</style></head><body>
<h1>🚀 Quotex Bot Pro (API-Quotex)</h1>
<div class="card">
<input id="email" placeholder="Email" style="width:250px">
<input id="password" type="password" placeholder="Password" style="width:250px">
<button onclick="connect()">🔗 Connect</button>
<span id="status" style="margin-left:10px"></span>
</div>
<div class="card">
<select id="asset" style="padding:10px;min-width:200px"></select>
<button onclick="generate()">🎯 Generate Signal</button>
</div>
<div id="signal" class="signal-box neutral">Waiting for connection...</div>
<script>
fetch('/api/assets').then(r=>r.json()).then(a=>{
  document.getElementById('asset').innerHTML = a.map(x=>`<option value="${x.id}">${x.name}</option>`).join('');
});
async function connect(){
  document.getElementById('status').textContent = '⏳ Connecting (browser khul sakta hai, 60-90 sec wait)...';
  const r = await fetch('/api/connect',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({email:document.getElementById('email').value,password:document.getElementById('password').value})});
  const d = await r.json();
  document.getElementById('status').textContent = d.success ? '✅ Connected' : '❌ '+(d.error||'Failed');
}
async function generate(){
  const id = document.getElementById('asset').value;
  const r = await fetch(`/api/generate/${id}?hour=10&minute=3`);
  const d = await r.json();
  const box = document.getElementById('signal');
  if(d.error){box.className='signal-box neutral'; box.textContent = '❌ '+d.error; return;}
  box.className = 'signal-box '+d.signal.toLowerCase();
  box.textContent = d.signal + ' (' + d.confidence + '%)';
}
</script></body></html>"""


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print("=" * 60)
    print("🚀 QUOTEX BOT PRO - API-QUOTEX (Real + Demo)")
    print("=" * 60)
    print(f"📊 Total Assets: {len(bot.assets)}")
    print(f"📊 API-Quotex: {'✅ Available' if API_QUOTEX else '❌ Not installed'}")
    print(f"🔐 Cloudflare Bypass: Playwright Browser")
    print(f"🌐 Dashboard: http://0.0.0.0:{PORT}")
    print("=" * 60)

    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', PORT)), debug=False)
