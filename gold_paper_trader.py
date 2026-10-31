"""
GOLD (XAUT) options LIVE PAPER TRADER  -  Delta Exchange India
=============================================================

Backtest se nikli strategy ko live market pe paper (nakli paise) me chalata hai:
  - Sirf aaj expire hone wale gold options.
  - Entry window: expiry se MAX_MINS_LEFT (default 60) minute pehle se.
  - Strike spot se +/- BAND (default $10) ke andar, CE aur PE dono.
  - Agar BEST ASK ENTRY_MIN aur ENTRY_MAX ke beech ho (dono shaamil) aur ask pe quantity ho -> paper BUY at ask.
    (har contract me ek hi entry)
  - Exit: BEST BID >= TARGET_X x entry -> paper SELL at bid.
    Warna expiry pe settle (OTM = 0, ITM = intrinsic, last spot se).

Backtest se farak: yahan fills asli order book (bid/ask) pe hain, last-traded price pe nahi.
App yeh bhi ginti hai ki kitni baar "last price < $0.10" tha lekin ask usse upar tha
-> isse pata chalta hai ki backtest wale sasta fill asli me milte the ya nahi.

Run:
  pip install requests
  python gold_paper_trader.py
  Browser me kholo:  http://localhost:8765

Options:
  python gold_paper_trader.py --band 10 --max-mins-left 60 --entry-max 0.10 --target-x 5
  python gold_paper_trader.py --target-x 1000      # target band, sirf expiry tak hold

Files (isi folder me banti hain):
  paper_state.json   - open positions + already-traded contracts (restart pe wapas load)
  paper_trades.csv   - har band hua trade
  paper_log.txt      - event log

Yeh sirf paper trading hai. Koi asli order place nahi hota, API key ki zaroorat nahi.
"""
import argparse, csv, datetime as dt, json, os, threading, time, traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

BASE = "https://api.india.delta.exchange/v2"
GOLD = {"XAUT", "PAXG", "XAU", "GOLD"}
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
STATE_FILE, TRADES_FILE, LOG_FILE = "paper_state.json", "paper_trades.csv", "paper_log.txt"
TRADE_COLS = ["expiry", "symbol", "type", "strike", "entry_time", "entry_px", "ask_size",
              "spot_at_entry", "mins_left_at_entry", "exit_time", "exit_px", "exit_reason",
              "qty", "fees", "pnl"]

S = requests.Session()


# ------------------------------------------------------------------ helpers
def fnum(x, default=None):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def ist(t):
    return t.astimezone(IST).strftime("%d-%b %H:%M:%S")


def parse_iso(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def is_gold_symbol(sym, und=""):
    sym, und = (sym or "").upper(), (und or "").upper()
    return und in GOLD or any(f"-{g}-" in sym for g in GOLD)


def get_json(path, params=None):
    r = S.get(f"{BASE}{path}", params=params, timeout=15)
    r.raise_for_status()
    return r.json()


# ------------------------------------------------------------------ market data
class Market:
    """Delta se live gold option tickers aur unke expiry times."""

    def __init__(self, expiry_utc_hour, expiry_utc_min):
        self.expiry_by_symbol = {}
        self.products_loaded_at = 0
        self.exp_h, self.exp_m = expiry_utc_hour, expiry_utc_min

    def refresh_products(self):
        found = {}
        after = None
        for _ in range(20):
            params = {"contract_types": "call_options,put_options", "states": "live", "page_size": 500}
            if after:
                params["after"] = after
            js = get_json("/products", params)
            for p in js.get("result") or []:
                und = (p.get("underlying_asset") or {}).get("symbol", "")
                if is_gold_symbol(p.get("symbol"), und) and p.get("settlement_time"):
                    found[p["symbol"]] = parse_iso(p["settlement_time"])
            after = (js.get("meta") or {}).get("after")
            if not after:
                break
        self.expiry_by_symbol = found
        self.products_loaded_at = time.time()

    def expiry_of(self, sym):
        if sym in self.expiry_by_symbol:
            return self.expiry_by_symbol[sym]
        # fallback: symbol ke DDMMYY se, configured UTC time pe (default 16:00 UTC = 21:30 IST)
        try:
            d = dt.datetime.strptime(sym.split("-")[-1], "%d%m%y")
            return d.replace(hour=self.exp_h, minute=self.exp_m, tzinfo=dt.timezone.utc)
        except ValueError:
            return None

    def snapshot(self):
        """Return (spot, list of option dicts)."""
        if time.time() - self.products_loaded_at > 600 or not self.expiry_by_symbol:
            try:
                self.refresh_products()
            except Exception as e:
                log(f"products refresh fail: {e}")
        js = get_json("/tickers", {"contract_types": "call_options,put_options"})
        opts, spots = [], []
        for t in js.get("result") or []:
            sym = t.get("symbol", "")
            if not is_gold_symbol(sym, t.get("underlying_asset_symbol", "")):
                continue
            q = t.get("quotes") or {}
            spot = fnum(t.get("spot_price"))
            if spot:
                spots.append(spot)
            ctype = t.get("contract_type") or ("call_options" if sym.startswith("C-") else "put_options")
            opts.append({
                "symbol": sym,
                "kind": "C" if ctype == "call_options" else "P",
                "strike": fnum(t.get("strike_price"), fnum(sym.split("-")[2], 0) if sym.count("-") >= 3 else 0),
                "bid": fnum(q.get("best_bid")),
                "ask": fnum(q.get("best_ask")),
                "bid_size": fnum(q.get("bid_size"), 0),
                "ask_size": fnum(q.get("ask_size"), 0),
                "last": fnum(t.get("close")),
                "mark": fnum(t.get("mark_price")),
                "expiry": self.expiry_of(sym),
            })
        spot = sorted(spots)[len(spots) // 2] if spots else None
        return spot, opts


# ------------------------------------------------------------------ logging
_log_lines = []
_log_lock = threading.Lock()


TG = {"token": None, "chat": None}


def telegram(msg):
    if not (TG["token"] and TG["chat"]):
        return
    def _send():
        try:
            S.post(f"https://api.telegram.org/bot{TG['token']}/sendMessage",
                   data={"chat_id": TG["chat"], "text": msg}, timeout=10)
        except Exception:
            pass
    threading.Thread(target=_send, daemon=True).start()


def log(msg, alert=False):
    if alert:
        telegram("Gold paper: " + msg)
    line = f"{ist(now_utc())}  {msg}"
    print(line, flush=True)
    with _log_lock:
        _log_lines.append(line)
        del _log_lines[:-200]
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass


# ------------------------------------------------------------------ strategy engine
class Engine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.open = {}          # symbol -> position dict
        self.traded = set()     # symbols already entered once
        self.closed = []        # closed trades (dicts)
        self.missed = {}        # symbol -> info when last<entry_max but ask too high / no ask
        self.spot = None
        self.spot_by_expiry = {}  # expiry iso -> last spot seen before expiry
        self.chain = []
        self.next_expiry = None
        self.last_update = None
        self.last_error = None
        self.load()

    # ---------- persistence
    def load(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE) as f:
                    st = json.load(f)
                self.open = st.get("open", {})
                self.traded = set(st.get("traded", []))
                self.spot_by_expiry = st.get("spot_by_expiry", {})
                self.missed = st.get("missed", {})
            except Exception as e:
                log(f"state load fail: {e}")
        if os.path.exists(TRADES_FILE):
            with open(TRADES_FILE, newline="") as f:
                self.closed = list(csv.DictReader(f))
        if self.open:
            log(f"{len(self.open)} open paper positions wapas load ki")

    def save(self):
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"open": self.open, "traded": sorted(self.traded),
                       "spot_by_expiry": self.spot_by_expiry, "missed": self.missed}, f, indent=1)
        os.replace(tmp, STATE_FILE)

    def record_close(self, pos, exit_time, exit_px, reason):
        c = self.cfg
        fees = c.fee_per_side * (2 if reason == "TARGET" else 1) * c.qty
        pnl = (exit_px - pos["entry_px"]) * c.qty - fees
        row = {**{k: pos.get(k) for k in TRADE_COLS}, "exit_time": ist(exit_time),
               "exit_px": round(exit_px, 4), "exit_reason": reason,
               "qty": c.qty, "fees": round(fees, 4), "pnl": round(pnl, 4)}
        new = not os.path.exists(TRADES_FILE)
        with open(TRADES_FILE, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=TRADE_COLS)
            if new:
                w.writeheader()
            w.writerow(row)
        self.closed.append(row)
        log(f"EXIT {reason:6s} {pos['symbol']}  entry {pos['entry_px']:.2f} -> {exit_px:.2f}  P&L {pnl:+.3f}",
            alert=True)

    # ---------- one tick
    def step(self, spot, opts, t):
        c = self.cfg
        with self.lock:
            self.spot = spot
            self.last_update = t
            self.last_error = None

            # aaj ki / sabse paas wali expiry
            future = sorted({o["expiry"] for o in opts if o["expiry"] and o["expiry"] > t})
            self.next_expiry = future[0] if future else None
            if spot and self.next_expiry:
                self.spot_by_expiry[self.next_expiry.isoformat()] = spot

            by_sym = {o["symbol"]: o for o in opts}

            # 1) open positions: target ya expiry
            for sym in list(self.open):
                pos = self.open[sym]
                exp = parse_iso(pos["expiry_iso"])
                o = by_sym.get(sym)
                if t >= exp:
                    s = self.spot_by_expiry.get(pos["expiry_iso"]) or spot
                    k = pos["strike"]
                    intrinsic = max(0.0, s - k) if pos["type"] == "CE" else max(0.0, k - s)
                    self.record_close(pos, exp, intrinsic, "EXPIRY")
                    del self.open[sym]
                    continue
                if o and o["bid"] and o["bid_size"] > 0:
                    pos["bid_now"] = o["bid"]
                    if o["bid"] >= pos["entry_px"] * c.target_x:
                        self.record_close(pos, t, o["bid"], "TARGET")
                        del self.open[sym]

            # 2) new entries + option chain for dashboard
            chain = []
            if spot and self.next_expiry:
                exp = self.next_expiry
                mins_left = (exp - t).total_seconds() / 60
                day_tag = exp.strftime("%d%m%y")
                taken_today = sum(1 for sym in self.traded if sym.endswith(day_tag))
                in_window = c.min_mins_left <= mins_left <= c.max_mins_left
                for o in opts:
                    if o["expiry"] != exp:
                        continue
                    dist = abs(o["strike"] - spot)
                    if dist > max(c.band, 30):  # dashboard ke liye thoda wide chain
                        continue
                    status = ""
                    in_band = dist <= c.band
                    ask_ok = (o["ask"] is not None and c.entry_min <= o["ask"] <= c.entry_max
                              and o["ask"] > 0 and o["ask_size"] > 0)
                    if o["symbol"] in self.open:
                        status = "OPEN"
                    elif o["symbol"] in self.traded:
                        status = "done"
                    elif in_band and in_window and ask_ok and (c.max_trades <= 0 or taken_today < c.max_trades):
                        px = o["ask"] + c.extra_slippage
                        self.open[o["symbol"]] = {
                            "expiry": ist(exp), "expiry_iso": exp.isoformat(),
                            "symbol": o["symbol"], "type": "CE" if o["kind"] == "C" else "PE",
                            "strike": o["strike"], "entry_time": ist(t), "entry_px": round(px, 4),
                            "ask_size": o["ask_size"], "spot_at_entry": round(spot, 2),
                            "mins_left_at_entry": round(mins_left, 1), "bid_now": o["bid"],
                        }
                        self.traded.add(o["symbol"])
                        taken_today += 1
                        status = "BOUGHT"
                        log(f"BUY  {o['symbol']} @ {px:.2f} (ask size {o['ask_size']:g})  "
                            f"spot {spot:.2f}  {mins_left:.0f} min left", alert=True)
                    elif in_band and in_window and (c.max_trades <= 0 or taken_today < c.max_trades):
                        cheap_last = o["last"] is not None and c.entry_min <= o["last"] <= c.entry_max and o["last"] > 0
                        if cheap_last and o["symbol"] not in self.missed:
                            self.missed[o["symbol"]] = {"time": ist(t), "last": o["last"], "ask": o["ask"]}
                            log(f"MISS {o['symbol']}: last {o['last']} range me tha lekin ask "
                                f"{o['ask'] if o['ask'] else 'nahi'} (backtest yahan buy kar leta)")
                        status = "watch"
                    chain.append({**o, "expiry": None, "dist": round(o["strike"] - spot, 2),
                                  "in_band": in_band, "status": status})
            self.chain = sorted(chain, key=lambda x: (x["strike"], x["kind"]))
            self.save()

    # ---------- dashboard data
    def state(self):
        c = self.cfg
        with self.lock:
            t = now_utc()
            mins_left = (self.next_expiry - t).total_seconds() / 60 if self.next_expiry else None
            closed = self.closed
            pnl = sum(fnum(r.get("pnl"), 0) for r in closed)
            wins = sum(1 for r in closed if fnum(r.get("pnl"), 0) > 0)
            open_rows = []
            unreal = 0.0
            for p in self.open.values():
                b = p.get("bid_now") or 0
                u = (b - p["entry_px"]) * c.qty
                unreal += u
                open_rows.append({**p, "unreal": round(u, 4)})
            return {
                "now": ist(t), "spot": self.spot,
                "last_update": ist(self.last_update) if self.last_update else None,
                "error": self.last_error,
                "next_expiry": ist(self.next_expiry) if self.next_expiry else None,
                "next_expiry_iso": self.next_expiry.isoformat() if self.next_expiry else None,
                "updated_iso": self.last_update.isoformat() if self.last_update else None,
                "mins_left": mins_left,
                "window": [c.min_mins_left, c.max_mins_left],
                "in_window": mins_left is not None and c.min_mins_left <= mins_left <= c.max_mins_left,
                "cfg": {"max_trades": c.max_trades, "band": c.band, "entry_min": c.entry_min, "entry_max": c.entry_max, "target_x": c.target_x,
                        "qty": c.qty, "fee": c.fee_per_side, "slip": c.extra_slippage},
                "chain": self.chain, "open": open_rows, "closed": closed[-100:][::-1],
                "stats": {"trades": len(closed), "wins": wins, "pnl": round(pnl, 4),
                          "unreal": round(unreal, 4), "missed": len(self.missed),
                          "capital": round(sum(fnum(r.get("entry_px"), 0) for r in closed) * c.qty, 4)},
                "log": _log_lines[-40:][::-1],
            }


def write_export(engine, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(engine.state(), f, default=str)
    os.replace(tmp, path)


def run_headless(engine, market, cfg):
    """GitHub Actions / server ke liye: bina dashboard server ke chalo, state JSON me likho,
    aur aaj ki expiry settle hone ke baad khud band ho jao."""
    started = time.time()
    target_exp = None
    last_export = 0
    while True:
        t = now_utc()
        try:
            spot, opts = market.snapshot()
            engine.step(spot, opts, t)
        except Exception as e:
            with engine.lock:
                engine.last_error = str(e)
            log(f"data error: {e}")
        with engine.lock:
            nxt = engine.next_expiry
            n_open = len(engine.open)
        if target_exp is None and nxt:
            target_exp = nxt
            log(f"Aaj ki expiry: {ist(target_exp)} IST. Uske settle hone tak chalunga.")
        if cfg.export and time.time() - last_export >= cfg.export_every:
            write_export(engine, cfg.export)
            last_export = time.time()
        if target_exp and t > target_exp + dt.timedelta(minutes=2) and n_open == 0:
            log("Expiry settle ho gayi, aaj ka run khatam.")
            break
        if (time.time() - started) / 60 > cfg.max_runtime:
            log("max runtime pura, band kar raha hoon.")
            break
        mins_left = (target_exp - t).total_seconds() / 60 if target_exp else None
        # window se pehle dheere (har minute), window me tez
        slow = mins_left is not None and mins_left > cfg.max_mins_left + 3
        time.sleep(60 if slow else cfg.interval)
    if cfg.export:
        write_export(engine, cfg.export)


def run_loop(engine, market, interval):
    while True:
        t0 = time.time()
        try:
            spot, opts = market.snapshot()
            engine.step(spot, opts, now_utc())
        except Exception as e:
            with engine.lock:
                engine.last_error = str(e)
            log(f"data error: {e}")
            if engine.cfg.debug:
                traceback.print_exc()
        time.sleep(max(1.0, interval - (time.time() - t0)))


# ------------------------------------------------------------------ dashboard
PAGE = r"""<!doctype html><html lang="hi"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Gold Paper Trader</title>
<style>
:root{--bg:#0f1115;--card:#171a21;--line:#262b36;--fg:#e6e8ee;--mute:#8a93a6;--gold:#e0b44c;--up:#3fbf7f;--dn:#e5636b;--blue:#6aa8ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid var(--line)}
h1{font-size:17px;margin:0;color:var(--gold);letter-spacing:.3px}
.pill{font-size:12px;padding:3px 9px;border-radius:99px;border:1px solid var(--line);color:var(--mute)}
.pill.on{color:#111;background:var(--up);border-color:var(--up)}.pill.err{color:#fff;background:var(--dn);border-color:var(--dn)}
main{padding:16px 18px;display:grid;gap:14px;max-width:1300px;margin:0 auto}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.k{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px}
.k b{display:block;font-size:20px;font-variant-numeric:tabular-nums;margin-top:2px}.k span{color:var(--mute);font-size:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px;overflow:auto}
.card h2{font-size:13px;margin:0 0 8px;color:var(--mute);font-weight:600;text-transform:uppercase;letter-spacing:.6px}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums;font-size:13px}
th,td{padding:5px 8px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--mute);font-weight:500}td:first-child,th:first-child{text-align:left}
tr.band td{background:rgba(224,180,76,.06)}.up{color:var(--up)}.dn{color:var(--dn)}.mute{color:var(--mute)}
.tag{font-size:11px;padding:1px 7px;border-radius:99px;background:var(--line)}
.tag.OPEN,.tag.BOUGHT{background:var(--blue);color:#0b1020}.tag.watch{background:#3a3220;color:var(--gold)}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}@media(max-width:900px){.grid2{grid-template-columns:1fr}}
pre{margin:0;font:12px/1.5 ui-monospace,Consolas,monospace;color:var(--mute);white-space:pre-wrap;max-height:260px;overflow:auto}
.note{color:var(--mute);font-size:12px}
</style></head><body>
<header><h1>GOLD (XAUT) Options · Paper Trader</h1>
<div style="display:flex;gap:8px;flex-wrap:wrap"><span id="win" class="pill">—</span><span id="upd" class="pill">connecting…</span></div></header>
<main>
<div class="kpis">
<div class="k"><span>Gold spot</span><b id="spot">—</b></div>
<div class="k"><span>Next expiry (IST)</span><b id="exp" style="font-size:15px">—</b></div>
<div class="k"><span>Expiry me baaki</span><b id="left">—</b></div>
<div class="k"><span>Realised P&amp;L</span><b id="pnl">—</b></div>
<div class="k"><span>Open P&amp;L (bid pe)</span><b id="unr">—</b></div>
<div class="k"><span>Trades / wins</span><b id="tw">—</b></div>
<div class="k"><span>Missed total (sasta last, mehnga ask)</span><b id="miss">—</b></div>
</div>
<div class="card"><h2>Option chain (aaj ki expiry)</h2><div id="chain" class="note">data aa raha hai…</div>
<p class="note" id="rules"></p></div>
<div class="grid2">
<div class="card"><h2>Open positions</h2><div id="open" class="note">koi nahi</div></div>
<div class="card"><h2>Log</h2><pre id="log"></pre></div>
</div>
<div class="card"><h2>Closed trades</h2><div id="closed" class="note">abhi koi nahi</div></div>
</main>
<script>
const f=(x,d=2)=>x==null||x===''||isNaN(x)?'—':Number(x).toFixed(d);
const cls=v=>v>0?'up':v<0?'dn':'';
function table(rows,cols){if(!rows.length)return null;
 return '<table><tr>'+cols.map(c=>'<th>'+c[0]+'</th>').join('')+'</tr>'+rows.map(r=>'<tr class="'+(r._cls||'')+'">'+cols.map(c=>'<td>'+c[1](r)+'</td>').join('')+'</tr>').join('')+'</table>'}
async function tick(){
 try{const s=await (await fetch('/api/state')).json();
  document.getElementById('spot').textContent=f(s.spot);
  document.getElementById('exp').textContent=s.next_expiry||'—';
  let m=s.mins_left;if(s.next_expiry_iso){m=(new Date(s.next_expiry_iso)-Date.now())/60000;if(m<0)m=0}
  const age=s.updated_iso?(Date.now()-new Date(s.updated_iso))/60000:null;
  if(age!=null&&age>5){s.error=(s.error?s.error+' | ':'')+'data '+Math.round(age)+' min purana (app abhi nahi chal rahi)'}
  document.getElementById('left').textContent=m==null?'—':(m>=60?Math.floor(m/60)+'h '+Math.floor(m%60)+'m':Math.floor(m)+'m '+Math.floor((m*60)%60)+'s');
  s.in_window=m!=null&&m>=s.window[0]&&m<=s.window[1]&&!(age>5);
  const w=document.getElementById('win');w.textContent=s.in_window?'ENTRY WINDOW ON':'entry window: last '+s.window[1]+' min';w.className='pill'+(s.in_window?' on':'');
  const u=document.getElementById('upd');u.textContent=s.error?s.error.slice(0,90):'updated '+(s.last_update||'—');u.className='pill'+(s.error?' err':'');
  const st=s.stats;
  const p=document.getElementById('pnl');p.textContent='$'+f(st.pnl,3);p.className=cls(st.pnl);
  const un=document.getElementById('unr');un.textContent='$'+f(st.unreal,3);un.className=cls(st.unreal);
  document.getElementById('tw').textContent=st.trades+' / '+st.wins;
  document.getElementById('miss').textContent=st.missed;
  const c=s.cfg;document.getElementById('rules').textContent=`Rules: strike spot se ±$${c.band} · best ask $${c.entry_min ?? 0}–$${c.entry_max} · din me max ${c.max_trades||'unlimited'} entry · target ${c.target_x}x (bid pe) · qty ${c.qty} · fee $${c.fee}/side · extra slip $${c.slip}. Highlighted rows = band ke andar.`;
  const ch=s.chain.map(r=>({...r,_cls:r.in_band?'band':''}));
  document.getElementById('chain').innerHTML=table(ch,[
   ['Symbol',r=>r.symbol],['Type',r=>r.kind==='C'?'CE':'PE'],['Strike',r=>f(r.strike,0)],['vs spot',r=>f(r.dist,1)],
   ['Bid',r=>f(r.bid)],['Ask',r=>f(r.ask)],['Ask size',r=>f(r.ask_size,0)],['Last',r=>f(r.last)],['Mark',r=>f(r.mark,3)],
   ['',r=>r.status?'<span class="tag '+r.status+'">'+r.status+'</span>':'']])||'aaj ki expiry ke liye koi gold option nahi mila';
  document.getElementById('open').innerHTML=table(s.open,[
   ['Symbol',r=>r.symbol],['Entry',r=>f(r.entry_px)],['Bid now',r=>f(r.bid_now)],['Target',r=>f(r.entry_px*c.target_x)],
   ['Open P&L',r=>'<span class="'+cls(r.unreal)+'">'+f(r.unreal,3)+'</span>'],['Entry time',r=>r.entry_time]])||'koi nahi';
  document.getElementById('closed').innerHTML=table(s.closed,[
   ['Symbol',r=>r.symbol],['Entry time',r=>r.entry_time],['Entry',r=>f(r.entry_px)],['Exit',r=>f(r.exit_px)],
   ['Reason',r=>r.exit_reason],['Min left',r=>f(r.mins_left_at_entry,0)],['P&L',r=>'<span class="'+cls(+r.pnl)+'">'+f(r.pnl,3)+'</span>']])||'abhi koi nahi';
  document.getElementById('log').textContent=s.log.join('\n');
 }catch(e){const u=document.getElementById('upd');u.textContent='app band hai?';u.className='pill err'}
}
tick();setInterval(tick,location.port?3000:20000);  // GitHub Pages pe har 20 sec
</script></body></html>"""


def make_handler(engine, password=None):
    import base64, hmac

    class H(BaseHTTPRequestHandler):
        def authed(self):
            if not password:
                return True
            h = self.headers.get("Authorization", "")
            if h.startswith("Basic "):
                try:
                    _, pw = base64.b64decode(h[6:]).decode().split(":", 1)
                    if hmac.compare_digest(pw, password):
                        return True
                except Exception:
                    pass
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="Gold Paper Trader"')
            self.end_headers()
            return False

        def do_GET(self):
            if not self.authed():
                return
            if self.path.startswith("/api/state"):
                body = json.dumps(engine.state(), default=str).encode()
                ctype = "application/json"
            elif self.path in ("/", "/index.html"):
                body, ctype = PAGE.encode(), "text/html; charset=utf-8"
            else:
                self.send_error(404); return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass
    return H


def main():
    p = argparse.ArgumentParser(description="Gold options live paper trader (Delta India)")
    p.add_argument("--band", type=float, default=10, help="strike spot se +/- $ (default 10)")
    p.add_argument("--entry-min", type=float, default=0.0, help="best ask kam se kam itna ho (inclusive)")
    p.add_argument("--entry-max", type=float, default=0.10, help="best ask zyada se zyada itna ho (inclusive)")
    p.add_argument("--target-x", type=float, default=5, help="bid >= entry x yeh -> sell")
    p.add_argument("--min-mins-left", type=float, default=0)
    p.add_argument("--max-mins-left", type=float, default=60, help="expiry se itne minute pehle se entry")
    p.add_argument("--qty", type=float, default=1, help="contracts per trade")
    p.add_argument("--max-trades", type=int, default=0, help="ek expiry din me max kitni entry (0 = koi limit nahi)")
    p.add_argument("--fee-per-side", type=float, default=0.005)
    p.add_argument("--extra-slippage", type=float, default=0.0, help="ask ke upar extra $ (default 0, kyunki ask pe hi buy)")
    p.add_argument("--interval", type=float, default=5, help="kitne second me data refresh")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--host", default="127.0.0.1",
                   help="0.0.0.0 do to server/phone se bhi khulega (tab --password zaroori)")
    p.add_argument("--password", default=os.environ.get("PAPER_PASSWORD"),
                   help="dashboard password (ya env PAPER_PASSWORD)")
    p.add_argument("--tg-token", default=os.environ.get("TG_TOKEN"), help="Telegram bot token (optional)")
    p.add_argument("--tg-chat", default=os.environ.get("TG_CHAT"), help="Telegram chat id (optional)")
    p.add_argument("--expiry-utc", default="16:00", help="fallback expiry time UTC agar API se na mile")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--headless", action="store_true",
                   help="dashboard server ke bina chalo, aaj ki expiry ke baad khud band (GitHub Actions)")
    p.add_argument("--export", default=None, help="state JSON yahan likho, e.g. docs/state.json")
    p.add_argument("--export-every", type=float, default=30, help="export har kitne second")
    p.add_argument("--max-runtime", type=float, default=170, help="headless me max minute")
    p.add_argument("--write-page", default=None,
                   help="static dashboard HTML yahan likho (state.json padhta hai) aur band ho jao")
    cfg = p.parse_args()

    if cfg.write_page:
        os.makedirs(os.path.dirname(cfg.write_page) or ".", exist_ok=True)
        page = PAGE.replace("fetch('/api/state')", "fetch('state.json?t='+Date.now())")
        with open(cfg.write_page, "w", encoding="utf-8") as f:
            f.write(page)
        print("page likh diya:", cfg.write_page)
        return
    if cfg.host not in ("127.0.0.1", "localhost") and not cfg.password:
        p.error("bahar se kholne (--host 0.0.0.0) ke liye --password dena zaroori hai")
    TG["token"], TG["chat"] = cfg.tg_token, cfg.tg_chat
    h, m = (int(x) for x in cfg.expiry_utc.split(":"))
    engine, market = Engine(cfg), Market(h, m)
    log(f"Start: band ±{cfg.band}, ask {cfg.entry_min}–{cfg.entry_max}, max {cfg.max_trades or 'unlimited'} trades/din, target {cfg.target_x}x, "
        f"window last {cfg.max_mins_left:g} min, qty {cfg.qty:g}")
    if cfg.headless:
        if TG["token"] and TG["chat"]:
            telegram("Gold paper trader start ho gaya (GitHub)")
        run_headless(engine, market, cfg)
        time.sleep(3)  # pending telegram messages
        return
    threading.Thread(target=run_loop, args=(engine, market, cfg.interval), daemon=True).start()
    srv = ThreadingHTTPServer((cfg.host, cfg.port), make_handler(engine, cfg.password))
    where = "localhost" if cfg.host in ("127.0.0.1", "localhost") else "<server-ip>"
    print(f"\nDashboard: http://{where}:{cfg.port}   (band karne ke liye Ctrl+C)\n", flush=True)
    if TG["token"] and TG["chat"]:
        telegram("Gold paper trader start ho gaya")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nBand kar diya. Open positions paper_state.json me save hain.")


if __name__ == "__main__":
    main()
