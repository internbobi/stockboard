#!/usr/bin/env python3
"""Refresh TTCKVN board data from the KBS live price board (vnstock 4.x).

Runs on a 5-minute schedule during trading hours (Mon-Fri, 08:45-15:00 VN time).
One price_board call fetches ALL 1517 stocks (HSX/HNX/UPCOM):
  - Block 1: the 132-symbol >10 ty basket (data.json, top 100 by value).
  - Block 2: Layers 2/3 (% above/below EMA50/200) and 4/5 (TMA counts),
    computed over the symbols passing Dat's DKKL() that day (full market).
The Block 2 layers are updated incrementally from the board (no extra API
calls) and pushed to internbobi/stockboard via git over SSH (deploy key, no
approval prompts). Outside the window, or when the market has no live data
yet, it exits quietly and keeps the previous data.json.

Run with the vnstock 4.x venv:  ~/.venv4x/bin/python refresh_live.py [--force]
  --force  ignore the trading-hours window (for manual tests only)
"""
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone

VN_TZ = timezone(timedelta(hours=7))
WORKDIR = "/home/hatch/workspace/stock-dashboard"
SYMBOLS_FILE = os.path.join(WORKDIR, "data_full_filtered.json")
DATA_FILE = os.path.join(WORKDIR, "data.json")
PCT_FILE = os.path.join(WORKDIR, "pct_ema50.json")
TMA_COUNTS_FILE = os.path.join(WORKDIR, "tma_counts.json")
TMA_STATE_FILE = os.path.join(WORKDIR, "tma_state.json")
# TMA phase EMAs (same spans as fetch_tma.py).
TMA_FAST_SPANS = [3, 5, 8, 10, 12, 15]
TMA_SLOW_SPANS = [30, 35, 40, 45, 50, 60]
TMA_ALL_SPANS = TMA_FAST_SPANS + TMA_SLOW_SPANS
# Block 2 % layers (Tang 2/3/...): (span, pct json, state json). The cron
# updates each layer's TODAY entry from the price board it already fetched
# (no extra API calls). New layers: add a row here + a fetch with
# fetch_block2.py and a <canvas> in index.html.
PCT_LAYERS = [
    (50, "pct_ema50.json", "ema50_state.json"),
    (200, "pct_ema200.json", "ema200_state.json"),
]
ALL_STOCKS_FILE = os.path.join(WORKDIR, "all_stocks.csv")
# Dat's DKKL() thresholds. Close is in THOUSANDS of VND, volume in shares,
# so the 1,000,000 VND value threshold is close*volume > 1000.
DKKL_VOL = 50000
DKKL_VAL = 1000.0


def _dkkl_today(prev, close, volume, today):
    """Dat's DKKL() for one symbol on `today` (close in thousands of VND,
    volume in shares), using the state's 6-day hist for the trailing 5 days:
      Nz(Cum(C>0)) > 0
      AND (MA(Ref(V,-1),5) > 50000 OR avg value of the 5 days before today
           > 1M VND)
      AND (V > 50000 OR C*V > 1M VND today)
    The trailing 5 days exclude today (Ref(.,-1) then MA over 5). If the
    state's hist ends on today, they are hist[-6:-1]; if it ends on the last
    processed day, they are hist[-5:]. If the state has no hist (older state
    files), fall back to True (old behaviour: count every symbol)."""
    hist = prev.get("hist")
    if not hist or len(hist) < 6:
        return True
    if hist[-1].get("d") == today:
        trailing = hist[-6:-1]
    else:
        trailing = hist[-5:]
    if len(trailing) < 5:
        return True
    vol_ma5 = sum(h["v"] for h in trailing) / 5.0
    val_ma5 = sum(h["c"] * h["v"] for h in trailing) / 5.0
    has_traded = close > 0 or any(h["c"] > 0 for h in hist)
    # 5-day clause is AND (Dat approved 2026-11-12): his code is OR but his
    # AmiBroker output (294 for 12/11 final) matches AND (310) not OR (811)
    # on vnstock data. Today's clause stays OR, per the code.
    return (has_traded
            and (vol_ma5 > DKKL_VOL and val_ma5 > DKKL_VAL)
            and (volume > DKKL_VOL or close * volume > DKKL_VAL))


def _roll_hist(prev, today, close, volume):
    """Roll the state's 6-day hist forward to `today`, using the live
    close/volume for today's entry (refreshed on every run so that by the
    close it holds the final values). Returns the new hist list."""
    hist = list(prev.get("hist") or [])
    today_entry = {"d": today, "c": round(close, 2), "v": int(volume)}
    if hist and hist[-1].get("d") == today:
        hist[-1] = today_entry
        return hist
    hist.append(today_entry)
    return hist[-6:]
SYNC_RECORD = "/home/hatch/workspace/stock-dashboard.muse-github-snapshot-sync.json"
OWNER, REPO, BRANCH = "internbobi", "stockboard", "main"
GIT_DIR = "/home/hatch/workspace/stockboard-git"
# Push via git over SSH (deploy key) so scheduled runs need no approval card.
# ssh runs as root, so the key and known_hosts paths are explicit.
GIT_SSH = ("ssh -i /home/hatch/.ssh/id_ed25519 -o IdentitiesOnly=yes "
           "-o UserKnownHostsFile=/home/hatch/.ssh/known_hosts")
MIN_ROWS = 100          # abort if the board returns fewer rows than this
VALUE_THRESHOLD = 10.0  # ty VND, same filter as v1


def log(msg):
    print(f"[{datetime.now(VN_TZ):%H:%M:%S}] {msg}", flush=True)


def in_window(now):
    """Mon-Fri, 08:45-15:00 Vietnam time, EXCLUDING the lunch break
    11:30-13:00 (Dat, 2026-11-12: no 5-min updates during lunch)."""
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    if not (8 * 60 + 45 <= t <= 15 * 60):
        return False
    if 11 * 60 + 30 <= t < 13 * 60:
        return False
    return True


def parse_board_date(s):
    # "11/11/2026" -> "2026-11-11"
    d, m, y = s.strip().split("/")
    return f"{y}-{m}-{d}"


def fetch_board(symbols):
    from vnstock import Vnstock
    v = Vnstock(source="KBS")
    tr = v.stock(symbol=symbols[0], source="KBS").trading
    df = tr.price_board(symbols_list=symbols)
    return df


def build_data(df, now):
    rows = df.to_dict("records")
    if len(rows) < MIN_ROWS:
        raise RuntimeError(f"board returned only {len(rows)} rows, aborting")
    td = str(rows[0].get("TD") or "").strip()
    date = parse_board_date(td) if td else now.strftime("%Y-%m-%d")
    if date != now.strftime("%Y-%m-%d"):
        raise RuntimeError(f"board date {date} is not today, aborting (holiday?)")
    items = []
    for r in rows:
        value_b = float(r["total_value"]) / 1e9
        if value_b <= VALUE_THRESHOLD:
            continue
        close = float(r["close_price"]) / 1000.0
        prev = float(r["reference_price"]) / 1000.0
        items.append({
            "symbol": str(r["symbol"]),
            "exchange": str(r["exchange"]),
            "close": round(close, 2),
            "prev_close": round(prev, 2),
            "change": round(close - prev, 2),
            "pct": round(float(r["percent_change"]), 1),
            "volume": int(r["volume_accumulated"]),
            "value_billion": round(value_b, 2),
        })
    items.sort(key=lambda x: x["value_billion"], reverse=True)
    old = json.load(open(DATA_FILE))
    data = dict(old)
    data.update({
        "date": date,
        "source": "vnstock 4.x KBS (live price board)",
        "count_filtered": len(items),
        "sort_by": "value_billion_desc",
        "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S+07:00"),
        "items": items[:100],
    })
    return data


def _atomic_write(path, data):
    tmp = path + ".tmp-" + uuid.uuid4().hex[:8]
    json.dump(data, open(tmp, "w"), ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def update_pct_layer(df, now, span, pct_name, state_name):
    """Block 2, Tang N: % of DKKL-passing stocks above/below EMA<span>,
    updated incrementally from the live price board — no extra API calls,
    so the 5-minute refresh stays cheap. Returns True if the pct json changed.

    Per symbol: ema_new = K*close_live + (1-K)*ema_prev (intraday estimate;
    at the close it equals the daily EMA). The EMA and the 6-day hist are
    updated for every symbol with a live close (the EMA is a property of the
    symbol); DKKL() decides only whether the symbol is COUNTED today.
    Units: history closes (and the stored EMA) are in THOUSANDS of VND, so
    the board close_price (raw VND) is divided by 1000 — same as build_data.
    The state stores the EMA under key "ema" (older EMA50 states used
    "ema50"; both are read). State EMAs are kept at 4 decimals: 2 decimals
    flipped the TMA phase comparison for flat stocks (2026-11-11).
    """
    pct_file = os.path.join(WORKDIR, pct_name)
    state_file = os.path.join(WORKDIR, state_name)
    if not os.path.exists(pct_file) or not os.path.exists(state_file):
        log(f"EMA{span} layer: state files missing, skipping "
            f"(run fetch_block2.py first)")
        return False
    state = json.load(open(state_file))
    pct = json.load(open(pct_file))
    rows = df.to_dict("records")
    today = now.strftime("%Y-%m-%d")
    k = 2.0 / (span + 1)
    above = 0
    total = 0
    new_state = dict(state)
    for r in rows:
        sym = str(r["symbol"])
        if sym not in state:
            continue
        close = float(r["close_price"]) / 1000.0  # thousands of VND
        volume = int(r["volume_accumulated"])
        if close <= 0:
            continue
        prev = state[sym]
        ema_prev = float(prev.get("ema", prev.get("ema50")))
        if prev.get("date") == today:
            # State already includes today's close (e.g. just built by the
            # batch fetch): do not apply the EMA recursion a second time.
            ema_new = ema_prev
        else:
            ema_new = k * close + (1.0 - k) * ema_prev
        new_state[sym] = {"ema": round(ema_new, 4),
                          "date": today,
                          "close": round(close, 2),
                          "volume": volume,
                          "hist": _roll_hist(prev, today, close, volume)}
        if not _dkkl_today(prev, close, volume, today):
            continue
        total += 1
        if close >= ema_new:
            above += 1
    if total < MIN_ROWS:
        log(f"EMA{span} layer: only {total} DKKL symbols, skipping")
        return False
    pct_above = round(above / total * 100, 2)
    entry = {"date": today, "total": total, "above": above,
             "below": total - above, "pct_above": pct_above,
             "pct_below": round(100 - pct_above, 2)}
    items = pct["items"]
    if items and items[-1]["date"] == today:
        items[-1] = entry
    else:
        items.append(entry)
    pct["latest"] = dict(entry)
    pct["updated_at"] = now.strftime("%Y-%m-%dT%H:%M:%S+07:00")
    _atomic_write(pct_file, pct)
    _atomic_write(state_file, new_state)
    log(f"EMA{span} layer: {today} above {above}/{total} = {pct_above}%")
    return True


def _classify_phase(f, s):
    """TMA phase for one symbol/day (same rules as fetch_tma.py):
    'v1t','v2t','v3t','v1g','v2g','v3g', or None (exact equality)."""
    max_f, min_f = max(f), min(f)
    max_s, min_s = max(s), min(s)
    e3, e5 = f[0], f[1]
    if e3 > e5:
        if max_f < min_s:
            return "v1t"
        if min_f > max_s:
            return "v3t"
        if max_f > min_s and min_f < max_s:
            return "v2t"
        return None
    if e3 < e5:
        if min_f > max_s:
            return "v1g"
        if min_s > max_f:
            return "v3g"
        if max_s > min_f and max_f > min_s:
            return "v2g"
        return None
    return None


def update_tma_layer(df, now):
    """Block 2, Tang 4 & 5: TMA 6-phase counts over the DKKL-passing symbols,
    updated incrementally from the live price board — no extra API calls.
    Returns True if changed.

    Per symbol: update all 12 EMAs from the live close (the EMAs and the
    6-day hist are updated for every symbol with a live close); classify the
    phase (same rules as fetch_block2.py; V3 uses the batdau/start
    conditions) and count it ONLY if the symbol passes DKKL() today.
    Units: thousands of VND (close_price / 1000), same as the % layers.
    """
    if not os.path.exists(TMA_COUNTS_FILE) or not os.path.exists(TMA_STATE_FILE):
        log("TMA layer: state files missing, skipping (run fetch_block2.py first)")
        return False
    state = json.load(open(TMA_STATE_FILE))
    tma = json.load(open(TMA_COUNTS_FILE))
    rows = df.to_dict("records")
    today = now.strftime("%Y-%m-%d")
    counts = {"v1t": 0, "v2t": 0, "v3t": 0, "v1g": 0, "v2g": 0, "v3g": 0}
    total = 0
    new_state = dict(state)
    for r in rows:
        sym = str(r["symbol"])
        if sym not in state:
            continue
        close = float(r["close_price"]) / 1000.0  # thousands of VND
        volume = int(r["volume_accumulated"])
        if close <= 0:
            continue
        prev = state[sym]
        if prev.get("date") == today:
            # State already includes today's close (just built by the batch
            # fetch): reuse the stored EMAs, do not recurse a second time.
            emas = {sp: float(prev[f"ema{sp}"]) for sp in TMA_ALL_SPANS}
        else:
            emas = {}
            for sp in TMA_ALL_SPANS:
                k = 2.0 / (sp + 1)
                emas[sp] = k * close + (1.0 - k) * float(prev[f"ema{sp}"])
        st = {"date": today, "close": round(close, 2), "volume": volume,
              "hist": _roll_hist(prev, today, close, volume)}
        for sp in TMA_ALL_SPANS:
            st[f"ema{sp}"] = round(emas[sp], 4)
        new_state[sym] = st
        if not _dkkl_today(prev, close, volume, today):
            continue
        f = [emas[sp] for sp in TMA_FAST_SPANS]
        s = [emas[sp] for sp in TMA_SLOW_SPANS]
        ph = _classify_phase(f, s)
        if ph:
            counts[ph] += 1
        total += 1
    if total < MIN_ROWS:
        log(f"TMA layer: only {total} DKKL symbols, skipping")
        return False
    vt = counts["v1t"] + counts["v2t"] + counts["v3t"]
    vg = counts["v1g"] + counts["v2g"] + counts["v3g"]
    tot = vt + vg
    pct_vt = round(vt / tot * 100, 2) if tot else None
    entry = {"date": today, "v1t": counts["v1t"], "v2t": counts["v2t"],
             "v3t": counts["v3t"], "v1g": counts["v1g"],
             "v2g": counts["v2g"], "v3g": counts["v3g"],
             "vt": vt, "vg": vg, "pct_vt": pct_vt,
             "pct_vg": round(100 - pct_vt, 2) if pct_vt is not None else None}
    items = tma["items"]
    if items and items[-1]["date"] == today:
        items[-1] = entry
    else:
        items.append(entry)

    def hhv(key, n):
        vals = [it[key] for it in items[-n:]]
        return max(vals) if vals else None
    last = items[-1]
    prev = items[-2] if len(items) >= 2 else None
    tma["latest"] = {
        "date": last["date"], "v1t": last["v1t"], "v2t": last["v2t"],
        "v3t": last["v3t"], "v1g": last["v1g"], "v2g": last["v2g"],
        "v3g": last["v3g"], "vt": last["vt"], "vg": last["vg"],
        "pct_vt": last["pct_vt"], "pct_vg": last["pct_vg"],
        "v3t_chg": (last["v3t"] - prev["v3t"]) if prev else 0,
        "v3g_chg": (last["v3g"] - prev["v3g"]) if prev else 0,
        "hhv260_v3t": hhv("v3t", 260),
        "hhv260_v3g": hhv("v3g", 260),
    }
    tma["updated_at"] = now.strftime("%Y-%m-%dT%H:%M:%S+07:00")
    _atomic_write(TMA_COUNTS_FILE, tma)
    _atomic_write(TMA_STATE_FILE, new_state)
    log(f"TMA layer: {today} VT {vt} ({pct_vt}%) VG {vg}, "
        f"V3T {counts['v3t']} V3G {counts['v3g']}")
    return True


VNINDEX_FILE = os.path.join(WORKDIR, "vnindex.json")
# VNINDEX bar colors (same AFL rules as fetch_vnindex.py).
VN_GREEN = "#00FF22"
VN_RED = "#C40000"
VN_YELLOW = "#FFCC00"
VN_MAGENTA = "#FF00FF"
VN_CYAN = "#00FFFF"


def _vn_bar_color(close, prev):
    if prev is None:
        return VN_GREEN
    if close == prev:
        return VN_YELLOW
    if close > prev:
        return VN_MAGENTA if (close / prev - 1) >= 0.05 else VN_GREEN
    return VN_CYAN if (prev / close - 1) >= 0.05 else VN_RED


def _vn_ma10_color(close, ma10):
    if ma10 is None:
        return "#98FB98"
    if close > ma10:
        return "#98FB98"
    if close < ma10:
        return "#FF0000"
    return "#FFFF00"


def update_vnindex_layer(now):
    """Block 2, Tang 1: VNINDEX daily bar for TODAY, updated live every
    5 min (Dat, 2026-11-12: both blocks update together).

    Fetches the VNINDEX daily history for the last few days (1 extra API
    call; the history API returns today's partial bar intraday, verified
    2026-11-12 10:00 VN: 2026-11-12 O 1748.14 H 1748.32 L 1740.55
    C 1740.68). Replaces today's bar if it exists, else appends it, then
    recomputes today's SMA10/50/150/200 from the closes in vnindex.json
    (the shown window holds 249 closes, enough for MA200) plus the bar
    color and MA10 color, using the same AFL rules as fetch_vnindex.py.
    Returns True if vnindex.json changed. A failure never breaks Block 1.
    """
    if not os.path.exists(VNINDEX_FILE):
        log("VNINDEX layer: vnindex.json missing, skipping")
        return False
    from vnstock import Vnstock
    today = now.strftime("%Y-%m-%d")
    start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    v = Vnstock(source="KBS")
    df = v.stock(symbol="VNINDEX", source="KBS").quote.history(
        start=start, end=today, interval="1D")
    if df is None or len(df) == 0:
        log("VNINDEX layer: history returned no rows, skipping")
        return False
    df = df.rename(columns={c: c.lower() for c in df.columns})
    df["date"] = df["time"].astype(str).str[:10]
    df = df.sort_values("date")
    row = df.iloc[-1]
    row_date = str(row["date"])
    if row_date != today:
        log(f"VNINDEX layer: latest history row is {row_date}, not today, "
            "skipping")
        return False
    o = round(float(row["open"]), 2)
    h = round(float(row["high"]), 2)
    lo = round(float(row["low"]), 2)
    c = round(float(row["close"]), 2)
    data = json.load(open(VNINDEX_FILE))
    items = data["items"]
    if not items:
        log("VNINDEX layer: vnindex.json has no items, skipping")
        return False
    prev_close = items[-1]["close"] if items[-1]["date"] != today else (
        items[-2]["close"] if len(items) >= 2 else None)
    # Build the close series ending today (replace today's bar if present).
    closes = [it["close"] for it in items]
    if items[-1]["date"] == today:
        closes[-1] = c
        items = items[:-1]
    closes.append(c)

    def sma(n):
        if len(closes) < n:
            return None
        return round(sum(closes[-n:]) / n, 2)
    ma10 = sma(10)
    ma50 = sma(50)
    ma150 = sma(150)
    ma200 = sma(200)
    item = {
        "date": today, "open": o, "high": h, "low": lo, "close": c,
        "color": _vn_bar_color(c, prev_close),
        "ma10": ma10, "ma10_color": _vn_ma10_color(c, ma10),
        "ma50": ma50, "ma150": ma150, "ma200": ma200,
    }
    items.append(item)
    data["items"] = items
    data["count_shown"] = len(items)
    data["to"] = today
    data["updated_at"] = now.strftime("%Y-%m-%dT%H:%M:%S+07:00")
    data["latest"] = {
        "date": today, "open": o, "high": h, "low": lo, "close": c,
        "change_pct": (round((c - prev_close) / prev_close * 100, 2)
                       if prev_close else 0.0),
        "ma10": ma10, "ma10_color": item["ma10_color"],
        "ma50": ma50, "ma150": ma150, "ma200": ma200,
    }
    _atomic_write(VNINDEX_FILE, data)
    log(f"VNINDEX layer: {today} O {o} H {h} L {lo} C {c} "
        f"({data['latest']['change_pct']}%), MA50 {ma50} MA200 {ma200}")
    return True


def _git(*args, timeout=120):
    env = dict(os.environ, GIT_SSH_COMMAND=GIT_SSH)
    out = subprocess.run(["git", "-C", GIT_DIR, *args],
                         capture_output=True, text=True, timeout=timeout, env=env)
    if out.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {out.stdout} {out.stderr}")
    return out.stdout.strip()


def push_to_github(pct_files=()):
    """Push data.json (and the pct jsons that changed) via git over SSH
    (deploy key) — no approval prompts.

    The clone is transport-only: reset --hard origin/main before each push keeps
    it in sync. Push index.html changes right after editing so a reset never
    discards an unpushed layout edit. No fallback to push-files: if git push
    fails the run exits 1 and the next 5-minute run retries.
    """
    for attempt in range(2):
        _git("fetch", "origin")
        _git("reset", "--hard", "origin/main")
        shutil.copyfile(DATA_FILE, os.path.join(GIT_DIR, "data.json"))
        files = ["data.json"]
        for name in pct_files:
            src = os.path.join(WORKDIR, name)
            if os.path.exists(src):
                shutil.copyfile(src, os.path.join(GIT_DIR, name))
                files.append(name)
        _git("add", *files)
        c = subprocess.run(
            ["git", "-C", GIT_DIR, "commit", "-m", "TTCKVN live refresh"],
            capture_output=True, text=True, timeout=60,
            env=dict(os.environ, GIT_SSH_COMMAND=GIT_SSH))
        if c.returncode != 0:
            if "nothing to commit" in (c.stdout + c.stderr):
                return _git("rev-parse", "HEAD")
            raise RuntimeError(f"git commit failed: {c.stdout} {c.stderr}")
        p = subprocess.run(
            ["git", "-C", GIT_DIR, "push", "origin", "main"],
            capture_output=True, text=True, timeout=120,
            env=dict(os.environ, GIT_SSH_COMMAND=GIT_SSH))
        if p.returncode == 0:
            return _git("rev-parse", "HEAD")
        log(f"git push failed (attempt {attempt + 1}), retrying: {p.stdout} {p.stderr}")
    raise RuntimeError("git push failed after 2 attempts")


def record_sync(sha, paths):
    recs = json.load(open(SYNC_RECORD))
    recs.append({
        "owner": OWNER, "repo": REPO, "branch": BRANCH,
        "source_dir": WORKDIR, "commit_sha": sha, "paths": paths,
    })
    tmp = SYNC_RECORD + ".tmp-" + uuid.uuid4().hex[:8]
    json.dump(recs, open(tmp, "w"), ensure_ascii=False, indent=2)
    os.replace(tmp, SYNC_RECORD)


def main():
    force = "--force" in sys.argv
    now = datetime.now(VN_TZ)
    if not force and not in_window(now):
        log(f"SKIP outside trading window ({now:%Y-%m-%d %H:%M} VN)")
        return 0
    # Block 1 keeps its 132-symbol >10 ty basket; Block 2 uses the full
    # market (1517 stocks). One price_board call fetches them all.
    import csv
    full = json.load(open(SYMBOLS_FILE))
    b1_symbols = {i["symbol"] for i in full["items"]}
    with open(ALL_STOCKS_FILE, newline="") as f:
        all_symbols = [row["symbol"] for row in csv.DictReader(f)]
    log(f"fetching price board for {len(all_symbols)} symbols...")
    df = fetch_board(all_symbols)
    log(f"board rows: {len(df)}")
    # Pre-open the KBS board is empty until the session board is published
    # (verified 2026-11-12 08:14 VN: 0 rows for all 1517 symbols). That is
    # not a failure - skip quietly and retry on the next 5-min run. Only
    # treat an empty board as an error once trading has started (09:00).
    if len(df) < MIN_ROWS and now.hour < 9:
        log(f"SKIP board not populated yet pre-open ({len(df)} rows), retry next run")
        return 0
    df_b1 = df[df["symbol"].isin(b1_symbols)]
    try:
        data = build_data(df_b1, now)
    except RuntimeError as e:
        # Same idea: before 09:00 the board may still carry yesterday's
        # date. Skip quietly; from 09:00 a stale date is a real error.
        if now.hour < 9 and "is not today" in str(e):
            log(f"SKIP pre-open, board still on yesterday's date: {e}")
            return 0
        raise
    if not data["items"]:
        log("SKIP no symbol above 10 ty yet (market not open), keeping old data")
        return 0
    json.dump(data, open(DATA_FILE, "w"), ensure_ascii=False, indent=1)
    log(f"top1 {data['items'][0]['symbol']} {data['items'][0]['value_billion']} ty, "
        f"filtered {data['count_filtered']}, items {len(data['items'])}")
    pct_files = []
    for span, pct_name, state_name in PCT_LAYERS:
        try:
            if update_pct_layer(df, now, span, pct_name, state_name):
                pct_files.append(pct_name)
        except Exception as e:
            # A % layer must never break the Block 1 refresh.
            log(f"EMA{span} layer update failed (Block 1 unaffected): {e}")
    try:
        if update_tma_layer(df, now):
            pct_files.append("tma_counts.json")
    except Exception as e:
        # The TMA layer must never break the Block 1 refresh.
        log(f"TMA layer update failed (Block 1 unaffected): {e}")
    try:
        if update_vnindex_layer(now):
            pct_files.append("vnindex.json")
    except Exception as e:
        # The VNINDEX layer must never break the Block 1 refresh.
        log(f"VNINDEX layer update failed (Block 1 unaffected): {e}")
    sha = push_to_github(pct_files)
    paths = ["data.json"] + pct_files
    record_sync(sha, paths)
    log(f"pushed commit {sha[:12]}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"ERROR {e}")
        sys.exit(1)
