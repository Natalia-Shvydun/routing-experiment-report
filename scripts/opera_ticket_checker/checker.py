#!/usr/bin/env python3
"""
Opera de Paris Ticket Checker Bot

Checks the Opera de Paris website for ticket availability across
multiple performances and dates. Sends Telegram notifications
when tickets appear. Uses DOM color inspection to determine
which specific categories have tickets (greyed-out = sold out,
dark text = available).
"""

import asyncio
import json
import logging
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from playwright.async_api import async_playwright
from playwright.async_api import TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR / ".env")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ---------------------------------------------------------------------------
# Performance configurations
# ---------------------------------------------------------------------------

PERFORMANCES = [
    {
        "name": "La Dame aux camélias",
        "url": "https://www.operadeparis.fr/en/season-25-26/ballet/la-dame-aux-camelias",
        "venue": "Palais Garnier",
        "target_days": ["08"],
        "target_month": "May",
        "excluded_categories": {"4", "5"},
    },
    {
        "name": "Vibrations",
        "url": "https://www.operadeparis.fr/saison-25-26/ballet/micaela-taylor-mats-ek-crystal-pite",
        "venue": "Palais Garnier",
        "target_days": ["27"],
        "target_month": "Jun",
        "excluded_categories": {"4", "5", "optima"},
    },
]

TIMEZONE = ZoneInfo("Europe/Zurich")
RUN_HOUR_START = 6
RUN_HOUR_END = 1

STATE_FILE = Path.home() / ".opera_ticket_checker_state.json"
RENOTIFY_INTERVAL = timedelta(minutes=30)
DEBUG_DIR = SCRIPT_DIR / "debug"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(SCRIPT_DIR / "checker.log"),
    ],
)
log = logging.getLogger("opera_checker")

MONTH_NAMES = (
    r"(?:January|February|March|April|May|June|July|August|September"
    r"|October|November|December"
    r"|Janvier|F[ée]vrier|Mars|Avril|Mai|Juin|Juillet"
    r"|Ao[uû]t|Septembre|Octobre|Novembre|D[ée]cembre)"
)

WEEKDAYS = (
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday"
    r"|Lundi|Mardi|Mercredi|Jeudi|Vendredi|Samedi|Dimanche)"
)

MONTH_LOOKUP = {
    "january": "Jan", "february": "Feb", "march": "Mar",
    "april": "Apr", "may": "May", "june": "Jun",
    "july": "Jul", "august": "Aug", "september": "Sep",
    "october": "Oct", "november": "Nov", "december": "Dec",
    "janvier": "Jan", "février": "Feb", "fevrier": "Feb",
    "mars": "Mar", "avril": "Apr", "mai": "May", "juin": "Jun",
    "juillet": "Jul", "août": "Aug", "aout": "Aug",
    "septembre": "Sep", "octobre": "Oct", "novembre": "Nov",
    "décembre": "Dec", "decembre": "Dec",
}


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured. Message:\n%s", message)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }).encode()

    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                log.info("Telegram notification sent")
            else:
                log.error("Telegram API returned status %d", resp.status)
    except Exception as e:
        log.error("Failed to send Telegram message: %s", e)


# ---------------------------------------------------------------------------
# State management (deduplication)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def should_notify(key: str, categories: Set[str], state: dict) -> bool:
    if key not in state:
        return True
    last = state[key]
    last_time = datetime.fromisoformat(last["time"])
    last_cats = set(last.get("categories", []))
    if categories != last_cats:
        return True
    return datetime.now() - last_time > RENOTIFY_INTERVAL


def record_notification(key: str, categories: Set[str], state: dict):
    state[key] = {
        "time": datetime.now().isoformat(),
        "categories": sorted(categories),
    }
    save_state(state)


# ---------------------------------------------------------------------------
# Cookie consent helper
# ---------------------------------------------------------------------------

async def dismiss_cookies(page):
    cookie_selectors = [
        'button:has-text("Accept")',
        'button:has-text("Accepter")',
        'button:has-text("Tout accepter")',
        'button:has-text("Accept all")',
        'button:has-text("OK")',
        '[id*="cookie"] button',
        '[class*="cookie"] button',
        '[id*="consent"] button',
        '[class*="consent"] button',
    ]
    for sel in cookie_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=1000):
                await btn.click()
                log.info("Dismissed cookie banner via: %s", sel)
                await page.wait_for_timeout(500)
                return
        except Exception:
            continue


# ---------------------------------------------------------------------------
# Calendar text parser
# ---------------------------------------------------------------------------

def parse_calendar_blocks(text: str) -> List[dict]:
    """
    Parse the booking calendar text into per-date blocks.
    Handles both English and French page content.
    NOTE: categories from this parser are just PRICE TIERS,
    not actual availability. Use DOM color inspection for that.
    """
    pattern = re.compile(
        r'(?:^|\n)(\d{2})\n+\n*'
        + WEEKDAYS +
        r'\n(' + MONTH_NAMES + r')\n',
        re.IGNORECASE,
    )

    splits = list(pattern.finditer(text))
    blocks = []

    for i, match in enumerate(splits):
        start = match.start()
        end = splits[i + 1].start() if i + 1 < len(splits) else len(text)
        raw = text[start:end].strip()

        day = match.group(1)
        month_raw = match.group(2).lower()
        month = MONTH_LOOKUP.get(month_raw, month_raw[:3].capitalize())

        raw_lower = raw.lower()

        sold_out = "sold out" in raw_lower or "complet" in raw_lower
        has_book = bool(re.search(r'\b(?:BOOK|R[ÉE]SERVER)\b', raw))
        last_seats = (
            "last seats" in raw_lower
            or "derni\u00e8res places" in raw_lower
        )
        has_exchange = (
            "ticket exchange" in raw_lower
            or "bourse" in raw_lower
        )

        status = "unknown"
        if sold_out and not has_book:
            status = "sold_out"
        elif has_book and not sold_out:
            status = "available"
        elif last_seats:
            status = "last_seats"
        elif has_exchange:
            status = "exchange"
        elif has_book:
            status = "available"

        blocks.append({
            "day": day,
            "month": month,
            "status": status,
            "has_book": has_book,
            "has_exchange": has_exchange,
            "raw": raw[:300],
        })

    return blocks


# ---------------------------------------------------------------------------
# DOM color inspection – determine actual per-category availability
# ---------------------------------------------------------------------------

_JS_GET_CATEGORY_COLORS = '''(day) => {
    const walker = document.createTreeWalker(
        document.body, NodeFilter.SHOW_TEXT, null
    );

    while (walker.nextNode()) {
        if (walker.currentNode.textContent.trim() !== day) continue;

        let card = walker.currentNode.parentElement;
        for (let level = 0; level < 20; level++) {
            if (!card || card.tagName === 'BODY') break;

            const cardText = card.textContent;
            if (!/\\bBOOK\\b|\\bRÉSERVER\\b/i.test(cardText) ||
                !/Cat\\.\\s*\\d/i.test(cardText)) {
                card = card.parentElement;
                continue;
            }

            /* Use text length to identify the single-date card level
               (~800 chars) vs the whole calendar section (>10k chars).
               The BOOK element may be a <button>, not <a>. */
            if (cardText.length > 2000) {
                card = card.parentElement;
                continue;
            }

            /* Walk every element inside the card looking for category
               labels ("Cat. 1", "Optima", etc.) and read their
               computed text color. */
            const result = {};
            const allEls = card.querySelectorAll('*');

            for (const el of allEls) {
                const t = el.textContent.trim();
                let catKey = null;

                const catMatch = t.match(/^Cat\\.?\\s*(\\d+)$/i);
                if (catMatch) catKey = catMatch[1];
                else if (/^Optima$/i.test(t)) catKey = 'optima';

                if (!catKey || catKey in result) continue;

                const style = window.getComputedStyle(el);
                const color = style.color;

                /* Walk ancestors to compute effective opacity */
                let effectiveOpacity = 1;
                let anc = el;
                while (anc && anc !== document.body) {
                    effectiveOpacity *= parseFloat(
                        window.getComputedStyle(anc).opacity
                    );
                    anc = anc.parentElement;
                }

                const rgbMatch = color.match(
                    /rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/
                );
                if (rgbMatch) {
                    const r = parseInt(rgbMatch[1]);
                    const g = parseInt(rgbMatch[2]);
                    const b = parseInt(rgbMatch[3]);
                    const luminance = (r + g + b) / 3;

                    result[catKey] = {
                        available: luminance < 80 && effectiveOpacity > 0.5,
                        color: color,
                        opacity: Math.round(effectiveOpacity * 100) / 100,
                        luminance: Math.round(luminance)
                    };
                }
            }

            return Object.keys(result).length > 0 ? result : null;
        }
    }

    return null;
}'''


async def get_available_categories_from_dom(
    page, day: str
) -> Optional[Set[str]]:
    """
    Inspect the calendar card's DOM for a given date.
    Available categories have dark text; sold-out ones are greyed out.
    Returns the set of category IDs whose text is dark, or None if
    the card could not be found / inspected.
    """
    result = await page.evaluate(_JS_GET_CATEGORY_COLORS, day)

    if result is None:
        log.warning("DOM color inspection found no card for day %s", day)
        return None

    available = set()  # type: Set[str]
    for key, info in result.items():
        tag = "AVAIL" if info["available"] else "grey"
        log.info(
            "  Cat %-6s  color=%-28s  opacity=%.2f  lum=%3d  => %s",
            key, info["color"], info["opacity"], info["luminance"], tag,
        )
        if info["available"]:
            available.add(key)

    return available


# ---------------------------------------------------------------------------
# Check a single performance
# ---------------------------------------------------------------------------

async def check_performance(
    page, perf: dict, state: dict
) -> List[dict]:
    """
    Check ticket availability for one performance.
    Returns a list of results (one per target day that has tickets
    in non-excluded categories).
    """
    name = perf["name"]
    url = perf["url"]
    target_days = perf["target_days"]
    excluded = perf["excluded_categories"]
    slug = re.sub(r'[^a-z0-9]', '_', name.lower())

    log.info("--- Checking: %s ---", name)
    results = []

    try:
        await page.goto(
            url + "#calendar",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await page.wait_for_timeout(5000)
        await dismiss_cookies(page)
        await page.wait_for_timeout(2000)

        DEBUG_DIR.mkdir(exist_ok=True)
        await page.screenshot(
            path=str(DEBUG_DIR / f"{slug}_page.png"), full_page=True
        )

        body_text = await page.inner_text("body")
        (DEBUG_DIR / f"{slug}_text.txt").write_text(body_text)

        blocks = parse_calendar_blocks(body_text)
        log.info("Parsed %d date blocks for %s", len(blocks), name)

        for block in blocks:
            is_target = block["day"] in target_days
            marker = " <<<" if is_target else ""
            log.info(
                "  %s %s: status=%s, book=%s%s",
                block["month"], block["day"],
                block["status"], block["has_book"], marker,
            )

        for day in target_days:
            target_block = None
            for block in blocks:
                if block["day"] == day:
                    target_block = block
                    break

            if target_block is None:
                log.info("Day %s not found for %s", day, name)
                continue

            log.info(
                "%s day %s: %s", name, day, target_block["status"]
            )

            if target_block["status"] == "sold_out":
                continue

            if target_block["status"] in ("available", "last_seats"):
                actual_cats = await get_available_categories_from_dom(
                    page, day,
                )

                if actual_cats is None:
                    log.warning(
                        "%s day %s: DOM color inspection failed, "
                        "skipping to avoid false positive",
                        name, day,
                    )
                    continue

                cats = actual_cats - excluded
                if not cats:
                    log.info(
                        "%s day %s: tickets only in excluded categories "
                        "(available: %s, excluded: %s)",
                        name, day, actual_cats, excluded,
                    )
                    continue

                results.append({
                    "categories": cats,
                    "source": "main_page",
                    "perf": perf,
                    "day": day,
                    "month": target_block["month"],
                    "status": target_block["status"],
                })

            elif target_block["has_exchange"]:
                results.append({
                    "categories": {"exchange"},
                    "source": "main_page",
                    "perf": perf,
                    "day": day,
                    "month": target_block["month"],
                    "status": "exchange",
                })

    except PlaywrightTimeout:
        log.error("Timeout loading %s", name)
    except Exception as e:
        log.error("Error checking %s: %s", name, e)

    return results


# ---------------------------------------------------------------------------
# Notification formatting
# ---------------------------------------------------------------------------

WEEKDAY_LOOKUP = {
    ("May", "08"): "Friday",
    ("Jun", "26"): "Friday",
    ("Jun", "27"): "Saturday",
    ("Jun", "28"): "Sunday",
    ("Jun", "29"): "Monday",
}

MONTH_FULL = {
    "Jan": "January", "Feb": "February", "Mar": "March",
    "Apr": "April", "May": "May", "Jun": "June",
    "Jul": "July", "Aug": "August", "Sep": "September",
    "Oct": "October", "Nov": "November", "Dec": "December",
}


def format_message(result: dict) -> str:
    perf = result["perf"]
    name = perf["name"]
    venue = perf["venue"]
    url = perf["url"]
    day = result["day"]
    month = result.get("month", "")
    month_full = MONTH_FULL.get(month, month)
    weekday = WEEKDAY_LOOKUP.get((month, day), "")
    weekday_str = f"{weekday}, " if weekday else ""

    cats = result["categories"]
    if cats == {"exchange"}:
        cat_text = "Available on ticket exchange platform"
    else:
        cat_text = ", ".join(
            f"Category {c}" if c != "optima" else "Optima"
            for c in sorted(cats)
        )

    return (
        f"\U0001f3ab <b>Tickets Available!</b>\n\n"
        f"<b>{name}</b>\n"
        f"\U0001f4c5 {weekday_str}{month_full} {day}, 2026\n"
        f"\U0001f4cd {venue}\n\n"
        f"<b>Categories:</b> {cat_text}\n\n"
        f'<a href="{url}#calendar">\U0001f449 Go to tickets</a>'
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def is_within_active_hours() -> bool:
    now = datetime.now(TIMEZONE)
    hour = now.hour
    return hour >= RUN_HOUR_START or hour < RUN_HOUR_END


async def main():
    if not is_within_active_hours():
        now_zurich = datetime.now(TIMEZONE).strftime("%H:%M")
        print(f"Outside active hours ({now_zurich} Zurich time). Skipping.")
        return

    log.info("=== Opera Ticket Checker starting ===")

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning(
            "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set. "
            "Notifications will be logged only."
        )

    state = load_state()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--headless=new",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-GB",
        )
        stealth = Stealth()
        await stealth.apply_stealth_async(context)
        page = await context.new_page()

        for perf in PERFORMANCES:
            results = await check_performance(page, perf, state)
            for result in results:
                day = result["day"]
                slug = re.sub(r'[^a-z0-9]', '_', perf["name"].lower())
                state_key = f"{slug}_day{day}"
                cats = result["categories"]

                if should_notify(state_key, cats, state):
                    msg = format_message(result)
                    log.info(
                        "Tickets found for %s day %s! Notifying...",
                        perf["name"], day,
                    )
                    send_telegram(msg)
                    record_notification(state_key, cats, state)
                else:
                    log.info(
                        "%s day %s: already notified recently",
                        perf["name"], day,
                    )

        await browser.close()

    log.info("=== Check complete ===\n")


if __name__ == "__main__":
    asyncio.run(main())
