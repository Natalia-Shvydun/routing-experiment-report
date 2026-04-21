# Opera de Paris Ticket Checker Bot

Monitors two sources for **May 8, 2026** "La Dame aux camélias" tickets at Palais Garnier and sends a Telegram notification when they appear.

**Sources checked:**
1. Main performance page — `operadeparis.fr`
2. Ticket exchange (Bourse) — `bourse.operadeparis.fr`

**Filters:** Category 4 and 5 tickets are ignored. The notification includes which categories are available.

---

## Setup

### 1. Python environment

```bash
cd scripts/opera_ticket_checker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Telegram bot

1. Open Telegram and message [@BotFather](https://t.me/BotFather).
2. Send `/newbot`, follow the prompts, and copy the **bot token**.
3. Open a chat with your new bot and send any message (e.g. "hello").
4. Visit `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` in a browser.
5. Find `"chat":{"id": 123456789}` in the response — that's your **chat ID**.

### 3. Configuration

```bash
cp .env.example .env
```

Edit `.env` and fill in your values:

```
TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_CHAT_ID=987654321
```

---

## Usage

### Manual test run

```bash
source .venv/bin/activate
python checker.py
```

Check `debug/` for screenshots of what the bot sees on each page.
Check `checker.log` for detailed output.

### Automatic scheduling (launchd)

Install the launchd job to run the checker every 5 minutes:

```bash
cp com.opera.ticketchecker.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.opera.ticketchecker.plist
```

Verify it's running:

```bash
launchctl list | grep opera
```

View logs:

```bash
tail -f /tmp/opera_ticket_checker.log
```

### Stop / uninstall

```bash
launchctl unload ~/Library/LaunchAgents/com.opera.ticketchecker.plist
rm ~/Library/LaunchAgents/com.opera.ticketchecker.plist
```

---

## Debugging

- **Screenshots**: After each run, check `debug/main_page.png` and `debug/bourse_page.png` to see exactly what the bot sees.
- **Log file**: `checker.log` contains timestamped details of each check.
- **State file**: `~/.opera_ticket_checker_state.json` tracks notification deduplication. Delete it to force a re-notification.

## Customisation

- **Target date**: Change `TARGET_DATE_PATTERNS` in `checker.py`.
- **Excluded categories**: Edit `EXCLUDED_CATEGORIES` in `checker.py`.
- **Check interval**: Change `<integer>300</integer>` in the plist (value is in seconds).
- **Re-notification interval**: Change `RENOTIFY_INTERVAL` in `checker.py` (default: 30 min).
