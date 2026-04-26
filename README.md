# Bambu Monitor 🖨️

A Raspberry Pi-based monitor for Bambu Lab printers with Telegram notifications and AMS filament tracking.

## Features
- 📡 Bambu Cloud MQTT connection with auto-reconnect
- 🔔 Telegram notifications: print start, milestones (25/50/75%), finish, failure
- 🧵 AMS filament tracking: per-slot weight, cost estimation, low-filament alerts
- 🌡️ Temperature alerts (nozzle & bed)
- 🏠 Home Assistant integration
- 💾 Print history logging
- 🔒 Thread-safe with state persistence between restarts

## Requirements
- Raspberry Pi (or any Linux machine)
- Bambu Lab account + printer serial number
- Telegram bot token (from @BotFather)
- Python 3.10+

## Installation
```bash
pip install paho-mqtt
```

## Configuration
Edit the constants at the top of `bambu_monitor.py`:
- `SERIAL` — your printer serial number
- `TELEGRAM_TOKEN` — your Telegram bot token
- `TELEGRAM_CHAT_ID` — your Telegram chat ID

Store your Bambu credentials in `~/.bambu_tokens.json`.

## Usage
```bash
python3 bambu_monitor.py
```

## License
MIT
