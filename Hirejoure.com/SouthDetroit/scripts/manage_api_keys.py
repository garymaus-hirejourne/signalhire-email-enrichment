#!/usr/bin/env python
"""Simple helper to manage API keys used by the SouthDetroit scripts.

It stores all keys in a local `.env` file (key=value per line).
Existing scraper scripts will read from environment variables, so after
updating the file you can either:
    • run `python -m dotenv run -- <command>` if you have python-dotenv, or
    • restart your terminal so that `setx` values take effect.

Supported keys (edit list as needed):
    HUNTER_API_KEY
    SERPAPI_API_KEY
    SNOV_API_USER
    SNOV_API_KEY
    APOLLO_API_KEY
    CLEARBIT_KEY
    SKRAPP_KEY
    NORBERT_KEY
    ROCKETREACH_KEY
    BING_KEY

Run:
    python scripts/manage_api_keys.py            # interactive menu
    python scripts/manage_api_keys.py --clear    # wipe all keys
    python scripts/manage_api_keys.py --show     # print current keys
"""
from __future__ import annotations
import argparse
import os
from pathlib import Path
from typing import Dict

ENV_PATH = Path.home() / "Desktop" / "southdetroit_api_keys.env"
SUPPORTED_KEYS = [
    "HUNTER_API_KEY",
    "SERPAPI_API_KEY",
    "SNOV_API_USER",
    "SNOV_API_KEY",
    "APOLLO_API_KEY",
    "CLEARBIT_KEY",
    "SKRAPP_KEY",
    "NORBERT_KEY",
    "ROCKETREACH_KEY",
    "BING_KEY",
]


def load_env() -> Dict[str, str]:
    if not ENV_PATH.exists():
        return {}
    content = ENV_PATH.read_text().splitlines()
    env: Dict[str, str] = {}
    for line in content:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def save_env(env: Dict[str, str]):
    with ENV_PATH.open("w", encoding="utf-8") as f:
        for k, v in env.items():
            f.write(f"{k}={v}\n")
    print(f"Saved keys to {ENV_PATH}")


def interactive():
    env = load_env()
    while True:
        print("\nCurrent keys:")
        for k in SUPPORTED_KEYS:
            print(f"  {k}: {'<set>' if k in env else '<empty>'}")
        print("\nMenu:\n 1) Set key  2) Clear key  3) Clear ALL  4) Save & exit  5) Exit without saving")
        choice = input("Select option: ").strip()
        if choice == "1":
            key = input("Key name: ").strip()
            if key not in SUPPORTED_KEYS:
                print("Unsupported key.")
                continue
            value = input("Paste value: ").strip()
            env[key] = value
        elif choice == "2":
            key = input("Key name to clear: ").strip()
            env.pop(key, None)
        elif choice == "3":
            env.clear()
            print("All keys cleared (not yet saved).")
        elif choice == "4":
            save_env(env)
            break
        elif choice == "5":
            print("Exiting without saving.")
            break
        else:
            print("Invalid selection.")


def main():
    parser = argparse.ArgumentParser(description="Manage API keys via .env")
    parser.add_argument("--clear", action="store_true", help="Clear all stored keys")
    parser.add_argument("--show", action="store_true", help="Show current keys and exit")
    args = parser.parse_args()

    if args.show:
        env = load_env()
        for k in SUPPORTED_KEYS:
            print(f"{k}={'<set>' if k in env else '<empty>'}")
        return

    if args.clear:
        if ENV_PATH.exists():
            ENV_PATH.unlink()
            print(f"Deleted {ENV_PATH}")
        else:
            print("No .env file to delete.")
        return

    interactive()


if __name__ == "__main__":
    main()
