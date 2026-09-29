#!/usr/bin/env python3
"""MagniDB build orchestrator. Single entry point for all build/test/fuzz flows.

Usage:
    python3 magni.py <command> [options]

Run `python3 magni.py help` for the full command list.

Implementation lives in the magni/ package.
"""

from magni.cli import main

if __name__ == "__main__":
    main()
