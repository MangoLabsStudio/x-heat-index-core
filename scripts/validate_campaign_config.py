#!/usr/bin/env python3
"""Validate a campaign config before onboarding or deployment."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from campaign_core.config import campaign_terms, campaign_watch_handles, validate_campaign_config
from campaign_core.io import load_json_object


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config")
    parser.add_argument("--campaign-id", default="")
    args = parser.parse_args()

    path = Path(args.config)
    try:
        config = load_json_object(path)
    except Exception as exc:
        print(f"config.json validation FAILED: {exc}", file=sys.stderr)
        return 1

    errors = validate_campaign_config(config, args.campaign_id)
    if errors:
        print("config.json validation FAILED:")
        for error in errors:
            print(f"  - {error}")
        return 1

    watch_handles = campaign_watch_handles(config)
    terms = campaign_terms(config)
    print(
        f"config valid: {len(watch_handles)} paid KOL, "
        f"{len(terms)} identity terms, start={config.get('campaign_start_at')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

