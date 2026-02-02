#!/usr/bin/env python3
"""
## Simple STAC substring search (Python equivalent of `stac_metadata_search.sh`)

This script mirrors the Bash helper by fetching collection/item pages from the EDITO STAC
API and scanning every string value in the response JSON for the provided substring. Client-side
filtering (via `--collections` patterns or `--all-collections`) and request pagination are handled
entirely in RAM before the final JSON/CSV export, and no extra data is persisted to the server.

The fastest “just search the JSON” workflow is `stac_metadata_search.py`. It:
- lists collections from the EDITO STAC API
- optionally filters those collections via `--collections` regex patterns before scanning
- scans `/collections/{id}/items` and matches the query against **any string value** in the item JSON
- additionally reports which **assets** matched by `key`, `title`, or `href`
- additionally reports the **product identifier** and **item title** of the STAC item
- chooses the best output format based on the `--format` argument (json or csv)
Examples:

```bash
# List all collections
python 6_stac_metadata_search.py --list-collections

# Search ALL collections for a simple substring (like your bash example)
python 6_stac_metadata_search.py "Koster historical" --all-collections

# Fast default (like `searchdataset.sh`): scan all collections
python 6_stac_metadata_search.py "Koster historical"

# Restrict collections with a regex-like pattern (optional)
python 6_stac_metadata_search.py "gbif.org" --collections "biology|bio|ecology"

# search for acoustic|tracking|ecology in collections and "acoustic" in items save to csv with title "acoustic"
python 6_stac_metadata_search.py "acoustic" --collections "acoustic|tracking|ecology" --format csv --title "acoustic"

# Scan stac items in all collections for "ipt.gbif.org.nz/resource?r=koster_historica"
python 6_stac_metadata_search.py "ipt.gbif.org.nz/resource?r=koster_historica"

# Only scan specific collections (comma-separated)
python 6_stac_metadata_search.py "Koster historical" --collections "bio|ecology|biology"

# Save the results to a file with the title "Koster historical" in json format
python 6_stac_metadata_search.py "Koster historical" --title "Koster historical"

# Save the results to a file with the title "myDOI" in json format
python 6_stac_metadata_search.py "doi.org/10.5281" --title "myDOI" --format json

# Save the results to a file with the title "myDOI" in csv format
python 6_stac_metadata_search.py "doi.org/10.5281" --title "myDOI" --format csv

# Do deep search and set max pages to 100 (200 records x 100 pages = 20,000 records per collection) takes forever
python 6_stac_metadata_search.py "Koster historical" --max-pages 100
```

Notes:
- `BASE` can be overridden via env var, same as the bash script: `BASE="https://api.dive.edito.eu/data" ...`
- The server-side STAC `/search` does support some parameters; see the OpenAPI docs at `https://rest.wiki/?https://api.dive.edito.eu/data/api`.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import requests


DEFAULT_BASE = "https://api.dive.edito.eu/data"
DEFAULT_OUTPUT_DIR = Path("data/stac_metadata_search")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _compile_optional_regex(pattern: Optional[str]) -> Optional[re.Pattern[str]]:
    if not pattern:
        return None
    try:
        return re.compile(pattern, flags=re.IGNORECASE)
    except re.error:
        return re.compile(re.escape(pattern), flags=re.IGNORECASE)


def _iter_strings(value: Any) -> Iterable[str]:
    """Yield all string *values* recursively (not dict keys)."""
    if value is None:
        return
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, dict):
        for v in value.values():
            yield from _iter_strings(v)
        return
    if isinstance(value, (list, tuple)):
        for v in value:
            yield from _iter_strings(v)
        return


def _contains_term(value: Any, term_lower: str) -> bool:
    for s in _iter_strings(value):
        if term_lower in s.lower():
            return True
    return False


def _collection_matches(collection: dict[str, Any], rx: re.Pattern[str]) -> bool:
    """Return true when the regex matches IDs, titles, descriptions, keywords, or summaries."""
    # Mirror the bash script: search in common fields.
    candidates: list[str] = []
    for key in ("id", "title", "description"):
        v = collection.get(key)
        if isinstance(v, str) and v:
            candidates.append(v)
    keywords = collection.get("keywords")
    if isinstance(keywords, list):
        candidates.append(" ".join(str(x) for x in keywords if x is not None))
    summaries = collection.get("summaries")
    if summaries is not None:
        candidates.append(str(summaries))
    blob = " ".join(candidates)
    return bool(rx.search(blob))


def _get_next_link(feature_collection: dict[str, Any]) -> str | None:
    links = feature_collection.get("links") or []
    if not isinstance(links, list):
        return None
    for link in links:
        if not isinstance(link, dict):
            continue
        if link.get("rel") == "next" and isinstance(link.get("href"), str):
            return link["href"]
    return None


def _viewer_url(collection_id: str, item_id: str) -> str:
    # Keep the exact viewer URL style used in the bash script.
    return (
        "https://viewer.dive.edito.eu/feature/https:~2F~2Fapi.dive.edito.eu~2Fdata~2Fcollections~2F"
        + collection_id
        + "~2Fitems~2F"
        + item_id
    )


@dataclass(frozen=True)
class Match:
    item_id: str
    collection: str
    product_identifier: str
    item_title: str
    datetime: str | None
    api_item_url: str
    viewer_url: str
    matching_assets: list[dict[str, Any]]


def _extract_datetime(feature: dict[str, Any]) -> str | None:
    props = feature.get("properties") or {}
    if not isinstance(props, dict):
        return None
    dt = props.get("datetime")
    if isinstance(dt, str) and dt:
        return dt
    dt = props.get("start_datetime")
    if isinstance(dt, str) and dt:
        return dt
    return None


def _matching_assets(feature: dict[str, Any], term_rx: re.Pattern[str]) -> list[dict[str, Any]]:
    """Capture assets whose key/title/href match the query regex, mirroring the printed hit context."""
    assets = feature.get("assets") or {}
    if not isinstance(assets, dict):
        return []

    matches: list[dict[str, Any]] = []
    for key, asset in assets.items():
        if not isinstance(asset, dict):
            continue
        href = asset.get("href")
        title = asset.get("title")
        if (
            (isinstance(key, str) and term_rx.search(key))
            or (isinstance(href, str) and term_rx.search(href))
            or (isinstance(title, str) and term_rx.search(title))
        ):
            matches.append(
                {
                    "key": key,
                    "type": asset.get("type"),
                    "href": href,
                }
            )
    return matches


def _ensure_output_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _format_assets_csv(assets: list[dict[str, Any]]) -> str:
    if not assets:
        return ""
    return ";".join(
        f"{asset.get('key','-')}|{asset.get('type','-')}|{asset.get('href','-')}"
        for asset in assets
    )


def _determine_output_path(args: argparse.Namespace) -> Path:
    if args.output:
        return Path(args.output)
    title = args.title or datetime.utcnow().strftime("search_results_%Y%m%dT%H%M%S")
    return DEFAULT_OUTPUT_DIR / f"{title}.{args.format}"


def _save_results_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_output_parent(path)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def _save_results_csv(path: Path, matches: list[Match]) -> None:
    _ensure_output_parent(path)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "collection",
                "item_id",
                "product_identifier",
                "item_title",
                "datetime",
                "api_item_url",
                "viewer_url",
                "matching_assets",
            ]
        )
        for match in matches:
            writer.writerow(
                [
                    match.collection,
                    match.item_id,
                    match.product_identifier or "",
                    match.item_title or "",
                    match.datetime or "",
                    match.api_item_url,
                    match.viewer_url,
                    _format_assets_csv(match.matching_assets),
                ]
            )


def _print_collection_list(collections: list[dict[str, Any]]) -> None:
    for c in collections:
        cid = c.get("id", "-")
        title = c.get("title", "-")
        print(f"{cid} | {title}")


def _get_collections(session: requests.Session, base: str) -> list[dict[str, Any]]:
    r = session.get(f"{base}/collections", timeout=60)
    r.raise_for_status()
    payload = r.json()
    cols = payload.get("collections") or []
    if not isinstance(cols, list):
        return []
    return [c for c in cols if isinstance(c, dict)]


def _scan_collection_items(
    *,
    session: requests.Session,
    base: str,
    collection_id: str,
    query: str,
    limit_per_page: int,
    max_pages: int,
    stop_on_first_hit: bool,
) -> tuple[list[Match], int]:
    """Page through `/collections/{id}/items`, scan every string value for the query, and stop per CLI caps."""
    logger.debug(
        "Preparing scan for collection %s (limit=%d/max_pages=%d/stop=%s)",
        collection_id,
        limit_per_page,
        max_pages,
        stop_on_first_hit,
    )
    term_lower = query.lower()
    term_rx = re.compile(re.escape(query), flags=re.IGNORECASE)

    next_url: str | None = f"{base}/collections/{collection_id}/items?limit={limit_per_page}"
    page = 0
    hits = 0
    matches: list[Match] = []

    while next_url and page < max_pages:
        page += 1
        logger.debug("Fetching collection %s page %d", collection_id, page)
        r = session.get(next_url, timeout=60)
        r.raise_for_status()
        fc = r.json()

        features = fc.get("features") or []
        if not isinstance(features, list):
            features = []

        page_matches: list[Match] = []
        for feature in features:
            if not isinstance(feature, dict):
                continue
            if not _contains_term(feature, term_lower):
                continue

            item_id = str(feature.get("id", ""))
            product_identifier = str(feature.get("properties", {}).get("productIdentifier", ""))
            item_title = str(feature.get("properties", {}).get("title", ""))
            if not item_id or not product_identifier or not item_title:
                continue
            item_collection = str(feature.get("collection") or collection_id)

            page_matches.append(
                Match(
                    item_id=item_id,
                    product_identifier=product_identifier,
                    item_title=item_title,
                    collection=item_collection,
                    datetime=_extract_datetime(feature),
                    api_item_url=f"{base}/collections/{item_collection}/items/{item_id}",
                    viewer_url=_viewer_url(item_collection, item_id),
                    matching_assets=_matching_assets(feature, term_rx),
                )
            )

        if page_matches:
            hits += len(page_matches)
            matches.extend(page_matches)
            if stop_on_first_hit:
                break

        next_url = _get_next_link(fc)

    return matches, hits


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the substring search workflow.

    It parses user inputs (query, collection filters, pagination caps, output preferences)
    and drives the page-by-page STAC item scan, stopping early if requested.
    Unlike the STAC `/search` endpoint, collection filtering and substring matching are
    handled locally via regex matching and client-side evaluation of every string value.
    """
    parser = argparse.ArgumentParser(
        prog="6_stac_metadata_search.py",
        description="Simple substring search over EDITO STAC collections/items (Python version of 6_stac_metadata_search.sh).",
    )
    parser.add_argument(
        "query",
        nargs="?",
        default="",
        help="Substring to search for inside items/assets (case-insensitive). If omitted, only lists collections.",
    )
    parser.add_argument(
        "--base",
        default=os.environ.get("BASE", DEFAULT_BASE),
        help=f"STAC API base URL. Default: {DEFAULT_BASE} (or env BASE).",
    )
    parser.add_argument(
        "--collections",
        default="",
        help=(
            "Comma-separated collection patterns to scan. "
            "Each pattern is treated as a regex against id/title/description/keywords/summaries."
        ),
    )
    parser.add_argument(
        "--all-collections",
        action="store_true",
        help="Scan every collection, ignoring --collections filters.",
    )
    parser.add_argument(
        "--list-collections",
        action="store_true",
        help="List the selected collections (after applying --collections filters) and exit.",
    )
    parser.add_argument(
        "--limit-per-page",
        type=int,
        default=int(os.environ.get("ITEM_LIMIT_PER_COLLECTION", "200")),
        help="Items per page when scanning a collection (default 200; env ITEM_LIMIT_PER_COLLECTION).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=int(os.environ.get("MAX_PAGES_PER_COLLECTION", "50")),
        help="Safety cap on pages per collection (default 50; env MAX_PAGES_PER_COLLECTION).",
    )
    parser.add_argument(
        "--stop-on-first-hit",
        action=argparse.BooleanOptionalAction,
        default=(os.environ.get("STOP_ON_FIRST_HIT", "0") != "0"),
        help="Stop scanning a collection after the first page with matches (default: disabled).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="File format for saved hits (json or csv).",
    )
    parser.add_argument(
        "--title",
        default="",
        help=(
            "Title to use for the saved results file (no extension). "
            "Default: generated name under data/stac_dataset_search."
        ),
    )
    parser.add_argument(
        "--output",
        default="",
        help="Exact path to write the JSON results file (overrides --title/default).",
    )

    args = parser.parse_args(argv)
    collection_terms = [c.strip() for c in args.collections.split(",") if c.strip()]

    with requests.Session() as session:
        all_collections = _get_collections(session, args.base)
        logger.info("Fetched %d collections from %s", len(all_collections), args.base)

        # Determine which collections to scan based on --collections patterns or --all-collections.
        selected: list[dict[str, Any]]
        if args.all_collections or not collection_terms:
            selected = list(all_collections)
            logger.info("Selecting all %d collections", len(selected))
        else:
            seen: set[str] = set()
            selected = []
            logger.info("Selecting collections matching patterns: %s", ", ".join(collection_terms))
            for term in collection_terms:
                rx = _compile_optional_regex(term)
                if rx is None:
                    continue
                for coll in all_collections:
                    cid = str(coll.get("id", "")).strip()
                    if not cid or cid in seen:
                        continue
                    if _collection_matches(coll, rx):
                        selected.append(coll)
                        seen.add(cid)
            logger.info("Selected %d collections", len(selected))

        if not selected:
            print("No collections selected.")
            return 0

        if args.list_collections or not args.query:
            if args.all_collections or not collection_terms:
                print("== All collections ==")
            else:
                print(f"== Collections matching: {args.collections} ==")
            _print_collection_list(selected)

            if not args.query:
                print()
                print(
                    'Tip: pass a query to also search within items/assets, e.g. `6_stac_metadata_search.py "gbif.org/dataset"`.')
            return 0

        print(f"== Now scanning items/assets for: {args.query} ==")
        if args.all_collections or not collection_terms:
            print("(All collections)")
        else:
            print(f"(Collections matching: {args.collections})")
        print()

        total_hits = 0
        all_matches: list[Match] = []
        # Walk through each selected collection and run the substring scan until pagination 
        # limits or if `--stop-on-first-hit` is enabled.
        for c in selected:
            cid = str(c.get("id", "")).strip()
            if not cid:
                continue
            print("-" * 80)
            print(f"COLLECTION: {cid}")
            logger.info(
                "Scanning collection %s (limit=%d, max_pages=%d, stop_on_first=%s)",
                cid,
                args.limit_per_page,
                args.max_pages,
                args.stop_on_first_hit,
            )
            collection_matches, hits = _scan_collection_items(
                session=session,
                base=args.base,
                collection_id=cid,
                query=args.query,
                limit_per_page=args.limit_per_page,
                max_pages=args.max_pages,
                stop_on_first_hit=args.stop_on_first_hit,
            )
            total_hits += hits
            for m in collection_matches:
                dt = m.datetime or "-"
                print(f"MATCH item_id={m.item_id} datetime={dt}")
                print(f" api: {m.api_item_url}")
                print(f" view: {m.viewer_url}")
                if m.matching_assets:
                    print(" assets:")
                    for a in m.matching_assets:
                        print(
                            f" - {a.get('key')} | {a.get('type', '-')} | {a.get('href', '-')}")
                else:
                    print(" assets: (no asset href/title/key matched query)")
                print()
            print(f"Hits in collection {cid}: {hits}")
            print()
            all_matches.extend(collection_matches)

        if total_hits == 0:
            print("No matches found.")

        # Persist search metadata/results using the CLI's output/path settings.
        output_path = _determine_output_path(args)
        filter_mode = (
            "all"
            if args.all_collections or not collection_terms
            else "collections"
        )
        filter_value = collection_terms if filter_mode == "collections" else None

        metadata = {
            "generated_at": datetime.utcnow().isoformat(),
            "base": args.base,
            "query": args.query,
            "format": args.format,
            "filter_mode": filter_mode,
            "filter_value": filter_value,
            "limit_per_page": args.limit_per_page,
            "max_pages": args.max_pages,
            "stop_on_first_hit": args.stop_on_first_hit,
            "collections": [
                {"id": str(coll.get("id", "")), "title": coll.get("title")}
                for coll in selected
            ],
            "total_hits": total_hits,
        }

        if args.format == "json":
            payload = {
                **metadata,
                "matches": [asdict(match) for match in all_matches],
            }
            _save_results_json(output_path, payload)
        else:
            _save_results_csv(output_path, all_matches)

        logger.info("Wrote %d matches to %s", len(all_matches), output_path)
        print(f"Results written to {output_path}")
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        # Allow piping into tools like `head` without a traceback.
        raise SystemExit(0)
