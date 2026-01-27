#!/usr/bin/env python3
"""
## Simple STAC substring search (Python equivalent of `stac_metadata_search.sh`)

The fastest “just search the JSON” workflow is `stac_metadata_search.py`. It:
- lists collections from the EDITO STAC API
- optionally filters collections by a regex over common collection fields
- scans `/collections/{id}/items` and matches your substring against **any string value** in the item JSON
- additionally reports which **assets** matched by `key`, `title`, or `href`

Examples:

```bash
# List all collections
python 6_stac_metadata_search.py --list-collections

# Search ALL collections for a simple substring (like your bash example)
python 6_stac_metadata_search.py "Koster historical" --all-collections

# Fast default (like `searchdataset.sh`): scan “biology-ish” collections
python 6_stac_metadata_search.py "Koster historical"

# Restrict collections with a regex (optional)
python 6_stac_metadata_search.py "gbif.org" --col-terms-regex "biology|bio|biodiversity|ecology"

# Find a STAC item by a substring of an asset URL (client-side scan)
python 6_stac_metadata_search.py "ipt.gbif.org.nz/resource?r=koster_historica"

# Only scan specific collections (comma-separated)
python 6_stac_metadata_search.py "Koster historical" --collections "some-collection-id,another-collection-id"
```

Notes:
- `BASE` can be overridden via env var, same as the bash script: `BASE="https://api.dive.edito.eu/data" ...`
- The server-side STAC `/search` supports many parameters; see the OpenAPI docs at `https://rest.wiki/?https://api.dive.edito.eu/data/api`.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import requests


DEFAULT_BASE = "https://api.dive.edito.eu/data"
DEFAULT_OUTPUT_DIR = Path("data/stac_dataset_search")
DEFAULT_COL_TERMS_REGEX = "biology|bio|biodiversity|ecology"


def _compile_optional_regex(pattern: Optional[str]) -> Optional[re.Pattern[str]]:
    if not pattern:
        return None
    return re.compile(pattern, flags=re.IGNORECASE)


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
    term_lower = query.lower()
    term_rx = re.compile(re.escape(query), flags=re.IGNORECASE)

    next_url: str | None = f"{base}/collections/{collection_id}/items?limit={limit_per_page}"
    page = 0
    hits = 0
    matches: list[Match] = []

    while next_url and page < max_pages:
        page += 1
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
            if not item_id:
                continue
            item_collection = str(feature.get("collection") or collection_id)

            page_matches.append(
                Match(
                    item_id=item_id,
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
        help="Comma-separated collection ids to scan. If omitted, scans all (or those matched by --col-terms-regex).",
    )
    parser.add_argument(
        "--all-collections",
        action="store_true",
        help="Ignore --col-terms-regex and scan all collections (unless --collections is provided).",
    )
    parser.add_argument(
        "--col-terms-regex",
        default=os.environ.get("COL_TERMS_REGEX", DEFAULT_COL_TERMS_REGEX),
        help=(
            "Regex to filter collections by id/title/description/keywords/summaries before scanning items. "
            f"Default: {DEFAULT_COL_TERMS_REGEX} (or env COL_TERMS_REGEX)."
        ),
    )
    parser.add_argument(
        "--list-collections",
        action="store_true",
        help="List the selected collections (after applying --collections/--col-terms-regex) and exit.",
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
        default=(os.environ.get("STOP_ON_FIRST_HIT", "1") != "0"),
        help="Stop scanning a collection after the first page with matches (default: enabled).",
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

    explicit_collections = [c.strip()
                            for c in args.collections.split(",") if c.strip()]
    rx = None if args.all_collections else _compile_optional_regex(
        args.col_terms_regex)

    with requests.Session() as session:
        all_collections = _get_collections(session, args.base)

        # Determine which collections to scan.
        selected: list[dict[str, Any]]
        if explicit_collections:
            wanted = set(explicit_collections)
            selected = [c for c in all_collections if str(
                c.get("id", "")) in wanted]
        elif rx is not None:
            selected = [
                c for c in all_collections if _collection_matches(c, rx)]
        else:
            selected = list(all_collections)

        if not selected:
            print("No collections selected.")
            return 0

        if args.list_collections or not args.query:
            if rx is not None:
                print(
                    f"== Collections matching regex: {args.col_terms_regex} ==")
            elif explicit_collections:
                print("== Selected collections (explicit) ==")
            else:
                print("== All collections ==")
            _print_collection_list(selected)

            if not args.query:
                print()
                print(
                    'Tip: pass a query to also search within items/assets, e.g. `6_stac_metadata_search.py "gbif.org/dataset"`.')
            return 0

        print(f"== Now scanning items/assets for: {args.query} ==")
        if rx is not None:
            print(f"(Collections filtered by regex: {args.col_terms_regex})")
        elif explicit_collections:
            print("(Collections selected explicitly)")
        else:
            print("(All collections)")
        print()

        total_hits = 0
        all_matches: list[Match] = []
        for c in selected:
            cid = str(c.get("id", "")).strip()
            if not cid:
                continue
            print("-" * 80)
            print(f"COLLECTION: {cid}")
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

        output_path = _determine_output_path(args)
        metadata = {
            "generated_at": datetime.utcnow().isoformat(),
            "base": args.base,
            "query": args.query,
            "format": args.format,
            "filter_mode": (
                "explicit"
                if explicit_collections
                else "regex"
                if rx is not None
                else "all"
            ),
            "filter_value": (
                explicit_collections
                if explicit_collections
                else args.col_terms_regex
                if rx is not None
                else None
            ),
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

        print(f"Results written to {output_path}")
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        # Allow piping into tools like `head` without a traceback.
        raise SystemExit(0)
