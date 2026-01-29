#!/usr/bin/env bash
#
# Lightweight `6_stac_metadata_search.sh` helper for the EDITO STAC catalog.
# Fast, client-side substring scan over collections/items/assets.
# Supports optional collection filtering, listing, and controlling pagination.
# This script fetches collections and `/collections/{id}/items` pages and runs regex/substring
# matching locally rather than delegating to the STAC `/search` endpoint.

set -euo pipefail

trap 'exit 0' PIPE

DEFAULT_BASE="https://api.dive.edito.eu/data"
DEFAULT_OUTPUT_DIR="data/stac_metadata_search"
DEFAULT_FORMAT="json"

BASE="${BASE:-$DEFAULT_BASE}"
ITEM_LIMIT_PER_COLLECTION="${ITEM_LIMIT_PER_COLLECTION:-200}"
MAX_PAGES_PER_COLLECTION="${MAX_PAGES_PER_COLLECTION:-50}"
STOP_ON_FIRST_HIT="${STOP_ON_FIRST_HIT:-0}"

ITEM_QUERY=""
ALL_COLLECTIONS="0"
LIST_COLLECTIONS="0"
COLLECTIONS=""
FORMAT="$DEFAULT_FORMAT"
TITLE=""
OUTPUT=""
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"

_usage() {
  cat <<'EOF'
Usage: 6_stac_metadata_search.sh [OPTIONS] [QUERY]

Options:
  --query TEXT              Same as providing TEXT as positional argument.
  --collections ID[,ID...]  Comma-separated collection ids or regex patterns to scan.
  --all-collections         Ignore the collection filters and scan every collection.
  --list-collections        Print the selected collections and exit.
  --limit-per-collection N  Items per page when scanning collections (default 200).
  --max-pages N             Pages per collection cap (default 50).
  --no-stop-on-first-hit    Scan all pages even after matches (default disabled).
  --format FORMAT           Format for saved results (json or csv). Default json.
  --title TITLE             Base title for the saved results file (no extension).
  --output PATH             Exact path where to write the results file.
  -h, --help                Show this help message.

If no QUERY is supplied (and --list-collections is omitted) the script just lists the selected collections.

Example:
# Search for "westerschelde" in all items and fields in all collections
bash 6_stac_metadata_search.sh "westerschelde"

# search for "Koster historical" in the collections with one of "bio", "ecology", or "biology" in any field
bash 6_stac_metadata_search.sh "Koster historical" --collections "bio|ecology|biology"

# search for "gbif.org" in items of collections whose metadata has 'biology', 'bio', 'biodiversity', or 'ecology' in any field
bash 6_stac_metadata_search.sh "gbif.org" --collections "bio|biodiversity|ecology"

# save the results to a file with the title "Koster historical" in json format
bash 6_stac_metadata_search.sh "Koster historical" --title "Koster historical"

# save the results to a file with the title "myDOI" in json format
bash 6_stac_metadata_search.sh "doi.org/10.5281" --output "data/stac_metadata_search/myDOI.json"

# search for acoustic|tracking|ecology in collections and "acoustic" in items save to csv
bash 6_stac_metadata_search.sh "acoustic" --collections "acoustic|tracking|ecology" --format csv

# Do deep search and set max pages to 100 (200 records x 100 pages = 20,000 records per collection) takes forever
bash 6_stac_metadata_search.sh "Koster historical" --max-pages 100
EOF
}

_trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

_safe_echo() {
  if ! echo "$@"; then
    exit 0
  fi
}

_safe_printf() {
  if ! printf -- "$@"; then
    exit 0
  fi
}

### argument parsing helpers ###
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --help|-h)
        _usage
        exit 0
        ;;
      --query)
        shift
        ITEM_QUERY="$1"
        shift
        ;;
      --collections)
        shift
        COLLECTIONS="$1"
        shift
        ;;
      --all-collections)
        ALL_COLLECTIONS="1"
        shift
        ;;
      --list-collections)
        LIST_COLLECTIONS="1"
        shift
        ;;
      --limit-per-collection)
        shift
        ITEM_LIMIT_PER_COLLECTION="$1"
        shift
        ;;
      --max-pages)
        shift
        MAX_PAGES_PER_COLLECTION="$1"
        shift
        ;;
      --format)
        shift
        FORMAT="$1"
        shift
        ;;
      --title)
        shift
        TITLE="$1"
        shift
        ;;
      --output)
        shift
        OUTPUT="$1"
        shift
        ;;
      --stop-on-first-hit)
        STOP_ON_FIRST_HIT="1"
        shift
        ;;
      --no-stop-on-first-hit)
        STOP_ON_FIRST_HIT="0"
        shift
        ;;
      --)
        shift
        break
        ;;
      -*)
        echo "Unknown option: $1" >&2
        _usage
        exit 1
        ;;
      *)
        if [[ -z "$ITEM_QUERY" ]]; then
          ITEM_QUERY="$1"
          shift
        else
          echo "Unexpected argument: $1" >&2
          _usage
          exit 1
        fi
        ;;
    esac
  done
}

fetch_collections() {
  curl -sS "$BASE/collections"
}

### build collection terms by regex patterns ####
build_collection_terms() {
  collection_terms=()
  if [[ -n "$COLLECTIONS" ]]; then
    IFS=',' read -ra provided <<< "$COLLECTIONS"
    for raw in "${provided[@]}"; do
      trimmed="$(_trim "$raw")"
      if [[ -n "$trimmed" ]]; then
        collection_terms+=("$trimmed")
      fi
    done
  fi
}

### select collections by regex patterns or all collections ####
select_collections() {
  local json="$1"
  declare -gA seen
  selected_collections=()
  collection_heading=""

  if [[ "$ALL_COLLECTIONS" == "1" ]]; then
    collection_heading="== All collections =="
    mapfile -t selected_collections < <(echo "$json" | jq -r '.collections[].id')
  elif [[ ${#collection_terms[@]} -gt 0 ]]; then
    collection_heading="== Collections matching: $COLLECTIONS =="
    for term in "${collection_terms[@]}"; do
      matches="$(echo "$json" | jq -r --arg re "$term" '
        .collections
        | map(select(
            ((.id // "") | test($re; "i")) or
            ((.title // "") | test($re; "i")) or
            ((.description // "") | test($re; "i")) or
            ((.keywords // [] | join(" ")) | test($re; "i")) or
            ((.summaries // {} | tostring) | test($re; "i"))
          ))
        | .[].id
      ')"
      while IFS= read -r cid; do
        if [[ -z "$cid" || -n "${seen[$cid]:-}" ]]; then
          continue
        fi
        seen["$cid"]=1
        selected_collections+=("$cid")
      done <<< "$matches"
    done
  else
    collection_heading="== All collections =="
    mapfile -t selected_collections < <(echo "$json" | jq -r '.collections[].id')
  fi
}

parse_args "$@"

if [[ "$FORMAT" != "json" && "$FORMAT" != "csv" ]]; then
  echo "format must be json or csv" >&2
  exit 1
fi

# fetch collections and build collection terms
collections_json="$(fetch_collections)"
build_collection_terms
select_collections "$collections_json"

if [[ ${#selected_collections[@]} -eq 0 ]]; then
  _safe_echo "$collection_heading"
  _safe_echo "No collections matched."
  exit 0
fi

_print_selected_collections() {
  _safe_echo "$collection_heading"
  for cid in "${selected_collections[@]}"; do
    title="$(echo "$collections_json" | jq -r --arg id "$cid" '.collections[] | select(.id == $id) | .title // "-"')"
    _safe_printf '%s | %s\n' "$cid" "${title:-"-"}"
  done
}

# print selected collections
if [[ "$LIST_COLLECTIONS" == "1" || -z "$ITEM_QUERY" ]]; then
  if [[ -t 1 ]]; then
    _print_selected_collections
  else
    _print_selected_collections 2>/dev/null
  fi
  if [[ -z "$ITEM_QUERY" && -t 1 ]]; then
    _safe_echo
    _safe_echo "Tip: pass a query to also search within items/assets of these collections."
    _safe_echo "Example: $0 \"gbif.org/dataset\""
  fi
  exit 0
fi

# print now scanning items/assets for query
_safe_echo
_safe_echo "== Now scanning items/assets for: ${ITEM_QUERY} =="
if [[ "$ALL_COLLECTIONS" == "1" ]]; then
  _safe_echo "(All collections)"
elif [[ ${#collection_terms[@]} -gt 0 ]]; then
  _safe_echo "(Collections matching: $COLLECTIONS)"
else
  _safe_echo "(All collections)"
fi
_safe_echo

# prepare results temporary file
ITEM_QUERY_LOWER="$(printf '%s' "$ITEM_QUERY" | LC_ALL=C tr '[:upper:]' '[:lower:]')"
# RESULTS_TEMP collects per-page JSON matches before aggregating into the final output.
RESULTS_TEMP="$(mktemp)"
trap 'rm -f "$RESULTS_TEMP"' EXIT

# for each selected collection, page through items and search
for cid in "${selected_collections[@]}"; do
  # For each chosen collection we page through items with 
  # `--limit-per-collection` and optional early exit on hits.
  [[ -z "$cid" ]] && continue
  _safe_echo "--------------------------------------------------------------------------------"
  _safe_echo "COLLECTION: $cid"

  next_url="$BASE/collections/$cid/items?limit=$ITEM_LIMIT_PER_COLLECTION"
  page=0
  hits=0

  while [[ -n "${next_url:-}" && $page -lt $MAX_PAGES_PER_COLLECTION ]]; do
    page=$((page + 1))
    item_json="$(curl -sS "$next_url")"

    page_matches="$(mktemp)"
    echo "$item_json" | jq -c --arg q "$ITEM_QUERY_LOWER" '
    [ (.features // [])
    | map(select(type=="object"))
    | map(select(([.. | strings | ascii_downcase] | any(contains($q)))))
    | .[]
    | {
      id,
      product_identifier: (.properties.productIdentifier // null),
      item_title: (.properties.title // null),
      collection: (.collection // null),
      datetime: (.properties.datetime // .properties.start_datetime // null),
      api_item_url: ("'"$BASE"'/collections/" + (.collection // "'"$cid"'") + "/items/" + .id),
      viewer_url: ("https://viewer.dive.edito.eu/feature/https:~2F~2Fapi.dive.edito.eu~2Fdata~2Fcollections~2F" + (.collection // "'"$cid"'") + "~2Fitems~2F" + .id),
      matching_assets: (
        (.assets // {})
        | (if type=="object" then to_entries else [] end)
        | map(select(.value | type=="object"))
        | map(select(
          ((.value.href // "") | ascii_downcase | contains($q)) or
          ((.value.title // "") | ascii_downcase | contains($q)) or
          ((.key // "") | ascii_downcase | contains($q))
        ))
        | map({key:.key, type:(.value.type//null), href:(.value.href//null)})
      )
    }
    ]
    ' >"$page_matches"

    page_hits="$(jq 'length' "$page_matches")"
    hits=$((hits + page_hits))
    if [[ "$page_hits" -gt 0 ]]; then
      jq -c '.[]' "$page_matches" >> "$RESULTS_TEMP"
    fi

    jq -r '
      .[]
      | "MATCH item_id=\(.id) datetime=\(.datetime // "-")\n api: \(.api_item_url)\n view: \(.viewer_url)\n"
      + (
        if (.matching_assets | length) > 0 then
          (" assets:\n" + (.matching_assets[] | " - \(.key) | \(.type // "-") | \(.href // "-")"))
        else
          " assets: (no asset href/title matched query)\n"
        end
      )
    ' "$page_matches"
    rm -f "$page_matches"

    if [[ "${STOP_ON_FIRST_HIT}" == "1" && $page_hits -gt 0 ]]; then
      next_url=""
      break
    fi

    next_url="$(echo "$item_json" | jq -r '(.links // []) | map(select(.rel=="next")) | .[0].href // empty')"
  done

  _safe_echo "Hits in collection $cid: $hits"
  _safe_echo
done

if [[ -s "$RESULTS_TEMP" ]]; then
  selected_ids_json="$(printf '%s\n' "${selected_collections[@]}" | jq -R -s 'split("\n") | map(select(length > 0))')"
else
  selected_ids_json="[]"
fi
collections_info="$(echo "$collections_json" | jq -c --argjson ids "$selected_ids_json" '
  .collections
  | map(select(.id as $id | $ids | index($id)))
  | map({id: .id, title: .title})
')"

filter_mode="collections"
filter_value="$COLLECTIONS"
if [[ "$ALL_COLLECTIONS" == "1" || ${#collection_terms[@]} -eq 0 ]]; then
  filter_mode="all"
  filter_value=""
fi

flatten_results_csv() {
  printf '%s\n' "collection,item_id,product_identifier,item_title,datetime,api_item_url,viewer_url,asset_key,asset_type,asset_href"
  jq -rc '
    (.matching_assets // []) as $assets
    | {
        collection: (.collection // ""),
        item_id: (.id // ""),
        product_identifier: (.product_identifier // ""),
        item_title: (.item_title // ""),
        datetime: (.datetime // ""),
        api_item_url: (.api_item_url // ""),
        viewer_url: (.viewer_url // "")
      } as $meta
    | if ($assets | length) == 0 then
        [
          $meta.collection,
          $meta.item_id,
          $meta.product_identifier,
          $meta.item_title,
          $meta.datetime,
          $meta.api_item_url,
          $meta.viewer_url,
          "",
          "",
          ""
        ]
      else
        $assets[]
        | [
            $meta.collection,
            $meta.item_id,
            $meta.product_identifier,
            $meta.item_title,
            $meta.datetime,
            $meta.api_item_url,
            $meta.viewer_url,
            (.key // ""),
            (.type // ""),
            (.href // "")
          ]
      end
    | @csv
  ' "$RESULTS_TEMP"
}

write_json_output() {
  jq -n \
    --arg base "$BASE" \
    --arg query "$ITEM_QUERY" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg format "$FORMAT" \
    --arg filter_mode "$filter_mode" \
    --arg filter_value "$filter_value" \
    --argjson collections "$collections_info" \
    --slurpfile items "$RESULTS_TEMP" \
    '{
      base: $base,
      query: $query,
      generated_at: $generated_at,
      format: $format,
      filter_mode: $filter_mode,
      filter_value: $filter_value,
      collections: $collections,
      items: $items
    }'
}

### output helpers ####

final_json="$(write_json_output)"

if [[ -n "$OUTPUT" ]]; then
  output_file="$OUTPUT"
else
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  title="${TITLE:-search_results_$timestamp}"
  mkdir -p "$OUTPUT_DIR"
  output_file="$OUTPUT_DIR/$title.$FORMAT"
fi

if [[ "$FORMAT" == "json" ]]; then
  printf '%s\n' "$final_json" > "$output_file"
else
  flatten_results_csv > "$output_file"
fi

_safe_echo
_safe_echo "Results saved to $output_file"
