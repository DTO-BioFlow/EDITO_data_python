#!/usr/bin/env bash
#
# Lightweight `6_stac_metadata_search.sh` helper for the EDITO STAC catalog.
# Fast, client-side substring scan over collections/items/assets.
# Supports optional collection filtering, listing, and controlling pagination.

set -euo pipefail

trap 'exit 0' PIPE

DEFAULT_BASE="https://api.dive.edito.eu/data"
DEFAULT_COL_TERMS_REGEX="biology|bio|biodiversity|ecology"

DEFAULT_OUTPUT_DIR="data/stac_metadata_search"
DEFAULT_FORMAT="json"

BASE="${BASE:-$DEFAULT_BASE}"
COL_TERMS_REGEX="${COL_TERMS_REGEX:-$DEFAULT_COL_TERMS_REGEX}"
ITEM_LIMIT_PER_COLLECTION="${ITEM_LIMIT_PER_COLLECTION:-200}"
MAX_PAGES_PER_COLLECTION="${MAX_PAGES_PER_COLLECTION:-50}"
STOP_ON_FIRST_HIT="${STOP_ON_FIRST_HIT:-1}"

ITEM_QUERY=""
ALL_COLLECTIONS="0"
LIST_COLLECTIONS="0"
COLLECTIONS="${COLLECTIONS:-}"
FORMAT="$DEFAULT_FORMAT"
TITLE=""
OUTPUT=""
OUTPUT_DIR="${OUTPUT_DIR:-$DEFAULT_OUTPUT_DIR}"

_usage() {
  cat <<'EOF'
Usage: 6_stac_metadata_search.sh [OPTIONS] [QUERY]

Options:
  --query TEXT              Same as providing TEXT as positional argument.
  --collections ID[,ID...]  Comma-separated collection ids to scan (overrides the regex filter).
  --col-terms-regex REGEX   Regex to filter collections by id/title/description/keywords/summaries.
                            Defaults to "biology|bio|biodiversity|ecology" (can be overridden via COL_TERMS_REGEX).
  --all-collections         Ignore the regex filter and scan every collection.
  --list-collections        Print the selected collections and exit.
  --limit-per-collection N  Same as ITEM_LIMIT_PER_COLLECTION (default 200 or env override).
  --max-pages N             Same as MAX_PAGES_PER_COLLECTION (default 50 or env override).
  --stop-on-first-hit       Keep old behavior of stopping after the first page with matches (default).
  --no-stop-on-first-hit    Scan all pages even if matches were found.
  --format FORMAT           Format for saved results (json or csv). Default json.
  --title TITLE             Base title for the saved results file (no extension).
  --output PATH             Exact path where to write the results file.
  -h, --help                Show this help message.

Environment variables:
  BASE                    Overrides the STAC API base URL.
  ITEM_LIMIT_PER_COLLECTION
  MAX_PAGES_PER_COLLECTION
  STOP_ON_FIRST_HIT
  COLLECTIONS
  COL_TERMS_REGEX
  TITLE                    Base title for the saved results file (no extension).
  OUTPUT_DIR               Directory where results files are written (default data/stac_metadata_search)

If no QUERY is supplied (and --list-collections is omitted) the script just lists the selected collections.

Example:
# Search for "westerschelde" in all items and fields in all collections
bash 6_stac_metadata_search.sh "westerschelde"

# search for "Koster historical" in the collections "some-collection-id" and "another-collection-id"
bash 6_stac_metadata_search.sh "Koster historical" --collections "some-collection-id,another-collection-id"

# search for "gbif.org" in the collections that match the regex "biology|bio|biodiversity|ecology"
bash 6_stac_metadata_search.sh ""gbif.org" --col-terms-regex "biology|bio|biodiversity|ecology"

# save the results to a file with the title "Koster historical" in json format
bash 6_stac_metadata_search.sh "Koster historical" --title "Koster historical"

# save the results to a file with the title "myDOI" in json format
bash 6_stac_metadata_search.sh "doi.org/10.5281" --output "data/stac_metadata_search/myDOI.json"
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
    --col-terms-regex)
      shift
      COL_TERMS_REGEX="$1"
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

if [[ "$FORMAT" != "json" && "$FORMAT" != "csv" ]]; then
  echo "format must be json or csv" >&2
  exit 1
fi

collections_json="$(curl -sS "$BASE/collections")"

declare -a explicit_collections=()
if [[ -n "$COLLECTIONS" ]]; then
  IFS=',' read -ra provided <<< "$COLLECTIONS"
  for raw in "${provided[@]}"; do
    trimmed="$(_trim "$raw")"
    if [[ -n "$trimmed" ]]; then
      explicit_collections+=("$trimmed")
    fi
  done
fi

declare -a selected_collections=()
collection_heading=""

if [[ ${#explicit_collections[@]} -gt 0 ]]; then
  collection_heading="== Selected collections (explicit) =="
  declare -A seen
  for cid in "${explicit_collections[@]}"; do
    match="$(echo "$collections_json" | jq -r --arg id "$cid" '.collections[] | select(.id == $id) | .id // empty')"
    if [[ -z "$match" ]]; then
      echo "Warning: collection '$cid' not found" >&2
      continue
    fi
    if [[ -n "${seen[$match]:-}" ]]; then
      continue
    fi
    seen["$match"]=1
    selected_collections+=("$match")
  done
elif [[ "$ALL_COLLECTIONS" == "1" ]]; then
  collection_heading="== All collections =="
  mapfile -t selected_collections < <(echo "$collections_json" | jq -r '.collections[].id')
else
  collection_heading="== Collections matching regex: $COL_TERMS_REGEX =="
  mapfile -t selected_collections < <(
    echo "$collections_json" | jq -r --arg re "$COL_TERMS_REGEX" '
      .collections
      | map(select(
          ((.id // "") | test($re; "i")) or
          ((.title // "") | test($re; "i")) or
          ((.description // "") | test($re; "i")) or
          ((.keywords // [] | join(" ")) | test($re; "i")) or
          ((.summaries // {} | tostring) | test($re; "i"))
        ))
      | .[].id
    '
  )
fi

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

ITEM_QUERY_LOWER="$(printf '%s' "$ITEM_QUERY" | LC_ALL=C tr '[:upper:]' '[:lower:]')"
RESULTS_TEMP="$(mktemp)"
trap 'rm -f "$RESULTS_TEMP"' EXIT

_safe_echo
_safe_echo "== Now scanning items/assets for: ${ITEM_QUERY} =="
if [[ ${#explicit_collections[@]} -gt 0 ]]; then
  _safe_echo "(Collections selected explicitly)"
elif [[ "$ALL_COLLECTIONS" == "1" ]]; then
  _safe_echo "(All collections)"
else
  _safe_echo "(Collections filtered by regex: $COL_TERMS_REGEX)"
fi
_safe_echo

# --- For each selected collection, page through items and search ---
for cid in "${selected_collections[@]}"; do
  [[ -z "$cid" ]] && continue
  _safe_echo "--------------------------------------------------------------------------------"
  _safe_echo "COLLECTION: $cid"

  next_url="$BASE/collections/$cid/items?limit=$ITEM_LIMIT_PER_COLLECTION"
  page=0
  hits=0

  while [[ -n "${next_url:-}" && $page -lt $MAX_PAGES_PER_COLLECTION ]]; do
    page=$((page + 1))
    item_json="$(curl -sS "$next_url")"

    matches="$(
      echo "$item_json" | jq -c --arg q "$ITEM_QUERY_LOWER" '
      [ (.features // [])
      | map(select(type=="object"))
      | map(select(([.. | strings | ascii_downcase] | any(contains($q)))))
      | .[]
      | {
        id,
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
      '
    )"

    page_hits="$(echo "$matches" | jq 'length')"
    hits=$((hits + page_hits))
    if [[ "$page_hits" -gt 0 ]]; then
      echo "$matches" | jq -c '.[]' >> "$RESULTS_TEMP"
    fi

    echo "$matches" | jq -r '
      .[]
      | "MATCH item_id=\(.id) datetime=\(.datetime // "-")\n api: \(.api_item_url)\n view: \(.viewer_url)\n"
      + (
        if (.matching_assets | length) > 0 then
          (" assets:\n" + (.matching_assets[] | " - \(.key) | \(.type // "-") | \(.href // "-")"))
        else
          " assets: (no asset href/title matched query)\n"
        end
      )
    '

    if [[ "${STOP_ON_FIRST_HIT}" == "1" && $page_hits -gt 0 ]]; then
      next_url=""
      break
    fi

    next_url="$(echo "$item_json" | jq -r '(.links // []) | map(select(.rel=="next")) | .[0].href // empty')"
  done

  _safe_echo "Hits in collection $cid: $hits"
  _safe_echo
done

matches_json="$(jq -s '.' "$RESULTS_TEMP")"
selected_ids_json="$(printf '%s\n' "${selected_collections[@]}" | jq -R -s 'split("\n") | map(select(length > 0))')"
collections_info="$(echo "$collections_json" | jq -c --argjson ids "$selected_ids_json" '
  .collections
  | map(select(.id as $id | $ids | index($id)))
  | map({id: .id, title: .title})
')"

filter_mode="regex"
filter_value="$COL_TERMS_REGEX"
if [[ ${#explicit_collections[@]} -gt 0 ]]; then
  filter_mode="explicit"
  filter_value=""
elif [[ "$ALL_COLLECTIONS" == "1" ]]; then
  filter_mode="all"
  filter_value=""
fi

final_json="$(jq -n \
  --arg base "$BASE" \
  --arg query "$ITEM_QUERY" \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg format "$FORMAT" \
  --arg filter_mode "$filter_mode" \
  --arg filter_value "$filter_value" \
  --argjson collections "$collections_info" \
  --argjson items "$matches_json" \
  '{
    base: $base,
    query: $query,
    generated_at: $generated_at,
    format: $format,
    filter_mode: $filter_mode,
    filter_value: $filter_value,
    collections: $collections,
    items: $items
  }')"

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
  {
    printf '"collection","item_id","datetime","api_item_url","viewer_url","matching_assets"\n'
    jq -r '
      .items[]
      | [
          .collection,
          .id,
          (.datetime // ""),
          .api_item_url,
          .viewer_url,
          (.matching_assets
            | map(
                [
                  (.key // "-"),
                  (.type // "-"),
                  (.href // "-")
                ]
                | join("|")
              )
            | join(";")
          )
        ]
      | @csv
    ' <<<"$final_json"
  } > "$output_file"
fi

_safe_echo
_safe_echo "Results saved to $output_file"
