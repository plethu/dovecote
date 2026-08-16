#!/usr/bin/env bash
set -euo pipefail

readonly LOC_WARNING_THRESHOLD=500

while IFS= read -r file; do
  line_count="$(wc -l < "$file")"
  if (( line_count >= LOC_WARNING_THRESHOLD )); then
    printf 'warning: %s is %d lines; files at or above %d lines deserve a maintainability review\n' \
      "$file" "$line_count" "$LOC_WARNING_THRESHOLD" >&2
  fi
done < <(rg --files -g '*.rs' -g '!target/**' | sort)
