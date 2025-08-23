#!/usr/bin/env bash
# Usage:
#   ./parse_log.sh logfile.txt > out.csv
#   cat logfile.txt | ./parse_log.sh > out.csv

awk -v OFS=',' '
# --- helpers (define before use for POSIX awk) ---
function reset_vars() {
  ts=""; dist=""; label=""; run=""; disk="";
  load=""; p50=""; p100="";
}
function flush_row() {
  if (ts != "") {
    print ts, dist, label, run, disk, load, p50, p100
  }
  reset_vars()
}

BEGIN {
  print "timestamp","distribution","label","run","disk","load_accepted_tps","batch_p50_ms","batch_p100_ms"
  in_section = 0
  reset_vars()
}

# Header lines like:
# ===== 2025-08-23T13:07:44+00:00 : normal baseline (run 4) : DISK=/dev/nvme6n1 =====
/^===== / {
  # finish previous block
  if (in_section) flush_row()
  in_section = 1

  line = $0
  sub(/^===== /,"",line)
  sub(/ =====$/,"",line)

  # split into: [1]=timestamp, [2]=middle, [3]=DISK=...
  n = split(line, parts, / : /)
  ts = parts[1]

  mid = (n>=2 ? parts[2] : "")
  disk_part = (n>=3 ? parts[3] : "")

  # dist = first word of mid
  split(mid, w, /[[:space:]]+/)
  dist = (w[1] ? w[1] : "")

  # label = mid without the first word and without "(run N)"
  label = mid
  sub(/^[^[:space:]]+[[:space:]]+/, "", label)           # drop first word
  sub(/[[:space:]]*\(run[[:space:]]+[0-9]+\).*/, "", label)
  sub(/^[[:space:]]+/, "", label); sub(/[[:space:]]+$/, "", label)

  # run number
  if (match(mid, /\(run[[:space:]]+([0-9]+)\)/, m)) { run = m[1] } else { run = "" }

  # disk
  disk = disk_part
  sub(/^DISK=/,"",disk)

  next
}

# metrics inside a section
in_section && /^load accepted/ {
  # load accepted = 376792 tx/s
  if (match($0, /load accepted[[:space:]]*=[[:space:]]*([0-9]+)/, m)) load = m[1]
  next
}

in_section && /^batch latency p50/ {
  if (match($0, /batch latency p50[[:space:]]*=[[:space:]]*([0-9]+)/, m)) p50 = m[1]
  next
}

in_section && /^batch latency p100/ {
  if (match($0, /batch latency p100[[:space:]]*=[[:space:]]*([0-9]+)/, m)) p100 = m[1]
  next
}

END {
  if (in_section) flush_row()
}
' "${1:-/dev/stdin}"
