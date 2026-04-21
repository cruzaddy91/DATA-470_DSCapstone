#!/bin/zsh

set -euo pipefail

readonly TARGET="${1-}"
readonly INTERVAL_SECONDS="${INTERVAL_SECONDS:-60}"
readonly WARN_RSS_KB="${WARN_RSS_KB:-6291456}"
readonly KILL_RSS_KB="${KILL_RSS_KB:-10485760}"

if [[ -z "$TARGET" ]]; then
  echo "Usage: $0 <pid|process-name-pattern>" >&2
  exit 1
fi

typeset -A WARNED_PIDS
seen_match=0

notify() {
  local title="$1"
  local message="$2"

  /usr/bin/osascript - "$title" "$message" <<'APPLESCRIPT' >/dev/null 2>&1 || true
on run argv
  set notificationTitle to item 1 of argv
  set notificationMessage to item 2 of argv
  display notification notificationMessage with title notificationTitle
end run
APPLESCRIPT
}

timestamp() {
  /bin/date '+%Y-%m-%d %H:%M:%S'
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*"
}

is_pid_target() {
  [[ "$TARGET" == <-> ]]
}

resolve_pids() {
  if is_pid_target; then
    /bin/ps -p "$TARGET" -o pid= 2>/dev/null | /usr/bin/awk '{print $1}'
    return
  fi

  /usr/bin/pgrep -f "$TARGET" 2>/dev/null | /usr/bin/awk -v self="$$" '$1 != self { print $1 }'
}

rss_kb_for_pid() {
  local pid="$1"
  /bin/ps -o rss= -p "$pid" 2>/dev/null | /usr/bin/awk '{print $1}'
}

command_for_pid() {
  local pid="$1"
  /bin/ps -o command= -p "$pid" 2>/dev/null
}

rss_gb_string() {
  local rss_kb="$1"
  /usr/bin/awk -v rss_kb="$rss_kb" 'BEGIN { printf "%.2f", rss_kb / 1048576 }'
}

kill_pid() {
  local pid="$1"

  /bin/kill "$pid" 2>/dev/null || true
  /bin/sleep 5
  if /bin/kill -0 "$pid" 2>/dev/null; then
    /bin/kill -9 "$pid" 2>/dev/null || true
  fi
}

log "memguard watching target: $TARGET"
log "thresholds: warn=6.00 GB kill=10.00 GB interval=${INTERVAL_SECONDS}s"

while true; do
  pids="$(resolve_pids || true)"

  if [[ -z "$pids" ]]; then
    if is_pid_target; then
      log "PID $TARGET is no longer running; exiting."
      notify "Memguard stopped" "PID $TARGET is no longer running."
      exit 0
    fi

    if (( seen_match )); then
      log "No processes matching '$TARGET' remain; exiting."
      notify "Memguard stopped" "No processes matching '$TARGET' remain."
      exit 0
    fi

    log "Waiting for a process matching '$TARGET'..."
    /bin/sleep "$INTERVAL_SECONDS"
    continue
  fi

  seen_match=1

  for pid in ${(f)pids}; do
    rss_kb="$(rss_kb_for_pid "$pid")"
    if [[ -z "$rss_kb" ]]; then
      continue
    fi

    command_line="$(command_for_pid "$pid")"
    rss_gb="$(rss_gb_string "$rss_kb")"
    log "pid=$pid rss=${rss_gb}GB command=${command_line}"

    if (( rss_kb >= KILL_RSS_KB )); then
      log "pid=$pid exceeded kill threshold; terminating."
      notify "Memguard killed process" "PID $pid reached ${rss_gb} GB RSS and was terminated."
      kill_pid "$pid"
      unset "WARNED_PIDS[$pid]" || true
      continue
    fi

    if (( rss_kb >= WARN_RSS_KB )); then
      if [[ -z "${WARNED_PIDS[$pid]-}" ]]; then
        log "pid=$pid exceeded warning threshold."
        notify "Memguard warning" "PID $pid is using ${rss_gb} GB RSS."
        WARNED_PIDS[$pid]=1
      fi
      continue
    fi

    unset "WARNED_PIDS[$pid]" || true
  done

  /bin/sleep "$INTERVAL_SECONDS"
done
