#!/usr/bin/env bash
# scripts/start_ibgateway.sh
# Start IB Gateway in the background. Idempotent — no-op if already running.
#
# Assumes:
#   - IB Gateway installed at ~/Jts/ibgateway/1037 (the standard Linux installer layout)
#   - WSLg (or another X server) is available; this script does NOT set DISPLAY
#     because WSLg sets it automatically. If you're using VcXsrv, export DISPLAY
#     before calling this script.
#   - You will log in manually via the login window that pops up.
#
# Usage:
#   ./scripts/start_ibgateway.sh           # start if not running
#   ./scripts/start_ibgateway.sh --status  # show status, don't start
#   ./scripts/start_ibgateway.sh --stop    # stop a running instance
#   ./scripts/start_ibgateway.sh --logs    # tail the log
#
# Exit codes:
#   0  — already running, or started successfully, or requested action completed
#   1  — install not found, or failed to start
#   2  — no X display available (WSLg not running, or VcXsrv not set)

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
IBG_DIR="${HOME}/Jts/ibgateway/1037"
IBG_BIN="${IBG_DIR}/ibgateway"
LOG_DIR="${HOME}/worfin/logs"
LOG_FILE="${LOG_DIR}/ibgateway.log"
PID_FILE="${LOG_DIR}/ibgateway.pid"

# Colours (only if stdout is a terminal)
if [[ -t 1 ]]; then
    RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'
    CYAN=$'\033[36m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
else
    RED=""; GREEN=""; YELLOW=""; CYAN=""; BOLD=""; RESET=""
fi

log()  { echo "${CYAN}[ibgateway]${RESET} $*"; }
ok()   { echo "${GREEN}✓${RESET} $*"; }
warn() { echo "${YELLOW}⚠${RESET}  $*"; }
err()  { echo "${RED}✗${RESET} $*" >&2; }

# ── Find a running Gateway PID (if any) ──────────────────────────────────────
# We match on the install dir, not just "ibgateway", to avoid false positives
# from other Java processes or a previous dead PID file.
find_running_pid() {
    pgrep -f "${IBG_DIR}/.*install4j" 2>/dev/null | head -n1 || true
}

is_running() {
    local pid
    pid="$(find_running_pid)"
    [[ -n "${pid}" ]]
}

cmd_status() {
    local pid
    pid="$(find_running_pid)"
    if [[ -n "${pid}" ]]; then
        ok "IB Gateway is running (pid=${pid})"
        if [[ -f "${LOG_FILE}" ]]; then
            log "Log file: ${LOG_FILE}"
        fi
        return 0
    fi
    warn "IB Gateway is not running"
    return 0
}

cmd_stop() {
    local pid
    pid="$(find_running_pid)"
    if [[ -z "${pid}" ]]; then
        warn "IB Gateway is not running — nothing to stop"
        rm -f "${PID_FILE}"
        return 0
    fi
    log "Stopping IB Gateway (pid=${pid})…"
    kill "${pid}" 2>/dev/null || true
    # Wait up to 10s for graceful exit
    for _ in {1..10}; do
        sleep 1
        if ! kill -0 "${pid}" 2>/dev/null; then
            ok "Stopped"
            rm -f "${PID_FILE}"
            return 0
        fi
    done
    warn "Still alive after 10s — sending SIGKILL"
    kill -9 "${pid}" 2>/dev/null || true
    rm -f "${PID_FILE}"
    ok "Killed"
}

cmd_logs() {
    if [[ ! -f "${LOG_FILE}" ]]; then
        err "No log file at ${LOG_FILE}"
        return 1
    fi
    log "Tailing ${LOG_FILE}  (Ctrl-C to stop)"
    exec tail -f "${LOG_FILE}"
}

cmd_start() {
    # ── Idempotency check ───────────────────────────────────────────────────
    if is_running; then
        local pid
        pid="$(find_running_pid)"
        ok "IB Gateway already running (pid=${pid}) — no action"
        return 0
    fi

    # ── Pre-flight checks ───────────────────────────────────────────────────
    if [[ ! -x "${IBG_BIN}" ]]; then
        err "IB Gateway launcher not found or not executable: ${IBG_BIN}"
        err "Re-run the IBKR installer, or adjust IBG_DIR at the top of this script."
        return 1
    fi

    if [[ -z "${DISPLAY:-}" ]] && [[ -z "${WAYLAND_DISPLAY:-}" ]]; then
        err "No X display available."
        err "  • On WSL2 (Windows 11): WSLg should set DISPLAY automatically — "
        err "    try 'wsl --shutdown' from PowerShell, then a fresh WSL terminal."
        err "  • On WSL2 without WSLg: start VcXsrv on Windows and "
        err "    'export DISPLAY=\$(ip route show default | awk '{print \$3}'):0' before running."
        return 2
    fi

    # Verify the display actually works — xset is cheap and universally available
    if command -v xset &>/dev/null; then
        if ! xset q &>/dev/null; then
            err "DISPLAY is set (${DISPLAY:-}) but not reachable."
            err "Is WSLg running? Is VcXsrv started on Windows?"
            return 2
        fi
    fi

    # ── Prep log directory ──────────────────────────────────────────────────
    mkdir -p "${LOG_DIR}"

    # ── Launch ──────────────────────────────────────────────────────────────
    log "Starting IB Gateway from ${IBG_BIN}"
    log "Log file: ${LOG_FILE}"
    log "A login window will appear — select Paper Trading and log in."

    # nohup + disown so it survives terminal close
    # setsid so it doesn't share our process group (won't die with Ctrl-C here)
    nohup setsid "${IBG_BIN}" >>"${LOG_FILE}" 2>&1 &
    disown

    # ── Wait briefly and confirm ────────────────────────────────────────────
    # The launcher script spawns a Java process and exits; we care about the
    # Java PID (via install4j), not the launcher's PID.
    sleep 3
    local real_pid
    real_pid="$(find_running_pid)"
    if [[ -n "${real_pid}" ]]; then
        echo "${real_pid}" > "${PID_FILE}"
        ok "Started  (pid=${real_pid})"
        log "Login window should appear within 10–20s."
        log "If nothing appears, check logs:  $0 --logs"
        return 0
    fi

    # Launcher didn't spawn a long-running Java process
    err "Launcher returned but no Java process is running."
    err "Recent log lines:"
    if [[ -f "${LOG_FILE}" ]]; then
        tail -n 20 "${LOG_FILE}" | sed 's/^/    /' >&2
    fi
    return 1
}

# ── Dispatch ─────────────────────────────────────────────────────────────────
case "${1:-start}" in
    ""|start)   cmd_start ;;
    --status|-s|status)   cmd_status ;;
    --stop|stop) cmd_stop ;;
    --logs|-l|logs) cmd_logs ;;
    -h|--help|help)
        cat <<EOF
${BOLD}start_ibgateway.sh${RESET} — manage a local IB Gateway process

Usage:
  $0                 Start Gateway (idempotent — no-op if already running)
  $0 --status        Show whether Gateway is running
  $0 --stop          Stop a running Gateway
  $0 --logs          Tail the Gateway log file
  $0 --help          This message

Paths:
  Install : ${IBG_DIR}
  Log     : ${LOG_FILE}
  PID     : ${PID_FILE}

Notes:
  • Assumes WSLg or VcXsrv provides the X display.
  • Does NOT auto-login — you log in via the window that appears.
  • For fully-automated login on a server, use IBC (IB Controller).
EOF
        ;;
    *)
        err "Unknown argument: $1"
        err "Run '$0 --help' for usage"
        exit 1
        ;;
esac