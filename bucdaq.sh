#!/usr/bin/env bash

export PATH="/usr/local/bin:/usr/bin:/bin"

# repo = folder where this script lives
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${SCRIPT_DIR}"
VENV_DIR="${REPO_DIR}/.venv"

ts() { date +"%Y-%m-%dT%H:%M:%S"; }
log() { echo "$(ts), INFO, bucdaq.sh, $*"; }

if pgrep -f -a "bucdaq.py" > /dev/null; then
  log "bucdaq.py already running."
  exit 0
fi

# activate venv
if [[ ! -f "${VENV_DIR}/bin/activate" ]]; then
  log "ERROR: missing venv at ${VENV_DIR}/bin/activate"
  exit 1
fi

# shellcheck disable=SC1090
source "${VENV_DIR}/bin/activate"
log ".venv activated (${VENV_DIR})"

cd "${REPO_DIR}"
log "Repo: ${REPO_DIR}"

# IMPORTANT: no stdout/stderr redirection here.
"${VENV_DIR}/bin/python3" -u "${REPO_DIR}/bucdaq.py"


# #!/usr/bin/env bash

# # If you SOURCE this script (". bucdaq.sh"), avoid changing your shell's error mode.
# # If you EXECUTE it ("./bucdaq.sh"), make it strict.
# if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
#   set -euo pipefail
# fi

# ts() { date +"%Y-%m-%dT%H:%M:%S"; }
# log() { echo "$(ts), INFO, bucdaq.sh, $*"; }

# # Repo dir = directory where this script lives
# SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# REPO_DIR="${SCRIPT_DIR}"
# VENV_DIR="${REPO_DIR}/.venv"

# # Log file under *current user* home (no /home/admin hardcoding)
# LOG_DIR="${HOME}/Documents/bucdaq"
# LOG_FILE="${LOG_DIR}/bucdaq.log"
# mkdir -p "${LOG_DIR}"

# # Check if already running
# if pgrep -f -a "bucdaq.py" >/dev/null 2>&1; then
#   log "bucdaq.py already running." | tee -a "${LOG_FILE}"
#   # If you want it to exit when already running, uncomment next line:
#   # [[ "${BASH_SOURCE[0]}" != "${0}" ]] && return 0 || exit 0
# else
#   # Load venv
#   if [[ ! -f "${VENV_DIR}/bin/activate" ]]; then
#     log "ERROR: venv not found at ${VENV_DIR}/bin/activate" | tee -a "${LOG_FILE}"
#     log "Create it with: cd \"${REPO_DIR}\" && python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt" | tee -a "${LOG_FILE}"
#     [[ "${BASH_SOURCE[0]}" != "${0}" ]] && return 1 || exit 1
#   fi

#   # shellcheck disable=SC1090
#   source "${VENV_DIR}/bin/activate"
#   log ".venv activated (${VENV_DIR})" | tee -a "${LOG_FILE}"

#   # Change cwd
#   cd "${REPO_DIR}"

#   log "== BUCDAQ (re)started ====" | tee -a "${LOG_FILE}"
#   log "Repo: ${REPO_DIR}" | tee -a "${LOG_FILE}"
#   log "Logging to: ${LOG_FILE}" | tee -a "${LOG_FILE}"

#   # Run (redirect stdout+stderr to log)
#   "${VENV_DIR}/bin/python3" -u "${REPO_DIR}/bucdaq.py" >> "${LOG_FILE}" 2>&1
# fi
