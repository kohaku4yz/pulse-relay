#!/usr/bin/env bash
# Root-only installer for the pulse-relay health MCP.
set -euo pipefail

BASE_DIR="/opt/pulse-relay-mcp"
APP_DIR="${BASE_DIR}/app"
VENV_DIR="${BASE_DIR}/venv"
UNIT_NAME="pulse-relay-mcp.service"
SERVICE_USER="pulse-relay-mcp"
MCP_VERSION="1.28.1"
HTTPX_VERSION="0.28.1"

die() { echo "FATAL: $*" >&2; exit 1; }
[[ "${EUID}" -eq 0 ]] || die "run as root"

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -d "${REPO_ROOT}/.git" ]] || die "run from a git checkout"
[[ "$(stat -c '%U' "${REPO_ROOT}")" == "root" ]] || die "repository checkout must be root-owned"
if find "${REPO_ROOT}" -maxdepth 0 -perm /022 -print -quit | grep -q .; then
  die "repository checkout must not be group/world writable"
fi
COMMIT="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
[[ "${COMMIT}" =~ ^[0-9a-f]{40}$ ]] || die "could not resolve full commit SHA"
git -C "${REPO_ROOT}" diff --quiet HEAD -- || die "working tree has unstaged changes"
git -C "${REPO_ROOT}" diff --cached --quiet HEAD -- || die "working tree has staged changes"

id "${SERVICE_USER}" >/dev/null 2>&1 || useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin "${SERVICE_USER}"
install -d -m 0755 -o root -g root "${BASE_DIR}"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  rm -rf "${VENV_DIR}"
  python3 -m venv "${VENV_DIR}"
fi
"${VENV_DIR}/bin/pip" install --disable-pip-version-check --no-cache-dir \
  "mcp==${MCP_VERSION}" "httpx==${HTTPX_VERSION}"

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
candidate="${BASE_DIR}/app.candidate.${run_id}"
backup="${BASE_DIR}/app.backup.${run_id}"
rm -rf "${candidate}"
install -d -m 0755 -o root -g root "${candidate}"
git -C "${REPO_ROOT}" archive "${COMMIT}" | tar -x -C "${candidate}"
printf 'commit=%s\ndeployed_utc=%s\n' "${COMMIT}" "${run_id}" > "${candidate}/DEPLOYED_FROM.txt"

python3 -m compileall -q "${candidate}/pulse_relay" "${candidate}/tests"
PYTHONPATH="${candidate}" python3 -m unittest discover -s "${candidate}/tests" -q
PYTHONPATH="${candidate}" "${VENV_DIR}/bin/python" -m unittest discover -s "${candidate}/tests_mcp" -q

install -m 0644 -o root -g root \
  "${candidate}/deploy/pulse-relay-mcp.service.example" \
  "/etc/systemd/system/${UNIT_NAME}"
systemctl daemon-reload

had_previous=false
if [[ -d "${APP_DIR}" ]]; then
  had_previous=true
  mv "${APP_DIR}" "${backup}"
fi
mv "${candidate}" "${APP_DIR}"

rollback() {
  local ec=$?
  trap - ERR EXIT
  systemctl stop "${UNIT_NAME}" >/dev/null 2>&1 || true
  rm -rf "${APP_DIR}"
  if [[ "${had_previous}" == true && -d "${backup}" ]]; then
    mv "${backup}" "${APP_DIR}"
    systemctl start "${UNIT_NAME}" >/dev/null 2>&1 || true
  fi
  exit "${ec}"
}
trap rollback ERR EXIT

systemctl enable "${UNIT_NAME}"
systemctl restart "${UNIT_NAME}"
for _ in $(seq 1 30); do
  if ss -ltnH 'sport = :8879' 2>/dev/null | grep -q '127.0.0.1:8879'; then
    break
  fi
  sleep 0.2
done
PYTHONPATH="${APP_DIR}" "${VENV_DIR}/bin/python" "${APP_DIR}/deploy/verify-mcp.py"

trap - ERR EXIT
if [[ "${had_previous}" == true && -d "${backup}" ]]; then
  rm -rf "${backup}"
fi

echo "PULSE_RELAY_MCP_DEPLOYED"
echo "commit=${COMMIT}"
echo "url=http://127.0.0.1:8879/mcp"
