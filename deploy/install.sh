#!/usr/bin/env bash
# Root-only installer for pulse-relay.
# Run this only from a root-owned git clone/check-out of this repository.
set -euo pipefail

INSTALL_DIR="/opt/pulse-relay"
SERVICE_NAME="pulse-relay"
UNIT_NAME="pulse-relay.service"
ETC_DIR="/etc/pulse-relay"
ENV_FILE="${ETC_DIR}/service.env"
DATA_DIR="/var/lib/pulse-relay"

die() { echo "FATAL: $*" >&2; exit 1; }
[[ "${EUID}" -eq 0 ]] || die "run as root"

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

[[ -d "${REPO_ROOT}/.git" ]] || die "run from a git checkout"
owner="$(stat -c '%U' "${REPO_ROOT}")"
[[ "${owner}" == "root" ]] || die "repository checkout must be root-owned"
if find "${REPO_ROOT}" -maxdepth 0 -perm /022 -print -quit | grep -q .; then
  die "repository checkout must not be group/world writable"
fi

COMMIT="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
[[ "${COMMIT}" =~ ^[0-9a-f]{40}$ ]] || die "could not resolve full commit SHA"
git -C "${REPO_ROOT}" diff --quiet HEAD -- || die "working tree has unstaged changes"
git -C "${REPO_ROOT}" diff --cached --quiet HEAD -- || die "working tree has staged changes"

id "${SERVICE_NAME}" >/dev/null 2>&1 ||   useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin "${SERVICE_NAME}"

install -d -m 0700 -o root -g root "${ETC_DIR}"
install -d -m 0700 -o "${SERVICE_NAME}" -g "${SERVICE_NAME}" "${DATA_DIR}"

if [[ ! -f "${ENV_FILE}" ]]; then
  token="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
  umask 0077
  printf 'PULSE_RELAY_INGEST_TOKEN=%s\n' "${token}" > "${ENV_FILE}"
  chown root:root "${ENV_FILE}"
  chmod 0600 "${ENV_FILE}"
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
candidate="${INSTALL_DIR}.candidate.${run_id}"
backup="${INSTALL_DIR}.backup.${run_id}"
rm -rf "${candidate}"
install -d -m 0755 -o root -g root "${candidate}"
git -C "${REPO_ROOT}" archive "${COMMIT}" | tar -x -C "${candidate}"
printf 'commit=%s\ndeployed_utc=%s\n' "${COMMIT}" "${run_id}" > "${candidate}/DEPLOYED_FROM.txt"
python3 -m compileall -q "${candidate}/pulse_relay" "${candidate}/tests"
PYTHONPATH="${candidate}" python3 -m unittest discover -s "${candidate}/tests" -q

install -m 0644 -o root -g root   "${candidate}/deploy/pulse-relay.service.example"   "/etc/systemd/system/${UNIT_NAME}"
systemctl daemon-reload

had_previous=false
if [[ -d "${INSTALL_DIR}" ]]; then
  had_previous=true
  mv "${INSTALL_DIR}" "${backup}"
fi
mv "${candidate}" "${INSTALL_DIR}"

rollback() {
  local ec=$?
  trap - ERR EXIT
  systemctl stop "${UNIT_NAME}" >/dev/null 2>&1 || true
  if [[ "${had_previous}" == true && -d "${backup}" ]]; then
    rm -rf "${INSTALL_DIR}"
    mv "${backup}" "${INSTALL_DIR}"
    systemctl start "${UNIT_NAME}" >/dev/null 2>&1 || true
  fi
  exit "${ec}"
}
trap rollback ERR EXIT

systemctl enable "${UNIT_NAME}"
systemctl restart "${UNIT_NAME}"
for _ in $(seq 1 30); do
  if curl --fail --silent --show-error http://127.0.0.1:8768/healthz >/dev/null; then
    break
  fi
  sleep 0.2
done
curl --fail --silent --show-error http://127.0.0.1:8768/healthz >/dev/null

trap - ERR EXIT
if [[ "${had_previous}" == true && -d "${backup}" ]]; then
  rm -rf "${backup}"
fi

echo "PULSE_RELAY_DEPLOYED"
echo "commit=${COMMIT}"
echo "token_file=${ENV_FILE}"
echo "next: bash ${INSTALL_DIR}/deploy/verify.sh"
