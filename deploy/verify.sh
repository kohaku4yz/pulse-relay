#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="/opt/pulse-relay"
UNIT_NAME="pulse-relay.service"

[[ "${EUID}" -eq 0 ]] || { echo "FATAL: run as root" >&2; exit 1; }
[[ -f "${INSTALL_DIR}/DEPLOYED_FROM.txt" ]] || { echo "FATAL: missing deployment provenance" >&2; exit 1; }

systemctl is-active --quiet "${UNIT_NAME}"
curl --fail --silent --show-error http://127.0.0.1:8768/healthz | grep -q '"status":"ok"'
curl --fail --silent --show-error http://127.0.0.1:8768/v1/health/day | grep -q '"status"'
curl --fail --silent --show-error http://127.0.0.1:8768/v1/health/heartbeat | grep -q '"status"'

token="$(sed -n 's/^PULSE_RELAY_INGEST_TOKEN=//p' /etc/pulse-relay/service.env | head -n1)"
[[ -n "${token}" ]] || { echo "FATAL: missing ingest token" >&2; exit 1; }
curl --fail --silent --show-error \
  --header "Authorization: Bearer ${token}" \
  --header 'X-Puls-Protocol: 1' \
  http://127.0.0.1:8768/api/health-sync/v1/capabilities | grep -q '"protocolVersions":\[1\]'

listener="$(ss -ltnH 'sport = :8768' 2>/dev/null || true)"
echo "${listener}" | grep -q '127.0.0.1:8768' || {
  echo "FATAL: service is not listening on 127.0.0.1:8768" >&2
  exit 1
}

echo "PULSE_RELAY_VERIFY_OK"
cat "${INSTALL_DIR}/DEPLOYED_FROM.txt"
