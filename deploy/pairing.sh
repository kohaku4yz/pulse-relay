#!/usr/bin/env bash
# Print a PulsHealth pairing link (and a terminal QR code if qrencode is
# installed) for this server. Scan it with the iPhone camera, or paste the
# link into PulsHealth > Sync > Database.
# Usage: sudo bash deploy/pairing.sh https://<your-host>
# The link contains the ingest token: do not screenshot or share it.
set -euo pipefail

ENV_FILE="${PULSE_RELAY_ENV_FILE:-/etc/pulse-relay/service.env}"
BASE_PATH="/api/health-sync"
# Single-user server: PulsHealth requires a UUID, the server ignores it.
USER_ID="00000000-0000-4000-8000-000000000001"

die() { echo "FATAL: $*" >&2; exit 1; }
[[ $# -eq 1 ]] || die "usage: pairing.sh https://<your-host>"
[[ -r "${ENV_FILE}" ]] || die "cannot read ${ENV_FILE} (run as root)"

token="$(sed -n 's/^PULSE_RELAY_INGEST_TOKEN=//p' "${ENV_FILE}" | head -n1)"
[[ -n "${token}" ]] || die "no PULSE_RELAY_INGEST_TOKEN in ${ENV_FILE}"

# The token travels only through stdin (bash builtins, no child argv), so it
# never shows up in ps or /proc/<pid>/cmdline for other local users.
read -r -d '' BUILD_LINK <<'PY' || true
import sys
from urllib.parse import quote, urlsplit

raw, base_path, user = sys.argv[1:]
token = sys.stdin.readline().rstrip("\n")
parts = urlsplit(raw.strip())
if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
    sys.exit("FATAL: give the public https:// origin, e.g. https://health.example.com")
if parts.query or parts.fragment:
    sys.exit("FATAL: the URL must not have a query or fragment")
path = parts.path.rstrip("/")
if path not in ("", base_path):
    sys.exit(f"FATAL: unexpected path {path!r}; pass only the origin")
url = f"https://{parts.netloc}{base_path}"
print(url)
print(f"puls://pair?url={quote(url, safe='')}&token={quote(token, safe='')}&user={user}")
PY

output="$(printf '%s\n' "${token}" | python3 -I -c "${BUILD_LINK}" "$1" "${BASE_PATH}" "${USER_ID}")"
database_url="${output%%$'\n'*}"
link="${output#*$'\n'}"

echo "Database URL: ${database_url}"
echo
echo "${link}"
echo
if command -v qrencode >/dev/null 2>&1; then
  printf '%s' "${link}" | qrencode -t ANSIUTF8
else
  echo "(install qrencode for a scannable QR code, e.g. apt install qrencode)"
fi
