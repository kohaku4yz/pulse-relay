#!/usr/bin/env bash
# Root-only opt-in for the Vitals extension: a local 5-minute detector timer.
# It sends nothing; consumers read GET /v1/health/vitals-outbox on loopback.
# Install the core first (deploy/install.sh), which deploys extensions/ too.
set -euo pipefail
set +x
INSTALL_DIR=/opt/pulse-relay
DATA_DIR=/var/lib/pulse-relay
unit=pulse-relay-vitals.service
timer=pulse-relay-vitals.timer
usage() { echo 'Usage: bash deploy/extensions/vitals/enable-vitals.sh --install | --disable | --status'; }
mode="${1:---status}"
case "$mode" in
  --status)
    printf 'VITALS_TIMER='; systemctl is-active "$timer" 2>/dev/null || true
    printf 'VITALS_UNIT_RESULT='; systemctl show "$unit" --value -p Result 2>/dev/null || true
    exit 0
    ;;
  --install|--disable) ;;
  *) usage; exit 2;;
esac
[[ "$(id -u)" == 0 ]] || { echo 'Run as root.' >&2; exit 2; }
if [[ "$mode" == --disable ]]; then
  systemctl disable --now "$timer" || true
  systemctl stop "$unit" || true
  echo 'VITALS_TIMER=disabled'
  echo 'HEALTH_DATA_AND_VITALS_STATE=preserved'
  exit 0
fi
# Units come from the installed, already tested copy, never from a working tree.
[[ -f "$INSTALL_DIR/DEPLOYED_FROM.txt" ]] || { echo 'Core not installed; run deploy/install.sh first.' >&2; exit 3; }
[[ -d "$INSTALL_DIR/pulse_relay/extensions/vitals" ]] || { echo 'Installed copy has no Vitals extension.' >&2; exit 3; }
[[ "$(stat -c %U "$INSTALL_DIR")" == root ]] || { echo 'Untrusted install owner.' >&2; exit 3; }
curl --fail --silent --show-error http://127.0.0.1:8768/healthz >/dev/null || { echo 'pulse-relay.service is not healthy.' >&2; exit 4; }
install -d -o pulse-relay -g pulse-relay -m 0700 "$DATA_DIR/vitals"
for file in "$unit" "$timer"; do
  dest="/etc/systemd/system/$file"
  if [[ -e "$dest" ]] && ! grep -q 'PulsHealth' "$dest"; then
    echo "Existing unexpected unit $file; refusing replacement" >&2; exit 4
  fi
done
for file in "$unit" "$timer"; do
  install -m 0644 -o root -g root "$INSTALL_DIR/deploy/extensions/vitals/$file" "/etc/systemd/system/$file"
done
systemctl daemon-reload
# The first run learns the current state silently: no historical candidates.
systemctl start "$unit"
unit_result="$(systemctl show "$unit" --value -p Result)"
[[ "$unit_result" == success ]] || { echo 'Vitals bootstrap not successful' >&2; exit 5; }
systemctl enable --now "$timer"
systemctl is-active --quiet "$timer"
printf 'VITALS_TIMER=active\n'
printf 'BOOTSTRAP_RESULT=%s\n' "$unit_result"
