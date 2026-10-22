"""Operator invariants: Vitals cannot publish or mutate old services."""
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]


class DeployTests(unittest.TestCase):
    def test_vitals_service_has_no_network_and_private_rw_scope(self):
        service=(ROOT/'deploy/extensions/vitals/pulse-relay-vitals.service').read_text()
        self.assertIn('User=pulse-relay',service)
        self.assertIn('IPAddressDeny=any',service)
        self.assertIn('RestrictAddressFamilies=AF_UNIX',service)
        self.assertIn('ProtectSystem=strict',service)
        self.assertIn('UMask=0077',service)
        # WAL readers must be able to create -shm/-wal next to the DB, but the
        # DB file itself stays read-only for this unit.
        self.assertIn('ReadWritePaths=/var/lib/pulse-relay\n',service)
        self.assertIn('ReadOnlyPaths=/var/lib/pulse-relay/health.sqlite3\n',service)
        self.assertNotIn('curl ',service)
        self.assertNotIn('mcp ',service)
        self.assertIn('-m pulse_relay.extensions.vitals.tick ',service)

    def test_vitals_timer_is_separate_from_the_core_service(self):
        timer=(ROOT/'deploy/extensions/vitals/pulse-relay-vitals.timer').read_text()
        self.assertIn('OnUnitActiveSec=5min',timer)
        self.assertIn('Unit=pulse-relay-vitals.service',timer)
        installer=(ROOT/'deploy/extensions/vitals/enable-vitals.sh').read_text()
        self.assertIn('systemctl enable --now "$timer"',installer)
        self.assertIn('systemctl disable --now "$timer"',installer)
        self.assertNotIn('automations',installer)
        self.assertNotIn('webhook',installer.lower())
        self.assertIn('deploy/extensions/vitals/$file',installer)

if __name__=='__main__':unittest.main()
