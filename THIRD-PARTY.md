# Third-party notices

The server (`pulse_relay/server.py`, `store.py`) uses only the Python standard library.

The optional MCP server (`pulse_relay/mcp_server.py`, `health_client.py`) installs:

| Package | Version | License |
|---|---|---|
| [mcp](https://github.com/modelcontextprotocol/python-sdk) | 1.28.1 | MIT |
| [httpx](https://github.com/encode/httpx) | 0.28.1 | BSD-3-Clause |

These are installed into a separate virtualenv by `deploy/install-mcp.sh` and are not vendored in this repository.

pulse-relay implements the receiving side of the
[Puls Sync Protocol v1](https://github.com/PulsHealth/pulshealth/tree/main/docs/protocol)
and the `puls://pair` pairing format used by the
[PulsHealth](https://github.com/PulsHealth/pulshealth) iOS app (Apache-2.0).
No PulsHealth code is included. The PulsHealth name belongs to its project (see its `TRADEMARK.md`).
