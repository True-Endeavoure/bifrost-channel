# bifrost-channel

Self-contained Go binary for the Bifrost Claude Code plugin. One binary, three roles:

| Mode | Purpose |
|---|---|
| `--mcp` | MCP server over stdio. Spawned by Claude Code's `.mcp.json` |
| `--monitor` | Background WS to bifrost-api.com — emits wake events on stdout for `monitors/monitors.json` |
| `--stop-hook` | Blocking Stop-hook handler — returns on wake event or timeout |

## Build

```bash
make build         # current platform → dist/bifrost-channel
make build-all     # all platforms → dist/bifrost-channel-{os}-{arch}
```

## Distribution

Pushed to bifrost zot registry + bundled in the `bifrost` Claude Code plugin under `bin/`.

## Status

Slice 1 scaffold — modes are stubs. Real MCP protocol + WS client land in subsequent slices.

Tracked by Epic `019e3c82-c2f6` (AR-BIFROST-CHANNEL-GO-BINARY).

## License

MIT.
