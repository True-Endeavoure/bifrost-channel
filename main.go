// bifrost-channel — self-contained MCP-stdio + WS-to-bifrost-api daemon.
//
// Modes (selected by flag):
//   --mcp       — speak MCP protocol over stdio (called by Claude Code's .mcp.json)
//   --monitor   — long-lived process emitting wake events to stdout (called by monitors/monitors.json)
//   --stop-hook — blocking call for Claude Code's Stop hook; returns when a wake event arrives or timeout
//   --version   — print version and exit
//
// One binary, three roles. Plugin runtime spawns the right mode at the right time.
package main

import (
	"flag"
	"fmt"
	"os"
)

var version = "dev"

func main() {
	mcpMode := flag.Bool("mcp", false, "MCP protocol over stdio")
	monitorMode := flag.Bool("monitor", false, "Background WS monitor; emits wake events on stdout")
	stopHookMode := flag.Bool("stop-hook", false, "Blocking stop-hook handler; returns on wake event")
	versionFlag := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Println("bifrost-channel", version)
		return
	}

	apiKey := os.Getenv("BIFROST_API_KEY")
	bifrostURL := os.Getenv("BIFROST_URL")
	if bifrostURL == "" {
		bifrostURL = "https://bifrost-api.com"
	}

	switch {
	case *mcpMode:
		runMCP(apiKey, bifrostURL)
	case *monitorMode:
		runMonitor(apiKey, bifrostURL)
	case *stopHookMode:
		runStopHook(apiKey, bifrostURL)
	default:
		fmt.Fprintln(os.Stderr, "usage: bifrost-channel [--mcp | --monitor | --stop-hook | --version]")
		os.Exit(2)
	}
}

// runMCP serves MCP protocol over stdio. Implements:
//   - initialize handshake
//   - tools/list (named tools: messages_send, debug_auth, execute, etc.)
//   - tools/call (proxies to bifrost-api.com REST endpoints)
//   - resources for streaming
//
// Slice 1 stub — full implementation in follow-up.
func runMCP(apiKey, url string) {
	fmt.Fprintln(os.Stderr, "[bifrost-channel] --mcp mode (stub, slice 1)")
	// TODO: wire @modelcontextprotocol-style stdio JSON-RPC handler
	select {} // block forever until stdin closes
}

// runMonitor opens a WS to bifrost-api.com/agent/stream and emits each inbound
// event as a stdout JSON line. The plugin runtime delivers each line as a
// notification to the Claude Code session.
//
// On disconnect: exit non-zero so plugin runtime restarts us.
func runMonitor(apiKey, url string) {
	fmt.Fprintln(os.Stderr, "[bifrost-channel] --monitor mode (stub, slice 1)")
	// TODO: ws.Dial wss://... /agent/stream with Bearer auth
	// TODO: on each frame: json.Marshal + stdout write + \n
	// TODO: heartbeat ping every 30s
	// TODO: on close: os.Exit(1)
	select {}
}

// runStopHook blocks until either:
//   - a wake event arrives via WS
//   - timeout (default 5min — Claude Code's hook timeout)
//
// Prints the wake event JSON to stdout on return, exits 0.
// Exit 0 + stdout = Claude continues; exit non-zero = error.
func runStopHook(apiKey, url string) {
	fmt.Fprintln(os.Stderr, "[bifrost-channel] --stop-hook mode (stub, slice 1)")
	// TODO: short-lived WS connection
	// TODO: wait for first wake event with 5min ceiling
	// TODO: print to stdout, exit 0
	os.Exit(0)
}
