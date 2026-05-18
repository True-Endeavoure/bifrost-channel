// bifrost-channel — wake-stream daemon for the Bifrost Claude Code plugin.
//
// The canonical MCP server is mcp.bifrost-api.com (Bun-based codemode-mcp
// with OpenAPI-spec-to-tools transformation + Deno sandbox). This binary
// does NOT speak MCP itself — it holds a WebSocket to bifrost-api.com and
// surfaces wake events to the Claude Code session.
//
// Modes:
//
//	--monitor    Long-lived WS client; emits wake events as stdout JSON lines.
//	             Plugin runtime delivers each line to Claude as a notification.
//	--stop-hook  Short-lived WS; blocks up to 5min on first wake event.
//	--version    Print version and exit.
//
// Plugin's `.mcp.json` points HTTP-MCP at mcp.bifrost-api.com — that's the
// canonical tool surface. This binary is purely the wake-bridge.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var version = "dev"

func main() {
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
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] ERROR: BIFROST_API_KEY not set")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch {
	case *monitorMode:
		runMonitor(ctx, apiKey, bifrostURL)
	case *stopHookMode:
		runStopHook(ctx, apiKey, bifrostURL)
	default:
		fmt.Fprintln(os.Stderr, "usage: bifrost-channel [--monitor | --stop-hook | --version]")
		os.Exit(2)
	}
}

func wsURL(bifrostURL, apiKey string) string {
	u := strings.Replace(bifrostURL, "https://", "wss://", 1)
	u = strings.Replace(u, "http://", "ws://", 1)
	// Phoenix Channels v2 protocol requires ?vsn=2.0.0
	// Auth via ?token= (matches UserSocket pattern; CF passes through cleanly).
	return u + "/agent/stream/websocket?vsn=2.0.0&token=" + apiKey
}

func dial(ctx context.Context, apiKey, target string) (*websocket.Conn, error) {
	_, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer "+apiKey)
	c, _, err := websocket.DefaultDialer.DialContext(ctx, target, header)
	return c, err
}

// runMonitor: long-lived WS, stream events as stdout JSON lines.
// On any disconnect: exit 1 so plugin runtime restarts us.
func runMonitor(ctx context.Context, apiKey, bifrostURL string) {
	target := wsURL(bifrostURL, apiKey)
	fmt.Fprintln(os.Stderr, "[bifrost-channel] --monitor", version, "→", target)

	c, err := dial(ctx, apiKey, target)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] WS dial failed:", err)
		os.Exit(1)
	}
	defer c.Close()

	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = c.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_, msg, err := c.ReadMessage()
		if err != nil {
			fmt.Fprintln(os.Stderr, "[bifrost-channel] WS read error:", err)
			os.Exit(1)
		}
		// Each frame is one stdout line — plugin runtime delivers as notification.
		// If the server sent a JSON object, wrap with our channel envelope.
		var parsed map[string]any
		if json.Unmarshal(msg, &parsed) == nil {
			parsed["source"] = "bifrost"
			out, _ := json.Marshal(parsed)
			os.Stdout.Write(out)
		} else {
			os.Stdout.Write(msg)
		}
		os.Stdout.Write([]byte{'\n'})
	}
}

// runStopHook: short-lived WS, block up to 5min on first wake event.
func runStopHook(ctx context.Context, apiKey, bifrostURL string) {
	target := wsURL(bifrostURL, apiKey)
	fmt.Fprintln(os.Stderr, "[bifrost-channel] --stop-hook", version, "→", target)

	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	c, err := dial(cctx, apiKey, target)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] WS dial failed:", err)
		return
	}
	defer c.Close()

	type readResult struct {
		msg []byte
		err error
	}
	out := make(chan readResult, 1)
	go func() {
		_, m, e := c.ReadMessage()
		out <- readResult{m, e}
	}()

	select {
	case <-cctx.Done():
		return
	case r := <-out:
		if r.err != nil {
			fmt.Fprintln(os.Stderr, "[bifrost-channel] WS read error:", r.err)
			return
		}
		os.Stdout.Write(r.msg)
		os.Stdout.Write([]byte{'\n'})
	}
}
