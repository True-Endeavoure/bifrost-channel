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
	"io"
	"net/http"
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

// phoenixJoin sends a phx_join to topic `agent:<agent_id>` after WS upgrade.
// Phoenix Channels v2 protocol: client must join a topic before events flow,
// else server closes with code 1002 protocol-error.
//
// Resolves agent_id by querying /auth/whoami with the api_key.
func phoenixJoin(c *websocket.Conn, apiKey, bifrostURL string) error {
	body, code, err := bifrostGET(apiKey, bifrostURL, "/auth/whoami")
	if err != nil {
		return fmt.Errorf("/auth/whoami: %w", err)
	}
	if code != 200 {
		return fmt.Errorf("/auth/whoami status=%d body=%s", code, body)
	}
	var whoami struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(body), &whoami); err != nil {
		return fmt.Errorf("whoami parse: %w", err)
	}
	agentID := whoami.Name
	if agentID == "" {
		return fmt.Errorf("whoami returned empty name")
	}
	// Allow override via env (matches BIFROST_AGENT_ID semantics).
	if envID := os.Getenv("BIFROST_AGENT_ID"); envID != "" {
		agentID = envID
	}
	// Phoenix Channels v2 message: ["join_ref", "msg_ref", "topic", "event", payload]
	msg := []any{"1", "1", "agent:" + agentID, "phx_join", map[string]any{}}
	if err := c.WriteJSON(msg); err != nil {
		return fmt.Errorf("write phx_join: %w", err)
	}
	fmt.Fprintln(os.Stderr, "[bifrost-channel] joined topic agent:"+agentID)
	return nil
}

func bifrostGET(apiKey, baseURL, path string) (string, int, error) {
	req, err := http.NewRequest("GET", baseURL+path, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode, nil
}

func dial(ctx context.Context, apiKey, target string) (*websocket.Conn, error) {
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

	if err := phoenixJoin(c, apiKey, bifrostURL); err != nil {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] phx_join failed:", err)
		os.Exit(1)
	}

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

	if err := phoenixJoin(c, apiKey, bifrostURL); err != nil {
		fmt.Fprintln(os.Stderr, "[bifrost-channel] phx_join failed:", err)
		return
	}

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
